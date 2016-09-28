#!/usr/bin/env python

################################################################################
#
# RP makes heavy use of processes and threads.  Communication is normally
# established via ZMQ channels -- but specifically in the case of abnormal
# conditions, an orderly termination sequence must be controled via exceptions
# and signals.
#
# Alas, a number of core python errors makes this difficult to achieve.  Amongst
# them are:
#
#   - https://bugs.python.org/issue24862 (08/2015)
#     processes can only be waited for by the parent process, which makes it
#     difficult to control process termination in a process tree if the process
#     chain gets interrupted, aven if the leaf PIDs are known.
#
#   - https://bugs.python.org/issue23395 (02/2015)
#     `SIGINT` signal handlers conflict with the *only* documented inter-thread
#     termination procedure `thread.interrupt_main()`.  This requires us to
#     either not handle `SIGINT`, or to find an alternative approach to thread
#     termination handling.
#
#   - https://bugs.python.org/issue21895 (07/2014)
#     several methods in python are not signal-interruptible, including
#     thread.join() and socket.select().  The reason is that those calls map to
#     libc level calls, but the CPython C-level signal handler is *not* directly
#     invoking the Python level signal handlers, but only sets a flag for later
#     handling.  That handling is supposed to happen at bytecode boundaries, ie.
#     after the any libc-call returns.
#
#     That could be circumvented by always using libc call equivalents with
#     a timeout.  Alas, that is not always possible -- for example, join() does
#     not have a timeout parameter.
#
#   - https://bugs.python.org/issue1856  (01/2008)
#     sys.exit can segfault Python if daemon threads are active.  This is fixed 
#     in python3, but will not be backported to 2.x, because...
#
#   - https://bugs.python.org/issue21963 (07/2014)
#     ... it would hang up for daemon threads which don't ever re-acquire the
#     GIL.  That is not an issue for us - but alas, no backport thus.  So, we
#     need to make sure our watcher threads (which are daemons) terminate 
#     on their own.
#
#   - https://bugs.python.org/issue27889 (08/2016)
#     signals can not reliably be translated into exceptions, as the delayed
#     signal handling implies races on the exception handling.  A nested
#     exception loop seems to avoid that problem -- but that cannot be enforced
#     in application code or 3rd party modules (and is very cumbersome to
#     consistently apply throughout the code stack).
#
#   - https://bugs.python.org/issue6634  (08/2009)
#     When called in a sub-thread, `sys.exit()` will, other than documented,
#     not exit the Python interpreter, nor will it (other than documented) print
#     any error to `stderr`.  The MainThread will not be notified of the exit
#     request.
#
#
# Not errors, but expected behavior which makes life difficult:
#
#   - https://joeshaw.org/python-daemon-threads-considered-harmful/
#     Python's daemon threads can still be alive while the interpreter shuts
#     down.  The shutdown will remove all loaded modules -- which will lead to
#     the dreaded 
#       'AttributeError during shutdown -- can likely be ignored'
#     exceptions.  There seems no clean solution for that, but we can try to
#     catch & discard the exception in the watchers main loop (which possibly
#     masks real errors though).
#
#   - cpython's delayed signal handling can lead to signals being ignored when
#     they are translated into exceptions.
#     Assume this pseudo-code loop in a low-level 3rd party module:
# 
#     data = None
#     while not data:
#         try:
#             if fd.select(timeout):
#                 data = read(size)
#         except:
#             # select timed out - retry
#             pass
#
#     Due to the verly generous except clauses, a signal interrupting the select
#     would be interpreted as select timeout.  Those clauses *do* exist in
#     modules and core libs.
#     (This race is different from https://bugs.python.org/issue27889)
#
#   - a cpython-level API call exists to inject exceptions into other threads:
#
#       import ctypes
#       ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(thread_id),
#                                                  ctypes.py_object(e))
#
#     Alas, almost the same problems apply as for signal handling: exceptions
#     thus inserted are interpreted delayed, and are thus prone to the same
#     races as signal handlers.  Further, they can easily get lost in
#     too-generous except clauses in low level modules and core libraries.
#     Further, unlike signals, they will not interrupt any libc calls.
#     That method is thus inferior to signal handling.
#
#   - mp.join() can miss starting child processes:
#     When start() is called, join can be called immediately after.  At that
#     point, the child may, however, not yet be alive, and join would *silently*
#     return immediately.  If between that failed join and process termination
#     the child process *actually* comes up, the process termination will hang,
#     as the child has not been waited upon.
#
#   - we actually can't really use fork() either, unless it is *immediately* (as
#     in *first call*) followed by an exec, because core python modules don't
#     free locks on fork.  We monkeypatch the logging module though and also
#     ensure unlock at-fork for our own stack, but the problem remains (zmq
#     comes to mind).
#     This problem could be addressed - but this is useless unless the other
#     problems are addressed, too (the problem applies to process-bootstrapping
#     only, and is quite easy to distinguish from other bugs / races).
#
# NOTE: For some GIL details, see http://www.dabeaz.com/python/GIL.pdf
#     This focuses on performance, but contains some details relevant to
#     signal handling.  Note this is from 2009, mentions that  GIL management
#     has not changed for the past 10 years.  It has not changed by now
#     either, FWIW.
#
#
# Bottom Line: We Can't Use:
# --------------------------
#
#   - signal handlers which raise exceptions
#   - exception injects into other threads
#   - thread.interrupt_main() in combination with SIGINT(CTRL-C) handlers
#   - daemon threads
#   - multiprocessing with a method target
#
#
# Chosen Approach
# ---------------
#
# We distinguish two termination 'directions' for each component: 
#
# i)  external, where a component's termination gets triggered by calling
#     `component.stop()` on the original component object instance.
#
# ii) internal, where a component's termination gets triggered from its main
#     thread, one of its sub-threads, or one of its child processes;
#
# Any external termination will ultimately trigger an internal one.
#
# Internal termination conditions need to be communicated to the component's
# MainThread.  This happens in one of two ways:
#
# sub-threads and child processes will terminate themself if they meet a 
# termination condition, and the MainThread will be notified by a thread
# and process watcherm (which itself is a sub-thread of the MainThread).
# Upon such a notification, the component's MainThread will raise an
# exception.
#
# TODO: clarify how termination causes are communicated to the main
#       thread, to include that information in the exception.
#
# Before the MainThread raises its exception, it communicates a termination
# command to (a) its remaining sub-threads, and (b) its remaining child 
# processes.
#
# a) a `mt.Event()` (`self._thread_term`) is set, and all threads are
#    `join()`ed
#    On timeout (default: 60s), a `SYSTEM_EXIT` exception is injected into
#    the sub-thread, and it is `join()`ed again.
#
# b) a `mp.Event()` (`self._proc_term`) is set, and all child processes are
#    `join()`ed.  
#    On timeout (default: 60s), a `SIGTERM` is sent to the child process,
#    and it is `join()`ed again.
#
# Despite the mentioned problems, we *must* install a `SIGTERM` signal handler,
# as SIGTERM will be used by the OS or middleware to communicate termination to
# the pilot agents.  Those signal handlers though cannot use exceptions, and
# will thus only set the termination events.  If the event is already set, the
# signal handler will invoke `os.exit()`
#
# NOTE: The timeout fallback mechanisms are error prone per discussion above,
#       and thus should only get triggered in exceptional circumstances.  No
#       guarantees can be made on overall clean termination in those cases!
#
#       This is complicated by the fact that we cannot (reliably) interrupt any
#       operation in sub-threads and child processes, so the wait for timeout
#       will also include the time until the terminee will discover the
#       termination request, which can be an arbitrary amount of time (consider
#       the child thread is transferring a large file).  It will be up to the
#       individual component implementations to try to mitigate this effect, and
#       to check for termination signals as frequently as possible.  
#
# NOTE: The timeout approach has a hierarchy problem: a child of a child of
#       a process is waited on for 60 seconds -- but at that point the
#       original process' `join()` will already have timed out, triggering
#       a hard termination of the intermediate child, thus skipping the
#       intermediate `join()`.  The timeout should thus take the hierarchy
#       depth into account.  
#       This is ignored for now, mainly because the depth of the hierarchy
#       is not communicated / known in all places.  The above mechanism will
#       thus only be able to handle at most one hierarchy layer of unclean
#       process or thread termination, and also only if thread and process
#       termination are triggered concurrently.
# 
################################################################################
# 
#
# This code demonstrates our approach to termination, and serves as a test for
# the general problem space.
#
# We create the followin process/thread hirarchy:
#
#   - main:         1: process 0, MainThread
#                   2: process 0, WatcherThread
#                   3: process 0, WorkerThread 1
#                   4: process 0, WorkerThread 2
#     - child 1:    5: process 1, MainThread
#                   6: process 1, WatcherThread
#                   7: process 1, WorkerThread
#                   8: process 1, WorkerThread
#       - child 2:  9: process 2, MainThread
#                  10: process 2, WatcherThread
#                  11: process 2, WorkerThread
#                  12: process 2, WorkerThread
#       - child 3:  -: process 3, MainThread
#                   -: process 3, WatcherThread
#                   -: process 3, WorkerThread
#                   -: process 3, WorkerThread
#     - child 4:    -: process 4, MainThread
#                   -: process 4, WatcherThread
#                   -: process 4, WorkerThread
#                   -: process 4, WorkerThread
#       - child 5:  -: process 5, MainThread
#                   -: process 5, WatcherThread
#                   -: process 5, WorkerThread
#                   -: process 5, WorkerThread
#       - child 6:  -: process 6, MainThread
#                   -: process 6, WatcherThread
#                   -: process 6, WorkerThread
#                   -: process 6, WorkerThread
#
# Worker threads will work on random items, consuming between 1 and 90 seconds
# of time each.  The enumerated entities will raise exceptions after 2 minutes,
# if the environment variable `RU_RAISE_ON_<N>` is set to `1`, where `<N>` is
# the respecitve enumeration value.  An additional test is defined by pressing
# `CONTROL-C`.
#
# A test is considered successful when all of the following conditions apply:
#
#   - the resulting log files contain termination notifications for all entities
#   - the application catches a `RuntimeError('terminated')` exception or a
#     `KeyboardInterrupt` exception
#   - no processes or threads remain (also, no zombies)
#
# ==============================================================================


import os
import sys
import time
import signal

import threading       as mt
import multiprocessing as mp

import radical.utils   as ru

# ------------------------------------------------------------------------------
#
dh = ru.DebugHelper()


# ------------------------------------------------------------------------------
#
SLEEP    = 1
RAISE_ON = 3


# ------------------------------------------------------------------------------
#
class WorkerThread(mt.Thread):

    def __init__(self, num, pnum, tnum):

        self.num  = num
        self.pnum = pnum
        self.tnum = tnum
        self.pid  = os.getpid() 
        self.tid  = mt.currentThread().ident 
        self.uid  = "t.%d.%s %8d.%s" % (self.pnum, self.tnum, self.pid, self.tid)
        self.term = mt.Event()
        
        mt.Thread.__init__(self, name=self.uid)

      # print '%s create' % self.uid


    def stop(self):
        self.term.set()


    def run(self):

        try:
          # print '%s start' % self.uid

            while not self.term.is_set():

              # print '%s run' % self.uid
                time.sleep(SLEEP)

                if self.num == 4 and self.pnum == 1:
                    print "4"
                    ru.raise_on(self.uid, RAISE_ON)
    
          # print '%s stop' % self.uid
    
        except Exception as e:
            print '%s error %s [%s]' % (self.uid, e, type(e))
    
        except SystemExit:
            print '%s exit' % (self.uid)
    
        except KeyboardInterrupt:
            print '%s intr' % (self.uid)
    
        finally:
            print '%s final' % (self.uid)


# ------------------------------------------------------------------------------
#
class WatcherThread(mt.Thread):

    # All entities which use a watcher thread MUST except KeyboardInterrupt, 
    # as we'll use that signal to communicate error conditions to the
    # MainThread.
    #
    # The watcher thread is a daemon thread: it must not be joined.  We thus
    # overload join() and disable it.
    #
    # To avoid races and locks during termination, we frequently check if the
    # MainThread is still alive, and terminate otherwise
    
    def __init__(self, to_watch, num, pnum, tnum):

        self.to_watch = to_watch
        self.num      = num
        self.pnum     = pnum
        self.tnum     = tnum
        self.pid      = os.getpid() 
        self.tid      = mt.currentThread().ident 
        self.uid      = "w.%d.%s %8d.%s" % (self.pnum, self.tnum, self.pid, self.tid)
        self.term     = mt.Event()

        self.main     = None
        for t in mt.enumerate():
             if t.name == "MainThread":
                 self.main = t

        if not self.main:
            raise RuntimeError('%s could not find main thread' % self.uid)
        
        mt.Thread.__init__(self, name=self.uid)
        self.daemon = True  # this is a daemon thread

        print '%s create' % self.uid


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self.term.set()


    # --------------------------------------------------------------------------
    #
    def join(self):

        print '%s: join ignored' % self.uid


    # --------------------------------------------------------------------------
    #
    def run(self):

        try:
            print '%s start' % self.uid

            while not self.term.is_set():

              # print '%s run' % self.uid
                time.sleep(SLEEP)

                if self.num == 2 and self.pnum == 0:
                    print "2"
                    ru.raise_on(self.uid, RAISE_ON)

                if self.num == 5 and self.pnum == 1:
                    print "5"
                    ru.raise_on(self.uid, RAISE_ON)

                # check watchables
                for thing in self.to_watch:
                    if thing.is_alive():
                        print '%s event: thing %s is alive' % (self.uid, thing.uid)
                    else:
                        print '%s event: thing %s has died' % (self.uid, thing.uid)
                        ru.cancel_main_thread()
                        assert(False) # we should never get here

                # check MainThread
                if not self.main.is_alive():
                    print '%s: main thread gone - terminate' % self.uid
                    self.stop()

            print '%s stop' % self.uid


        except Exception as e:
            print '%s error %s [%s]' % (self.uid, e, type(e))
            ru.cancel_main_thread()
       
        except SystemExit:
            print '%s exit' % (self.uid)
            # do *not* cancel MainThread here!  We get here after the cancel
            # signal has been sent in the main loop above
       
        finally:
            print '%s final' % (self.uid)


# ------------------------------------------------------------------------------
#
class ProcessWorker(mp.Process):
    
    def __init__(self, num, pnum):

        self.num   = num
        self.pnum  = pnum
        self.ospid = os.getpid() 
        self.tid   = mt.currentThread().ident 
        self.uid   = "p.%d.%s %8s.%s" % (self.pnum, 0, self.ospid, self.tid)

        print '%s create' % (self.uid)

        mp.Process.__init__(self, name=self.uid)

        self.worker  = None
        self.watcher = None


    # --------------------------------------------------------------------------
    #
    def join(self):

        # Due to the overloaded stop, we may see situations where the child
        # process pid is not known anymore, and an assertion in the mp layer
        # gets triggered.  We except that assertion and assume the join
        # completed.
        #
        # NOTE: the except can mask actual errors

        try:
            # when start() is called, join can be called immediately after.  At
            # that point, the child may, however, not yet be alive, and join
            # would *silently* return immediately.  If between that failed join
            # and process termination the child process *actually* comes up, the
            # process termination will hang, as the child has not been waited
            # upon.
            #
            # We thus use a timeout on join, and, when the child did not appear
            # then, attempt to terminate it again.
            #
            # TODO: choose a sensible timeout.  Hahahaha...

            print '%s join: child join %s' % (self.uid, self.pid)
            mp.Process.join(self, timeout=1)

            # give child some time to come up in case the join
            # was racing with creation
            time.sleep(1)  

            if self.is_alive(): 
                # we still (or suddenly) have a living child - kill/join again
                self.stop()
                mp.Process.join(self, timeout=1)

            if self.is_alive():
                raise RuntimeError('Cannot kill child %s' % self.pid)
                
            print '%s join: child joined' % (self.uid)

        except AssertionError as e:
            print '%s join: ignore race' % (self.uid)


    # --------------------------------------------------------------------------
    #
    def stop(self):

        # we terminate all threads and processes here.

        # The mp stop can race with internal process termination.  We catch the
        # respective OSError here.

        # In some cases, the popen module seems finalized before the stop is
        # gone through.  I suspect that this is a race between the process
        # object finalization and internal process termination.  We catch the
        # respective AttributeError, caused by `self._popen` being unavailable.
        #
        # NOTE: both excepts can mask actual errors

        try:
            # only terminate child if it exists -- but there is a race between
            # check and signalling anyway...
            if self.is_alive():
                self.terminate()  # this sends SIGTERM to the child process
                print '%s stop: child terminated' % (self.uid)

        except OSError as e:
            print '%s stop: child already gone' % (self.uid)

        except AttributeError as e:
            print '%s stop: popen module is gone' % (self.uid)


    # --------------------------------------------------------------------------
    #
    def run(self):

        # We can't catch signals from child processes and threads, so we only
        # look out for SIGTERM signals from the parent process.  Upon receiving
        # such, we'll stop.
        #
        # We also start a watcher (WatcherThread) which babysits all spawned
        # threads and processes, an which will also call stop() on any problems.
        # This should then trickle up to the parent, who will also have
        # a watcher checking on us.

        self.ospid = os.getpid() 
        self.tid   = mt.currentThread().ident 
        self.uid   = "p.%d.0 %8d.%s" % (self.pnum, self.ospid, self.tid)

        try:
            # ------------------------------------------------------------------
            def sigterm_handler(signum, frame):
                # on sigterm, we invoke stop(), which will exit.
                # Python should (tm) give that signal to the MainThread.  
                # If not, we lost.
                assert(mt.currentThread().name == 'MainThread')
                self.stop()
            # ------------------------------------------------------------------
            signal.signal(signal.SIGTERM, sigterm_handler)

            print '%s start' % self.uid

            # create worker thread
            self.worker1 = WorkerThread(self.num, self.pnum, 0)
            self.worker1.start()
     
            self.worker2 = WorkerThread(self.num, self.pnum, 0)
            self.worker2.start()
     
            self.watcher = WatcherThread([self.worker1, self.worker2], 
                                          self.num, self.pnum, 1)
            self.watcher.start()

            while True:
                print '%s run' % self.uid
                time.sleep(SLEEP)
                if self.num == 3 and self.pnum == 1:
                    print "3"
                    ru.raise_on(self.uid, RAISE_ON)

            print '%s stop' % self.uid

        except Exception as e:
            print '%s error %s [%s]' % (self.uid, e, type(e))
       
        except SystemExit:
            print '%s exit' % (self.uid)
       
        except KeyboardInterrupt:
            print '%s intr' % (self.uid)
       
        finally:
            # we came here either due to errors in run(), KeyboardInterrupt from
            # the WatcherThread, or clean exit.  Either way, we stop all
            # children.
            self.stop()


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        # the finally clause of run() can again be interrupted!  We thus move
        # the complete finalization into a separate method which shields the
        # finalization from that.  It will though abort any finalization on
        # interruption, as we have no means to distinguich finalization errors
        # from external interruptions.  This can, however lead to incomplete
        # finalization.
        #
        # This problem is mitigated when `ru.cancel_main_thread(once=True)` is
        # used to initiate finalization, as that method will make sure that any
        # signal is sent at most once to the process, thus avoiding any further
        # interruption.

        try:
            print '%s final' % (self.uid)
            if self.watcher:
                print '%s final -> twatcher' % (self.uid)
                self.watcher.stop()
            if self.worker:
                print '%s final -> tworker' % (self.uid)
                self.worker.stop()

            if self.watcher:
                print '%s final => twatcher' % (self.uid)
                self.watcher.join
                print '%s final |> twatcher' % (self.uid)
            if self.worker:
                print '%s final => tworker' % (self.uid)
                self.worker.join
                print '%s final |> tworker' % (self.uid)

            print '%s final' % (self.uid)

        except Exception as e:
            print '%s error %s [%s]' % (self.uid, e, type(e))
       
        except SystemExit:
            print '%s exit' % (self.uid)
       
        except KeyboardInterrupt:
            print '%s intr' % (self.uid)
       
        finally:
            print 'worker finalized'



# ------------------------------------------------------------------------------
#
def main(num):

    # *always* install SIGTERM and SIGINT handlers, which will translate those
    # signals into exceptable exceptions.

    watcher = None
    p1      = None
    p2      = None

    try:
        pid = os.getpid() 
        tid = mt.currentThread().ident 
        uid = "m.0.0 %8d.%s" % (pid, tid)

        print '%s start' % uid
        p1 = ProcessWorker(num, 1)
        p2 = ProcessWorker(num, 2)
        
        p1.start()
        p2.start()

        watcher = WatcherThread([p1, p2], num, 0, 1)
        watcher.start()

        while True:
            print '%s run' % uid
            time.sleep(SLEEP)
            if num == 1:
                print "1"
                ru.raise_on(uid, RAISE_ON)

        print '%s stop' % uid

    except RuntimeError as e:
        print '%s error %s [%s]' % (uid, e, type(e))
    
    except SystemExit:
        print '%s exit' % (uid)
    
    except KeyboardInterrupt:
        print '%s intr' % (uid)
    
    finally:
        finalize(p1, p2, watcher)


def finalize(p1, p2, watcher):

    try:
        if p1:
            print '%s final -> p.1' % (uid)
            p1.stop()
            print '%s final => p.1' % (uid)
            p1.join()
            print '%s final |> p.1' % (uid)
        else:
            print '%s final |? p.1' % (uid)

        if p2:
            print '%s final -> p.2' % (uid)
            p2.stop()
            print '%s final => p.2' % (uid)
            p2.join()
            print '%s final |> p.2' % (uid)
        else:
            print '%s final |? p.2' % (uid)

        if watcher:
            print '%s final -> pwatcher' % (uid)
            watcher.stop()
            print '%s final => pwatcher' % (uid)
            watcher.join()
            print '%s final |> pwatcher' % (uid)
        else:
            print '%s final |? pwatcher' % (uid)
        print '%s final' % (uid)

    except RuntimeError as e:
        print '%s finalize error %s [%s]' % (uid, e, type(e))
    
    except SystemExit:
        print '%s finalize exit' % (uid)
    
    except KeyboardInterrupt:
        print '%s finalize intr' % (uid)
    
    finally:
        print 'finalized'


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    uid = 'm.0.0 %8s.%15s' % (0, 0)

    if len(sys.argv) > 1:
        num = int(sys.argv[1])
    else:
        num = 1

    try:
        print '-------------------------------------------'
        main(num)

    except RuntimeError as e:
        print '%s error %s [%s]' % (uid, e, type(e))
    
    except SystemExit:
        print '%s exit' % (uid)
    
    except KeyboardInterrupt:
        print '%s intr' % (uid)
    
    finally:
        print 'success %d\n\n' % num

    print '-------------------------------------------'


# ------------------------------------------------------------------------------

