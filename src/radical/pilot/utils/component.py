
import os
import sys
import copy
import time
import pprint
import signal
import Queue

import setproctitle    as spt
import threading       as mt
import multiprocessing as mp
import radical.utils   as ru

from ..          import constants      as rpc
from ..          import states         as rps

from .misc       import hostip
from .prof_utils import timestamp      as rpu_timestamp

from .queue      import Queue          as rpu_Queue
from .queue      import QUEUE_OUTPUT   as rpu_QUEUE_OUTPUT
from .queue      import QUEUE_INPUT    as rpu_QUEUE_INPUT
from .queue      import QUEUE_BRIDGE   as rpu_QUEUE_BRIDGE

from .pubsub     import Pubsub         as rpu_Pubsub
from .pubsub     import PUBSUB_PUB     as rpu_PUBSUB_PUB
from .pubsub     import PUBSUB_SUB     as rpu_PUBSUB_SUB
from .pubsub     import PUBSUB_BRIDGE  as rpu_PUBSUB_BRIDGE

# ==============================================================================
#
class Component(mp.Process):
    """
    This class provides the basic structure for any RP component which operates
    on stateful things.  It provides means to:

      - define input channels on which to receive new things in certain states
      - define work methods which operate on the things to advance their state
      - define output channels to which to send the things after working on them
      - define notification channels over which messages with other components
        can be exchanged (publish/subscriber channels)

    All low level communication is handled by the base class -- deriving classes
    will register the respective channels, valid state transitions, and work
    methods.  When a 'thing' is received, the component is assumed to have full
    ownership over it, and that no other component will change the 'thing's
    state during that time.

    The main event loop of the component -- run() -- is executed as a separate
    process.  Components inheriting this class should be fully self sufficient,
    and should specifically attempt not to use shared resources.  That will
    ensure that multiple instances of the component can coexist for higher
    overall system throughput.  Should access to shared resources be necessary,
    it will require some locking mechanism across process boundaries.

    This approach should ensure that

      - 'thing's are always in a well defined state;
      - components are simple and focus on the semantics of 'thing' state
        progression;
      - no state races can occur on 'thing' state progression;
      - only valid state transitions can be enacted (given correct declaration
        of the component's semantics);
      - the overall system is performant and scalable.

    Inheriting classes may overload the methods:

        initialize
        initialize_child
        finalize
        finalize_child

    These method should be used to

      - set up the component state for operation
      - register input/output/notification channels
      - register work methods
      - register callbacks to be invoked on state notification
      - tear down the same on closing

    Inheriting classes MUST call the constructor:

        class StagingComponent(rpu.Component):
            def __init__(self, cfg, session):
                rpu.Component.__init__(self, cfg, session)

    Further, the class must implement the registered work methods, with
    a signature of:

        work(self, things)

    The method is expected to change the state of the 'thing's given.  'Thing's
    will not be pushed to outgoing channels automatically -- to do so, the work
    method has to call (see call documentation for other options):

        self.advance(thing)

    Until that method is called, the component is considered the sole owner of
    the 'thing's.  After that method is called, the 'thing's are considered
    disowned by the component.  If, however, components return from the work
    methods without calling advance on the given 'thing's, then the component
    keeps ownership of the 'thing's to advance it asynchronously at a later
    point in time.  That implies that a component can collect ownership over an
    arbitrary number of 'thing's over time, and they can be advanced at the
    component's descretion.

    NOTE: this class expects an unaltered name for the main thread 'MainThread'!
    """

    # FIXME:
    #  - make state transitions more formal

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):
        """
        This constructor MUST be called by inheriting classes, as it specifies
        the operation mode of the component: components can spawn a child
        process, or not.

        If a child will be spawned later, then the child process state can be
        initialized by overloading the`initialize_child()` method.
        Initialization for component the parent process is similarly done via
        `initializale_parent()`, which will be called no matter if the component
        spawns a child or not.

        Note that this policy should be strictly followed by all derived
        classes, as we will otherwise carry state over the process fork.  That
        can become nasty if the state included any form of locking (like, for
        profiling or locking).

        The symmetric teardown methods are called `finalize_child()` and
        `finalize_parent()`, for child and parent process, repsectively.

        Constructors of inheriting components *may* call start() in their
        constructor.
        """

        # NOTE: a fork will not duplicate any threads of the parent process --
        #       but it will duplicate any locks which are shared between the
        #       parent process and its threads -- and those locks might be in
        #       any state at this point.  As such, each child has to make
        #       sure to never, ever, use any of the inherited locks, but instead
        #       to create it's own set of locks in self.initialize_child
        #       / self.initialize_parent!

        self._cfg     = copy.deepcopy(cfg)
        self._session = session

        # we always need an UID
        if not hasattr(self, 'uid'):
            self._uid = ru.generate_id(self.__class__.__name__ + '.%(counter)s', 
                    ru.ID_CUSTOM)

        # state we carry over the fork
        self._started   = False
        self._debug     = cfg.get('debug', 'DEBUG')
        self._owner     = cfg.get('owner', self.uid)
        self._cname     = cfg.get('cname', self.__class__.__name__)
        self._ctype     = "%s.%s" % (self.__class__.__module__,
                                     self.__class__.__name__)
        self._number    = cfg.get('number', 0)
        self._name      = cfg.get('name.%s' %  self._number,
                                  '%s.%s'   % (self._ctype, self._number))

        self._term      = mt.Event()  # control watcher threads
        self._in_stop   = mt.Event()  # guard stop()

        if self._owner == self.uid:
            self._owner = 'root'

        # don't create log and profiler, yet -- we do that all on start, and
        # inbetween nothing should happen anyway
        # NOTE: is this a sign that we should call start right here?

        # initialize the Process base class for later fork.
        mp.Process.__init__(self, name=self.uid)


    # --------------------------------------------------------------------------
    #
    def __str__(self):
        return "%s <%s> [%s]" % (self.uid, self.__class__.__name__, self._owner)


    # --------------------------------------------------------------------------
    #
    def _heartbeat_monitor_cb(self, topic, msg):

      # self._log.debug('command incoming: %s', msg)

        cmd = msg['cmd']
        arg = msg['arg']

        if self._term.is_set():
            self._log.debug('command [%s] ignored during shutdown', cmd)
            return

        if cmd == 'heartbeat':
            sender = arg['sender']
            if sender == self._cfg['heart']:
              # self._log.debug('heartbeat monitored (%s)', sender)
                self._heartbeat = time.time()
            else:
                pass
              # self._log.debug('heartbeat ignored (%s)', sender)
        else:
            pass
          # self._log.debug('command ignored: %s', cmd)


    # --------------------------------------------------------------------------
    #
    def _cancel_monitor_cb(self, topic, msg):
        """
        We listen on the control channel for cancel requests, and append any
        found UIDs to our cancel list.
        """
        
        # FIXME: We do not check for types of things to cancel - the UIDs are
        #        supposed to be unique.  That abstraction however breaks as we
        #        currently have no abstract 'cancel' command, but instead use
        #        'cancel_units'.

      # self._log.debug('command incoming: %s', msg)

        if self._term.is_set():
            self._log.debug('command ignored during shutdown')
            return

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'cancel_units':

            uids = arg['uids']

            if not isinstance(uids, list):
                uids = [uids]

            self._log.debug('register for cancellation: %s', uids)

            with self._cancel_lock:
                self._cancel_list += uids
        else:
            pass
          # self._log.debug('command ignored: %s', cmd)


    # --------------------------------------------------------------------------
    #
    def _heartbeat_checker_cb(self):

        # This thread will raise and exit on failing heartbeat checks.  
        # That will be detected by the thread watcher, which will then 
        # terminate the component proper.

        if self._term.is_set():
            self._log.debug('hbeat check disabled during shutdown')
            return

        last = time.time() - self._heartbeat
        tout = self._heartbeat_timeout

        if last > tout:
            raise RuntimeError('heartbeat failed (%s / %s)' % (last, tout))

      # else:
      #     self._log.debug('heartbeat check ok (%s / %s)', last, tout)

        return False # always sleep


    # --------------------------------------------------------------------------
    #
    @property
    def cfg(self):
        return copy.deepcopy(self._cfg)

    @property
    def session(self):
        return self._session

    @property
    def uid(self):
        return self._uid

    @property
    def owner(self):
        return self._owner

    @property
    def name(self):
        return self._name

    @property
    def ctype(self):
        return self._ctype

    @property
    def is_parent(self):
        return self._is_parent

    @property
    def is_child(self):
        return not self.is_parent

    @property
    def has_child(self):
        return self.is_parent and self.pid


    # --------------------------------------------------------------------------
    #
    def _initialize_common(self):
        """
        This private method contains initialization for both parent a child
        process, which gets the component into a proper functional state.

        This method must be called *after* fork (this is asserted).
        """

        self_thread = mt.current_thread()
        assert(self_thread.name == 'MainThread')
        assert(not self._started)

        self._inputs        = dict()       # queues to get things from
        self._outputs       = dict()       # queues to send things to
        self._workers       = dict()       # methods to work on things
        self._publishers    = dict()       # channels to send notifications to
        self._subscribers   = dict()       # callbacks to receive notifications
        self._idlers        = dict()       # callbacks to call in intervals

        self._finalized     = False        # finalization guard
        self._final_cause   = None         # finalization info
        # FIXME: signals are only useful in the child process...
        self._signal        = mt.Event()   # signal flag

        self._cb_lock       = mt.RLock()   # guard threaded callback invokations

        # get debugging, logging, profiling set up
        if 'update' in self.uid:
            ru.attach_pudb()
        self._dh   = ru.DebugHelper(name=self.uid)
        self._log  = self._session._get_logger(self.uid, level=self._debug)
        self._log.info('a 1')
        self._prof = self._session._get_profiler(self.uid)
        self._log.info('a 2')

        self._prof.prof('initialize', uid=self.uid)
        self._log.info('initialize %s',   self.uid)
        self._log.info('cfg: %s', pprint.pformat(self._cfg))

        # all components need at least be able to talk to a control pubsub
        assert('bridges' in self._cfg)
        assert(rpc.LOG_PUBSUB     in self._cfg['bridges'])
        assert(rpc.STATE_PUBSUB   in self._cfg['bridges'])
        assert(rpc.CONTROL_PUBSUB in self._cfg['bridges'])
        assert(self._cfg['bridges'][rpc.LOG_PUBSUB    ]['addr_in'])
        assert(self._cfg['bridges'][rpc.STATE_PUBSUB  ]['addr_in'])
        assert(self._cfg['bridges'][rpc.CONTROL_PUBSUB]['addr_in'])

        # components can always publish logs, state updates and send control messages
        self.register_publisher(rpc.LOG_PUBSUB)
        self.register_publisher(rpc.STATE_PUBSUB)
        self.register_publisher(rpc.CONTROL_PUBSUB)

        # make sure we watch all threads
        self.register_timed_cb(self._thread_watcher_cb, timer= 1.0)

        # give any derived class the opportunity to perform initialization in
        # parent *and* child context
        self.initialize_common()


    # --------------------------------------------------------------------------
    #
    def initialize_common(self):
        """
        This method may be overloaded by the components.  It is called once in
        the context of the parent process, *and* once in the context of the
        child process, after start().
        """
        self._log.debug('initialize_common (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _initialize_parent(self):
        """
        parent initialization of component base class goes here
        """

        self_thread = mt.current_thread()
        assert(self_thread.name == 'MainThread')

        # give any derived class the opportunity to perform initialization in
        # the parent context
        self.initialize_parent()


    # --------------------------------------------------------------------------
    #
    def initialize_parent(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the parent process, upon start(), and should be used to
        set up component state before things arrive.
        """
        self._log.debug('initialize_parent (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _initialize_child(self):
        """
        Child initialization of component base class goes here.  The deriving
        class' `initialize_child()` method is called from here, after the base
        class initialization is done.
        """

        self_thread = mt.current_thread()
        assert(self_thread.name == 'MainThread')

        # The heartbeat monotoring is performed in the child, which is
        # registering two callbacks:
        #   - an CONTROL_PUBSUB _heartbeat_monitor_cb which listens for
        #     heartbeats with 'src == self.owner', and records the time of
        #     heartbeat in self._heartbeat
        #   - an idle _heartbeat_checker_cb which checks the timer in frequent
        #     intervals, and which will call self.stop() if the last heartbeat
        #     is longer that self._heartbeat_timeout seconds ago
        #
        # Note that heartbeats are also used to keep sub-agents alive.
        # FIXME: this is not yet done
        assert(self._cfg.get('heart'))
        assert(self._cfg.get('heartbeat_interval'))

        # set up for eventual heartbeat send/recv
        self._heartbeat          = time.time()  # startup =~ heartbeat
        self._heartbeat_timeout  = self._cfg['heartbeat_timeout']
        self._heartbeat_interval = self._cfg['heartbeat_interval']
        self.register_timed_cb(self._heartbeat_checker_cb,
                               timer=self._heartbeat_interval)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._heartbeat_monitor_cb)

        # set controller callback to handle cancellation requests
        self._cancel_list = list()
        self._cancel_lock = mt.RLock()
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._cancel_monitor_cb)

        # give any derived class the opportunity to perform initialization in
        # the child context
        self.initialize_child()


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the child process, upon start(), and should be used to
        set up component state before things arrive.
        """
        self._log.debug('initialize_child (NOOP)')


    # --------------------------------------------------------------------------
    #
    def _finalize_common(self):

        self_thread = mt.current_thread()
        assert(self_thread.name == 'MainThread')

        try:
            self._log.debug('TERM : %s _finalize_common', self.uid)
            self.finalize_common()
            self._log.debug('TERM : %s _finalize_common done', self.uid)
            
        except:
            self._log.exception('TERM : %s _finalize_common failed', self.uid)


        if self._finalized:
            self._log.debug('_finalize_common found done (%s)', self)
            # some other thread is already taking care of finalization
            return

        self._finalized = True

        # call finalizer of deriving classes

        # ----------------------------------------------------------------------
        # reverse order from _initialize_common
        #
        self.unregister_timed_cb(self._thread_watcher_cb)

        self.unregister_publisher(rpc.LOG_PUBSUB)
        self.unregister_publisher(rpc.STATE_PUBSUB)
        self.unregister_publisher(rpc.CONTROL_PUBSUB)
        #
        # ----------------------------------------------------------------------
      


        # signal all threads to terminate
        for s in self._subscribers:
            self._log.debug('%s -> term %s', self.uid, s)
            self._subscribers[s]['term'].set()
        for i in self._idlers:
            self._log.debug('%s -> term %s', self.uid, i)
            self._idlers[i]['term'].set()

        # collect the threads
        for s in self._subscribers:
            t = self._subscribers[s]['thread']
            if t != self_thread:
                self._log.debug('%s -> join %s', self.uid, s)
                t.join()
                self._log.debug('%s >> join %s', self.uid, s)
        for i in self._idlers:
            t = self._idlers[i]['thread']
            if t != self_thread:
                self._log.debug('%s -> join %s', self.uid, i)
                t.join()
                self._log.debug('%s >> join %s', self.uid, i)

        self._log.debug('%s close prof', self.uid)
        try:
            self._prof.prof("stopped", uid=self._uid)
            self._prof.close()
        except Exception:
            pass

        self._log.debug('_finalize_common done')


    # --------------------------------------------------------------------------
    #
    def finalize_common(self):
        """
        This method may be overloaded by the components.  It is called once in
        the context of the parent *and* the child process, upon stop().  It
        should be used to tear down component state after things have been
        processed.
        """
        self._log.debug('TERM : %s finalize_common (NOOP)', self.uid)

        # FIXME: finaliers should unrergister all callbacks/idlers/subscribers


    # --------------------------------------------------------------------------
    #
    def _finalize_parent(self):

        self_thread = mt.current_thread()
        assert(self_thread.name == 'MainThread')

        try:
            self._log.debug('TERM : %s _finalize_parent', self.uid)
            self.finalize_parent()
            self._log.debug('TERM : %s _finalize_parent done', self.uid)
            
        except:
            self._log.exception('TERM : %s _finalize_parent failed', self.uid)


    # --------------------------------------------------------------------------
    #
    def finalize_parent(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the parent process, upon stop(), and should be used to
        tear down component state after things have been processed.
        """
        self._log.debug('TERM : %s finalize_parent (NOOP)', self.uid)


    # --------------------------------------------------------------------------
    #
    def _finalize_child(self):

        self_thread = mt.current_thread()
        assert(self_thread.name == 'MainThread')

        try:
            self._log.debug('TERM : %s _finalize_child', self.uid)
            self.finalize_child()
            self._log.debug('TERM : %s _finalize_child done', self.uid)

        except:
            self._log.debug('TERM : %s _finalize_child failed', self.uid)


    # --------------------------------------------------------------------------
    #
    def finalize_child(self):
        """
        This method may be overloaded by the components.  It is called *once* in
        the context of the child process, upon stop(), and should be used to
        tear down component state after things have been processed.
        """
        self._log.debug('TERM : %s finalize_child (NOOP)', self.uid)


    # --------------------------------------------------------------------------
    #
    def start(self, spawn=True, timeout=60):
        """
        This method will start the child process.  After doing so, it will
        call `initialize_parent()`, which is thus only executed in the
        parent's process context (because: after fork).  
        
        In the child process, `start()` will execute the run loop, and in that
        context will call `initialize_child()` (before entering the loop).

        Thus `start()` essentially performs:

            pid = fork()

            if pid:
                self._initialize_common()
                self._initialize_parent()
            else:
                self._initialize_common()
                self._initialize_child()
                run()

        This is not really correct though, as we don't have control over the
        fork itself (that happens in the base class).  So our implementation
        does:

        def run():
            self._initialize_common()
            self._initialize_child()
            ...

        which takes care of the child initialization, and here we take care of
        the parent initialization after we called the base class' `start()`
        method, ie. after fork.

        On child failures, we do nothing, and wait for the thread/process
        watcher tread to pick up any early process demise, to relay that fact to
        the parent process.  Errors in the parent code path
        (`initialize_common()` / `initialize_parent()` will fall through, it is
        the callee's responsibility to stop the child process by calling
        `self.stop()`.


        BOOTSTRAPPING
        -------------
        
        Before fork, the parent will create a multiprocessing.Queue instance,
        and pass it on to the child process.  The child process will, after
        initialization, send an 'ALIVE' message through that queue, and is then
        considered up and running.  The parent will block until that ALIVE
        message is received, so that the child process is, up, running, and
        initialized when `start()` returns.   

        In the case of child-initialization errors, the child will send an error
        message instead, and the parent will raise a `RuntimeError` with the
        respective message instead.  The parent will also fail after a 10 second
        timeout (start optionally accepts a different timeout argument though).
        Both child and parent will close the queue immediately after
        initialization.

        NOTE: The parent initialization will only start *before* the child's
              ALIVE message has been received.  `stop()` will be called on
              failed child startup.

        NOTE: Due to various problems with Python's threading, multiprocessing
              and signal handling, we can only start components from the main
              thread.
        """

        
        self_thread = mt.current_thread()
        assert(self_thread.name == 'MainThread')

        if spawn:
            mp.Process.start(self)  # fork happens here

        # this is now the parent process context
        self._is_parent = True
        self._initialize_common()  # get logging up and stuff
        self._initialize_parent()

        # if we spawned a child, we wait for it to come up
        if spawn:
            self._log.info('before alive get %s', self.uid)
            msg = ru.fs_event_wait('%s.sync' % self.uid, timeout)
            self._log.info('after  alive get %s', self.uid)

            if not msg:
                raise RuntimeError('component startup: timeout [%s]' % self.uid)

            if msg != 'alive':
                raise RuntimeError('component startup: error [%s] (%s)' % (self.uid, msg))

            self._log.info('component startup: ok %s [%d]', self.uid, self.pid)


    # --------------------------------------------------------------------------
    #
    def stop(self):
        """
        Shut down the process hosting the event loop.  If the parent calls
        stop(), the child is terminated via SIGTERM, and the child process is
        responsible for finilization in its own scope, by installing a SIGTERM
        signal handler which calls `stop()` in the child context.

        NOTE: The child should *not* use `thread.interrupt_main()` 
              see http://bugs.python.org/issue23395

        When `stop()` is called from a sub-thread, it will raise a runtime error
        so that the thread exits.  We expect that thread termination to be
        caught by the mian thread (or at least to be communicated to the main
        thread) which then should invoke stop() itself.  Finalizers are thus
        only invoked in the main thread (just as initializers).

        It is in general important that all finalization steps are executed in
        reverse order of their initialization -- any deviation from that scheme
        should be carefully evaluated.  This specifically holds for the
        overloaded methods:

            - initialize_common / finalize_common
            - initialize_parent / finalize_parent
            - initialize_child  / finalize_child

        but also for their private counterparts defined in this base class.

        stop() basically performs:

            if parent:
                self.terminate()  # send SIGTERM to child
                finalize_parent()
                finalize_common()
            else:
                finalize_child()
                finalize_common()
                sys.exit()        # child process ends here

        Parent and child finalization will have all components and bridges
        available.
        """

        self_thread = mt.current_thread()

        self._log.debug('TERM : stop %s (%s : %s : %s) [%s]', self.uid, os.getpid(),
                        self.pid, self_thread.name, ru.get_caller_name())

        if self_thread.name != 'MainThread':
            raise RuntimeError('stop sub thread [%s]' % self_thread.name)

        # We are in the main thread -- either in the child or parent process
        # tear down all threads.  We want to make sure that this procedure does
        # not invoke any code path (like signal handlers) which *again* ends up
        # in the main thread's `stop()`, so we guard against that
        if self._in_stop.is_set():
            self._log.debug('early return from stop')
            return
        self._in_stop.set()
        

        if self._is_parent:
            self.terminate()      # send SIGTERM to child
            self._finalize_parent()
            self._finalize_common()

            self._log.debug('TERM : stop as parent (return)')
            return

        else:
            self._finalize_child()
            self._finalize_common()

            self._log.debug('TERM : stop as child (exit)')
            sys.exit()            # child process ends here


    # --------------------------------------------------------------------------
    #
    def terminate(self):
        '''
        This overloads `mp.Process.terminate()` and adds some guards against
        common failure modes.  If no child exists, this call has no effect.
        self.join() SHOULD be called after termination, to collect the child
        process.
        '''

        if not self.has_child:
            return

        try:
            self._log.info("TERM stop    %s (child)", self.pid)
            mp.Process.terminate(self)
            self._log.info("TERM : stopped %s" % self.pid)

        except OSError as e:
            # The mp stop can race with internal process termination.  
            # We catch the respective OSError here.
            
            self._log.warn('TERM : %s stop: child is gone', self.uid)

        except AttributeError as e:
            # In some cases, the popen module seems finalized before the stop is
            # gone through.  I suspect that this is a race between the process
            # object finalization and internal process termination.  We catch
            # the respective AttributeError, caused by `self._popen` being
            # unavailable.  Alas, this will hide any other attribute errors
            # - but we would not know how to handle those anyway.
            self._log.warn('TERM : %s stop: popen is gone', self.uid)


    # --------------------------------------------------------------------------
    #
    def join(self, timeout=None):
        '''
        This overloads `mp.Process.join()` and adds some guards against common
        failure modes.  If no child exists, this call has no effect.  The call
        will return `True` or `False`, depending if after `timeout` the child
        has been joined or not.  If no timeout is given, the call will always
        return `True`.
        '''

        if not self.has_child:
            return

        try:
            # we only really join when the component child process has been started
            # this is basically a wait(2) on the child pid.
            self._log.debug('TERM : %s join   (%s)', self.uid, self.pid)
            mp.Process.join(self, timeout)
            self._log.debug('TERM : %s joined (%s)', self.uid, self.pid)

        except AssertionError as e:
            # Due to the overloaded stop, we may seen situations where the child
            # process pid is not known anymore, and an assertion in the mp layer
            # gets triggered.  We catch that assertion and assume the join
            # completed.
            self._log.debug('TERM : ignored assertion error on join')

        # let callee know if child has been joined after timeout
        return (not self.is_alive())


    # --------------------------------------------------------------------------
    #
    def poll(self):
        """
        This is a wrapper around is_alive() which mimics the behavior of the
        same call in the sp.Popen class with the same name.  It does not return
        an exitcode though, but 'None' if the process is still alive, and always
        '0' otherwise.  This is used by the watcher thread which uniformly
        handles threads, components and sp.Popen handles.
        """

        if self.is_alive(): return None
        else:               return 0


    # --------------------------------------------------------------------------
    #
    def register_input(self, states, input, worker):
        """
        Using this method, the component can be connected to a queue on which
        things are received to be worked upon.  The given set of states (which
        can be a single state or a list of states) will trigger an assert check
        upon thing arrival.

        This method will further associate a thing state with a specific worker.
        Upon thing arrival, the thing state will be used to lookup the respective
        worker, and the thing will be handed over.  Workers should call
        self.advance(thing), in order to push the thing toward the next component.
        If, for some reason, that is not possible before the worker returns, the
        component will retain ownership of the thing, and should call advance()
        asynchronously at a later point in time.

        Worker invocation is synchronous, ie. the main event loop will only
        check for the next thing once the worker method returns.
        """

        if not isinstance(states, list):
            states = [states]


        name = '%s.%s.%s' % (self.uid, worker.__name__, '_'.join(states))

        if name in self._inputs:
            raise ValueError('input %s already registered' % name)

        # get address for the queue
        addr = self._cfg['bridges'][input]['addr_out']
        self._log.debug("using addr %s for input %s" % (addr, input))

        q = rpu_Queue(self._session, input, rpu_QUEUE_OUTPUT, self._cfg, addr=addr)
        self._inputs['name'] = {'queue'  : q,
                                'states' : states}

        self._log.debug('registered input %s', name)

        # we want exactly one worker associated with a state -- but a worker can
        # be responsible for multiple states
        for state in states:

            self._log.debug('START: %s register input %s: %s', self.uid, state, name)

            if state in self._workers:
                self._log.warn("%s replaces worker for %s (%s)" \
                        % (self.uid, state, self._workers[state]))
            self._workers[state] = worker

            self._log.debug('registered worker %s [%s]', worker.__name__, state)


    # --------------------------------------------------------------------------
    #
    def unregister_input(self, states, input, worker):
        """
        This methods is the inverse to the 'register_input()' method.
        """

        if not isinstance(states, list):
            states = [states]

        name = '%s.%s.%s' % (self.uid, worker.__name__, '_'.join(states))

        if name not in self._inputs:
            raise ValueError('input %s not registered' % name)
        del(self._inputs[name])
        self._log.debug('unregistered input %s', name)

        for state in states:
            self._log.debug('TERM : %s unregister input %s: %s', self.uid, state, name)
            if state not in self._workers:
                raise ValueError('worker %s not registered for %s' % worker.__name__, state)
            del(self._workers[state])
            self._log.debug('unregistered worker %s [%s]', worker.__name__, state)


    # --------------------------------------------------------------------------
    #
    def register_output(self, states, output=None):
        """
        Using this method, the component can be connected to a queue to which
        things are sent after being worked upon.  The given set of states (which
        can be a single state or a list of states) will trigger an assert check
        upon thing departure.

        If a state but no output is specified, we assume that the state is
        final, and the thing is then considered 'dropped' on calling advance() on
        it.  The advance() will trigger a state notification though, and then
        mark the drop in the log.  No other component should ever again work on
        such a final thing.  It is the responsibility of the component to make
        sure that the thing is in fact in a final state.
        """

        if not isinstance(states, list):
            states = [states]

        for state in states:

            self._log.debug('START: %s register output %s', self.uid, state)

            # we want a *unique* output queue for each state.
            if state in self._outputs:
                self._log.warn("%s replaces output for %s : %s -> %s" \
                        % (self.uid, state, self._outputs[state], output))

            if not output:
                # this indicates a final state
                self._outputs[state] = None
            else:
                # get address for the queue
                addr = self._cfg['bridges'][output]['addr_in']
                self._log.debug("using addr %s for output %s" % (addr, output))

                # non-final state, ie. we want a queue to push to
                q = rpu_Queue(self._session, output, rpu_QUEUE_INPUT, self._cfg, addr=addr)
                self._outputs[state] = q

                self._log.debug('registered output    : %s : %s : %s' \
                     % (state, output, q.name))


    # --------------------------------------------------------------------------
    #
    def unregister_output(self, states):
        """
        this removes any outputs registerd for the given states.
        """

        if not isinstance(states, list):
            states = [states]

        for state in states:
            self._log.debug('TERM : %s unregister output %s', self.uid, state)
            if not state in self._outputs:
                raise ValueError('state %s hasno output to unregister' % state)
            del(self._outputs[state])
            self._log.debug('unregistered output for %s', state)


    # --------------------------------------------------------------------------
    #
    def register_timed_cb(self, cb, cb_data=None, timer=None):
        """
        Idle callbacks are invoked at regular intervals -- they are guaranteed
        to *not* be called more frequently than 'timer' seconds, no promise is
        made on a minimal call frequency.  The intent for these callbacks is to
        run lightweight work in semi-regular intervals.  
        """

        name = "%s.idler.%s" % (self.uid, cb.__name__)
        self._log.debug('START: %s register idler %s', self.uid, name)

        with self._cb_lock:
            if name in self._idlers:
                raise ValueError('cb %s already registered' % cb.__name__)

        if None != timer:
            timer = float(timer)

        # create a separate thread per idle cb
        # NOTE: idle timing is a tricky beast: if we sleep for too long, then we
        #       have to wait that long on stop() for the thread to get active
        #       again and terminate/join.  So we always sleep for 1sec, and
        #       manually check if timer has passed before activating the
        #       callback.
        # ----------------------------------------------------------------------
        def _idler(terminate, callback, callback_data, to):
          # print 'thread %10s : %s' % (ru.gettid(), mt.current_thread().name)
            try:
                last = 0.0  # never been called
                while not terminate.is_set() and not self._term.is_set():
                    now = time.time()
                    if to == None or (now-last) > to:
                        with self._cb_lock:
                            if callback_data != None:
                                callback(cb_data=callback_data)
                            else:
                                callback()
                        last = now
                    if to:
                        time.sleep(0.1)
            except Exception as e:
                self._log.exception("TERM : %s idler failed %s", self.uid, mt.current_thread().name)
                # notify main thread of failure
                ru.raise_in_thread(e=ru.ThreadExit)
            finally:
                self._log.debug("TERM : %s idler final %s", self.uid, mt.current_thread().name)
        # ----------------------------------------------------------------------

        # create a idler thread
        e = mt.Event()
        t = mt.Thread(target=_idler, args=[e,cb,cb_data,timer], name=name)
        t.start()

        with self._cb_lock:
            self._idlers[name] = {'term'   : e,  # termination signal
                                  'thread' : t}  # thread handle

        self._log.debug('%s registered idler %s' % (self.uid, t.name))


    # --------------------------------------------------------------------------
    #
    def unregister_timed_cb(self, cb):
        """
        This method is reverts the register_timed_cb() above: it
        removes an idler from the component, and will terminate the
        respective thread.
        """

        name = "%s.idler.%s" % (self.uid, cb.__name__)
        self._log.debug('TERM : %s unregister idler %s', self.uid, name)

        with self._cb_lock:
            if name not in self._idlers:
                raise ValueError('%s is not registered' % name)

            entry = self._idlers[name]
            entry['term'].set()
            entry['thread'].join()
            del(self._idlers[name])

        self._log.debug("TERM : %s unregistered idler %s", self.uid, name)


    # --------------------------------------------------------------------------
    #
    def register_publisher(self, pubsub):
        """
        Using this method, the component can registered itself to be a publisher
        of notifications on the given pubsub channel.
        """

        if pubsub in self._publishers:
            raise ValueError('publisher for %s already registered' % pubsub)

        # get address for pubsub
        if not pubsub in self._cfg['bridges']:
            self._log.error('no addr: %s' % pprint.pformat(self._cfg['bridges']))
            raise ValueError('no bridge known for pubsub channel %s' % pubsub)

        self._log.debug('START: %s register publisher %s', self.uid, pubsub)

        addr = self._cfg['bridges'][pubsub]['addr_in']
        self._log.debug("using addr %s for pubsub %s" % (addr, pubsub))

        q = rpu_Pubsub(self._session, pubsub, rpu_PUBSUB_PUB, self._cfg, addr=addr)
        self._publishers[pubsub] = q

        self._log.debug('registered publisher : %s : %s' % (pubsub, q.name))


    # --------------------------------------------------------------------------
    #
    def unregister_publisher(self, pubsub):
        """
        This removes the registration of a pubsub channel for publishing.
        """

        if pubsub not in self._publishers:
            raise ValueError('publisher for %s is not registered' % pubsub)

        self._log.debug('TERM : %s unregister publisher %s', self.uid, pubsub)

        del(self._publishers[pubsub])
        self._log.debug('unregistered publisher %s', pubsub)


    # --------------------------------------------------------------------------
    #
    def register_subscriber(self, pubsub, cb, cb_data=None):
        """
        This method is complementary to the register_publisher() above: it
        registers a subscription to a pubsub channel.  If a notification
        is received on thag channel, the registered callback will be
        invoked.  The callback MUST have one of the signatures:

          callback(topic, msg)
          callback(topic, msg, cb_data)

        where 'topic' is set to the name of the pubsub channel.

        The subscription will be handled in a separate thread, which implies
        that the callback invocation will also happen in that thread.  It is the
        caller's responsibility to ensure thread safety during callback
        invocation.
        """

        name = "%s.subscriber.%s" % (self.uid, cb.__name__)
        self._log.debug('START: %s unregister subscriber %s', self.uid, name)

        # get address for pubsub
        if not pubsub in self._cfg['bridges']:
            raise ValueError('no bridge known for pubsub channel %s' % pubsub)

        addr = self._cfg['bridges'][pubsub]['addr_out']
        self._log.debug("using addr %s for pubsub %s" % (addr, pubsub))

        # subscription is racey for the *first* subscriber: the bridge gets the
        # subscription request, and forwards it to the publishers -- and only
        # then can we expect the publisher to send any messages *at all* on that
        # channel.  Since we do not know if we are the first subscriber, we'll
        # have to wait a little to let the above happen, before we go further
        # and create any publishers.
        # FIXME: this barrier should only apply if we in fact expect a publisher
        #        to be created right after -- but it fits here better logically.
      # time.sleep(0.1)

        # ----------------------------------------------------------------------
        def _subscriber(q, terminate, callback, callback_data):

            try:
                while not terminate.is_set() and not self._term.is_set():
                    topic, msg = q.get_nowait(1000) # timout in ms
                    if topic and msg:
                        if not isinstance(msg,list):
                            msg = [msg]
                        for m in msg:
                          # self._log.debug("<= %s:%s: %s", self.uid, callback.__name__, topic)
                            with self._cb_lock:
                                if callback_data != None:
                                    callback(topic=topic, msg=m, cb_data=callback_data)
                                else:
                                    callback(topic=topic, msg=m)
                self._log.debug("x< %s:%s: %s", self.uid, callback.__name__, topic)
            except Exception as e:
                self._log.exception("subscriber failed %s", mt.current_thread().name)
                # notify main thread of failure
                ru.raise_in_thread(e=ru.ThreadExit)
        # ----------------------------------------------------------------------

        with self._cb_lock:
            if name in self._subscribers:
                raise ValueError('cb %s already registered for %s' % (cb.__name__, pubsub))

            # create a pubsub subscriber (the pubsub name doubles as topic)
            q = rpu_Pubsub(self._session, pubsub, rpu_PUBSUB_SUB, self._cfg, addr=addr)
            q.subscribe(pubsub)

            e = mt.Event()
            t = mt.Thread(target=_subscriber, args=[q,e,cb,cb_data], name=name)
            t.start()

            self._subscribers[name] = {'term'   : e,  # termination signal
                                       'thread' : t}  # thread handle

        self._log.debug('%s registered %s subscriber %s' % (self.uid, pubsub, t.name))


    # --------------------------------------------------------------------------
    #
    def unregister_subscriber(self, pubsub, cb):
        """
        This method is reverts the register_subscriber() above: it
        removes a subscription from a pubsub channel, and will terminate the
        respective thread.
        """

        name = "%s.subscriber.%s" % (self.uid, cb.__name__)
        self._log.debug('TERM : %s unregister subscriber %s', self.uid, name)

        with self._cb_lock:
            if name not in self._subscribers:
                raise ValueError('%s is not subscribed to %s' % (cb.__name__, pubsub))

            entry = self._subscribers[name]
            entry['term'].set()
            entry['thread'].join()
            del(self._subscribers[name])

        self._log.debug("unregistered %s", name)


    # --------------------------------------------------------------------------
    #
    def _thread_watcher_cb(self):

        for s in self._subscribers:
            t = self._subscribers[s]['thread']
            if not t.is_alive() and not self._term.is_set():
                self._log.error('TERM : %s subscriber %s died', self.uid, t.name)
                ru.raise_in_thread(e=ru.ThreadExit)
              # os.kill(os.getpid(), signal.SIGTERM)
              # sys.exit(0)
                raise RuntimeError('exit watcher thread')

        for i in self._idlers:
            t = self._idlers[i]['thread']
            if not t.is_alive() and not self._term.is_set():
                self._log.error('TERM : %s idler %s died', self.uid, t.name)
                ru.raise_in_thread(e=ru.ThreadExit)
              # os.kill(os.getpid(), signal.SIGTERM)
              # sys.exit(0)
                raise RuntimeError('exit watcher thread')


    # --------------------------------------------------------------------------
    #
    def _signal_handler(self, signum, frame):

        # The signal handler behaves differently in the child and parent
        # process: in the parent, we traise a ru.SignalRaised exception, which
        # will (hopefully) be cleanly caught by the nested try/except clause
        # around the main loop -- the child's main thread should never have any
        # execute any code outside of those clauses, unless it it already
        # terminating anyway.
        #
        # The parent process is either 
        #   - a component child process itself, and is thus covered by the
        #     mechanism above,
        #   - the RP agent process, for # which the same constraints as above hold
        #     (doubly nested try/except clause),
        #   - the user application.
        #
        # For the latter, we can't enforce a cumbersome nested try/except, so we
        # can't raise an exception [1].  In this case we thus partially
        # re-implement the functionality of the low level python signal
        # handling: sets a flag that the signal has been received, and then
        # opportunistically evaluate that flag in the main thread.
        #
        # toward the latter action, we provide a separate method
        # `_signal_checker` to raise the respective exception when called (which
        # thus id delayed from the time when the signal was received).  That
        # method should indirectly be called on all RP API calls -- all
        # components will thus register the signal checkers in the session, and
        # all API methods will need to make sure to ask the session for the
        # respective checks now and then.
        #
        # The main drawback of the approach is that signals will only
        # interpreted with delay, and will specifically not interrupt any ongoing
        # operation.  The advantage is that an application can always guard RP
        # API calls by simple try/except clauses, and will then not miss any RP
        # level error or termination conditions.
        #
        # NOTE: The application MUST NOT register a SIGTERM signal handler!
        #
        # [1] https://bugs.python.org/issue27889
        #
        
        self_thread = mt.current_thread()
        assert(self_thread.name == 'MainThread')
        
        if self.is_child:
            self._log.info('sigterm caught - raise exeption')
            raise ru.SignalRaised('signal handling (sigterm)')
        else:
            self._log.info('sigterm caught - set flag')
            self._signal.set()

    
    # --------------------------------------------------------------------------
    #
    def _signal_checker(self):
        
        # This signal checker MUST be called from the main thread.
        # the parent process should register it on the session, so that any RP
        # API call can check for any intercepted signal and trigger the
        # respective action (if delayed).
        self_thread = mt.current_thread()
        assert(self_thread.name == 'MainThread')

        # this signal checker can only be used by a component parent -- it
        # reduces to a NOOP for any child
        if self.is_child:
            return
        
        # if this checker is called in a parent, and if a signal has been
        # received by that process, then we now terminate the component.  
        if self._signal.is_set():
            self._log.info('delayed signal handling (sigterm)')
            raise ru.SignalRaised('delayed signal handling (sigterm)')


    # --------------------------------------------------------------------------
    #
    def run(self):
        """
        This is the main routine of the component, as it runs in the component
        process.  It will first initialize the component in the process context.
        Then it will attempt to get new things from all input queues
        (round-robin).  For each thing received, it will route that thing to the
        respective worker method.  Once the thing is worked upon, the next
        attempt on getting a thing is up.
        """

        # https://bugs.python.org/issue27889 requires us to use *two* nested
        # try/except clauses to mitigate the effects of the delayed cpython
        # signal handling race
        try:

            try:
                # we don't have a child, we *are* the child
                self._is_parent  = False
                self._parent_uid = self._uid
                self._uid        = self._uid + '.child'

                spt.setproctitle('rp.%s' % self.uid)

                # Install signal handler for termination handling.  The termination
                # signal can either be sent by the parent process, by the thread
                # & component watcher thread, or in fact by any other subthread.
                #
                # Python guarantees that the signal handler is called in the main
                # thread of the process, and we are thus presumably safe to
                # raise an exception to break the main loop.  We define
                # a SignalRaised exception which inherits from SystemExit --
                # this way, we should not interfere with component
                # implementation level exception handling, which mostly uses
                # `except Exception as e`, which will *not* catch
                # `SystemExit` exceptions.
                #
                # There is no guarantee that the signal is handled before the
                # inner `try`'s `finally` clause, and it can thus interfere with
                # the regular component termination.  The purpose of the outer
                # `try` is to handle that condition, and to perform clean
                # termination on faulty (delayed) signal handling.  The signal
                # handler will set an Event befor raising the exception, and the
                # outer `except` clause can check for that event, to determine
                # if the signal handler got invoked, and if the signal was acted
                # upon.
                # FIXME: is the latter needed if in this case, as we cann
                #        `stop()` exactly once anyway, and that point of calling
                #        is delayed until after both `try/except` clauses expire?
                #
                # NOTE:  We install this signal handler before running the
                #        initializers.  The finalizers can thus *not* depend on the
                #        initializers to have completed!  
                # FIXME: guard the finalizers against that race.
                #
                signal.signal(signal.SIGTERM, self._signal_handler)

                # this is now the child process context
                self._initialize_common()
                self._log.info(1)
                self._initialize_child()
                self._log.info(2)

                # initialization is done: let the parent know we are alive
                self._log.info('before alive put %s', self.uid)
                ru.fs_event_create('%s.sync' % self._parent_uid, 'alive')
                self._log.info('after  alive put %s', self.uid)
                self._log.info(3)

                self._log.debug('START: %s run', self.uid)

                # The main event loop will repeatedly iterate over all input
                # channels.  It can only be terminated by
                #   - exceptions
                #   - sys.exit()
                #   - kill from the parent (which becomes a sys.exit(), too)
                while True:
                    self._main_loop()

            # the inner try/except intercepts component implementation level
            # exceptions, and SignalRaised exceptions which arrive timely
            except Exception as e:
                self._final_cause = str(e)
                self._log.exception('caught exception from main loop')

            except ru.SignalRaised as e:
                self._final_cause = str(e)
                self._log.warn('signalled while in main loop [%s]', e)

            else:
                self._final_cause = 'final'

            finally:
                self._log.info('finally (inner) after main loop')

        # the outer try/except intercepts delayed signal exceptions, but also
        # random other exceptions which are caused by the signal delay

        except Exception as e:
            self._final_cause = str(e)
            if self._signal.is_set():
                self._log.warn('delayed signal error')
            else:
                self._log.exception('delayed loop exception')
        
        except ru.SignalRaised as e:
            self._final_cause = str(e)
            self._log.exception('delayed signal handling')

        else:
            if not self._final_cause:
                self._final_cause = 'final'
        
        finally:
            # There is one and *exactly* one place in the child process which
            # calls self.stop(), and this is here, when the MainThread falls out
            # of the main_loop.  
            #
            # That implies one of the following conditions happened:
            #   - a termination signal from system, parent 
            #   - an uncaught exception in the main loop
            #   - an enacted cancelation command
            #
            # Since we call stop() in all cases, we don't really care much about
            # the reason -- but we log the value of 'self._final_cause'.
            # FIXME: implement final_cause
            self._log.info('terminate %s child [%s]', self.uid, self._final_cause)
            self.stop()


    # --------------------------------------------------------------------------
    #
    def _main_loop(self):

        # NOTE: this loop *must* run in the main thread -- otherwise we will
        #       not be able to cleanly terminate the component.

        # if no action occurs in this iteration, idle a bit to avoid a busy loop
        idle = True

        for name in self._inputs:
            input  = self._inputs[name]['queue']
            states = self._inputs[name]['states']

            # check if we have anything to work on.  This will automatically
            # idle a bit if there is nothing to do.
            things = input.get_nowait(10) # timeout in microseconds

            if not things:
                continue

            # we found something to do, so there is no need to idle around
            idle = False

            if not isinstance(things, list):
                things = [things]

            self._log.debug('input bulk %s things on %s' % (len(things), name))

            # the worker target depends on the state of things, so we 
            # need to sort the things into buckets by state before 
            # pushing them.  At the same time we filter out any thing which
            # needs cancellation
            buckets = dict()
            with self._cancel_lock:
                for thing in things:

                    uid   = thing['uid']
                    state = thing['state']

                    self._log.debug('got %s', uid)
                    self._prof.prof(event='get', state=state, uid=uid, msg=name)

                    # FIXME: this can become expensive over time
                    #        if the cancel list is never cleaned
                    if uid in self._cancel_list:
                        self._cancel_list.remove(uid)
                        self.advance(thing, rps.CANCELED, publish=True, push=False)

                    else:
                        if not state in buckets:
                            buckets[state] = list()
                        buckets[state].append(thing)

            # We now can push bucketed bulks of things to the workers
            for state,things in buckets.iteritems():

                assert(state in states)
                assert(state in self._workers)

                try:
                   self._prof.prof(event='work start', state=state,
                                   uid=[t['uid'] for t in things])
                   self._workers[state](things)
                   self._prof.prof(event='work done ', state=state,
                                   uid=[t['uid'] for t in things])

                except Exception as e:
                    # NOTE: a failure here causes the full bulk to fail.
                    #       Component implementations should thus add their own
                    #       error handling for individual operations
                    self._log.exception("worker %s failed", self._workers[state])
                    self.advance(things, rps.FAILED, publish=True, push=False)

        if idle:
            # nothing was found in any of the known inputs -- avoid a busy idle
            # loop by sleeping a bit
            time.sleep(0.1)


    # --------------------------------------------------------------------------
    #
    def advance(self, things, state=None, publish=True, push=False,
                timestamp=None):
        """
        Things which have been operated upon are pushed down into the queues
        again, only to be picked up by the next component, according to their
        state model.  This method will update the thing state, and push it into
        the output queue registered as target for that state.

        things:  list of things to advance
        state:   new state to set for the things
        publish: determine if state update notifications should be issued
        push:    determine if things should be pushed to outputs
        prof:    determine if state advance creates a profile event
                 (publish, and push are always profiled)

        'Things' are expected to be a dictionary, and to have 'state', 'uid' and
        optionally 'type' set.

        If 'thing' contains an '$all' key, the complete dict is published;
        otherwise, *only the state* is published.

        This is evaluated in self.publish.
        """

        if not timestamp:
            timestamp = rpu_timestamp()

        if not isinstance(things, list):
            things = [things]

        self._log.debug(' === advance bulk size: %s', len(things))

        target = state
        state  = None

        # assign state, sort things by state
        buckets = dict()
        for thing in things:

            uid   = thing['uid']
            ttype = thing['type']

            if ttype not in ['unit', 'pilot']:
                raise TypeError("thing has unknown type (%s)" % uid)

            if target:
                # state advance done here
                thing['state'] = target

            state = thing['state']

            self._log.debug(' === advance bulk size: %s', len(things))
            self._prof.prof('advance', uid=uid, state=state, timestamp=timestamp)

            if not state in buckets:
                buckets[state] = list()
            buckets[state].append(thing)

        # should we publish state information on the state pubsub?
        if publish:

            to_publish = list()

            # If '$all' is set, we update the complete thing_dict.  
            # Things in final state are also published in full.
            # In all other cases, we only send 'uid', 'type' and 'state'.
            for thing in things:
                if '$all' in thing:
                    del(thing['$all'])
                    to_publish.append(thing)

                elif state in rps.FINAL:
                    to_publish.append(thing)

                else:
                    to_publish.append({'uid'   : thing['uid'],
                                       'type'  : thing['type'],
                                       'state' : state})

            self.publish(rpc.STATE_PUBSUB, {'cmd': 'update', 'arg': to_publish})
            ts = rpu_timestamp()
            for thing in things:
                self._prof.prof('publish', uid=thing['uid'], state=thing['state'], timestamp=ts)

        # never carry $all across component boundaries!
        else:
            for thing in things:
                if '$all' in thing:
                    del(thing['$all'])

        # should we push things downstream, to the next component
        if push:

            # the push target depends on the state of things, so we need to sort
            # the things into buckets by state before pushing them
            # now we can push the buckets as bulks
            for state,things in buckets.iteritems():

                self._log.debug(" === bucket: %s : %s", state, [t['uid'] for t in things])

                if state in rps.FINAL:
                    # things in final state are dropped
                    for thing in things:
                        self._log.debug('push %s ===| %s', thing['uid'], thing['state'])
                    continue

                if state not in self._outputs:
                    # unknown target state -- error
                    self._log.error("%s", ru.get_stacktrace())
                    self._log.error("%s can't route state for %s: %s (%s)" \
                            % (self.uid, things[0]['uid'], state, self._outputs.keys()))

                    continue

                if not self._outputs[state]:
                    # empty output -- drop thing
                    for thing in things:
                        self._log.debug('%s %s ~~~| %s' % ('push', thing['uid'], thing['state']))
                    continue

                output = self._outputs[state]

                # push the thing down the drain
                # FIXME: we should assert that the things are in a PENDING state.
                #        Better yet, enact the *_PENDING transition right here...
                self._log.debug(' === put bulk %s: %s', state, len(things))
                output.put(things)

                ts = rpu_timestamp()
                for thing in things:
                    
                    # never carry $all across component boundaries!
                    if '$all' in thing:
                        del(thing['$all'])

                    uid   = thing['uid']
                    state = thing['state']

                    self._log.debug('push %s ---> %s', uid, state)
                    self._prof.prof('put', uid=uid, state=state,
                            msg=output.name, timestamp=ts)


    # --------------------------------------------------------------------------
    #
    def publish(self, pubsub, msg):
        """
        push information into a publication channel
        """

        if pubsub not in self._publishers:
            raise RuntimeError("can't route '%s' notification: %s" % (pubsub, msg))

        if not self._publishers[pubsub]:
            raise RuntimeError("no route for '%s' notification: %s" % (pubsub, msg))

        self._publishers[pubsub].put(pubsub, msg)



# ==============================================================================
#
class Worker(Component):
    """
    A Worker is a Component which cannot change the state of the thing it
    handles.  Workers are emplyed as helper classes to mediate between
    components, between components and database, and between components and
    notification channels.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        Component.__init__(self, cfg=cfg, session=session)


    # --------------------------------------------------------------------------
    #
    # we overload state changing methods from component and assert neutrality
    # FIXME: we should insert hooks around callback invocations, too
    #
    def advance(self, things, state=None, publish=True, push=False, prof=True):

        if state:
            raise RuntimeError("worker %s cannot advance state (%s)"
                    % (self.uid, state))

        Component.advance(self, things, state, publish, push, prof)


# ------------------------------------------------------------------------------

