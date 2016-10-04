#!/usr/bin/env python

import os, sys, threading

def worker():
    childpid = os.fork()
    if childpid != 0:
        # Parent waits for child.
        os.waitpid(childpid, 0)
    else:
        # Child spawns a daemon thread and then returns immediately.
        def daemon():
            while True:
                pass
        d = threading.Thread(target=daemon)
        d.daemon = True
        d.start()
        # Return, do not call sys.exit(0) or d.join().  The process should exit
        # without waiting for the daemon thread, but we expect that due to a bug
        # relating to os.fork and threads it will hang.


w = threading.Thread(target=worker)
w.start()
w.join()
#worker()

