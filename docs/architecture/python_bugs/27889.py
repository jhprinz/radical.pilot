#!/usr/bin/env python

import multiprocessing as mp
import threading       as mt
import signal
import time
import os

# - create a thread which sends SIGUSR1 to main.  
# - install a signal handler for SIGUSR1 which raises an exception
# - the exception should be excepted in main, and arrive while it is waiting

import ctypes, ctypes.util
lib = ctypes.CDLL(ctypes.util.find_library('uuid'))

def sigusr2_handler(signum, frame):
    print 'signal caught - raise'
    raise RuntimeError('caught sigusr2')
signal.signal(signal.SIGUSR2, sigusr2_handler)


def sub(pid):
    time.sleep(1)
    os.kill(pid, signal.SIGUSR2)
    print 'signal sent'


caught = False
try:
    t = mt.Thread(target=sub, args=[os.getpid()])
    t.start()
    print 'begin wait'
    time.sleep(3)
except Exception as e:
    print 'exception caught'
    caught = True
else:
    print 'exception missed'
finally:
    t.join()

assert(caught)

