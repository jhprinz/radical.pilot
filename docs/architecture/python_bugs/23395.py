#!/usr/bin/env python

import signal, threading, thread, time
signal.signal(signal.SIGINT, signal.SIG_DFL) # or SIG_IGN

def thread_run():
    thread.interrupt_main()

t = threading.Thread(target=thread_run)
t.start()
time.sleep(10)

