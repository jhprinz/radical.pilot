#!/usr/bin/env python

import os
import signal
import subprocess
import sys
import time
import threading


def sigchld_handler(signum, frame):
    print('caught SIGCHLD in process {0}'.format(os.getpid()))
signal.signal(signal.SIGCHLD, sigchld_handler)


def main():
    print('parent pid is {0}'.format(os.getpid()))
    print('starting process')
    subprocess.Popen(['sleep', '1'])
    print('going to sleep')
    time.sleep(3)
    print('done')

if __name__ == '__main__':
    t = threading.Thread(target=main)
    t.start()
    signal.pause()

