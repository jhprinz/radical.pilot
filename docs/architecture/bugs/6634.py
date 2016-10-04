#!/usr/bin/env python

import threading as mt
import time
import sys


def sub():
    time.sleep(1)
    sys.exit(1)

try:
    t = mt.Thread(target=sub)
    t.start()
    time.sleep(3)
    assert(False) # should never have gotten here
except SystemExit as e:
    print 'exit: %s' % e
finally:
    t.join()

