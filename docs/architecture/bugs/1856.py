#!/usr/bin/env python

import os
import random
import sys
import time
import threading

thread_has_run = set()

class Worker(threading.Thread):
  def run(self):
    while True:
      zero_f = open('/dev/zero', 'r')
      blank = zero_f.read(200)
      null_f = open('/dev/null', 'w')
      null_f.write(blank)
      zero_f.close()
      time.sleep(random.random()/199)
      null_f.close()
      thread_has_run.add(self.name)

def main(_):
  count = 0
  for _ in xrange(40):
    new_thread = Worker()
    new_thread.setDaemon(True)
    new_thread.start()
    count += 1
  #sys.setcheckinterval(random.randint(0, 100))
  while len(thread_has_run) < count:
    time.sleep(0.001)
  sys.exit(0)

if __name__ == '__main__':
  main(sys.argv)

