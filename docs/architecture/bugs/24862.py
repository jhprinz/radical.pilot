#!/usr/bin/env python

import subprocess
import multiprocessing as mp

class ProcessManager(object):

    def __init__(self):
        self.queue   = mp.Queue()
        self.watcher = mp.Process(target=self.watch)
        self.watcher.start ()

    def watch(self):
        job       = self.queue.get()
        exit_code = job.poll()
        assert(exit_code == 1)

    def wait(self):
        self.watcher.join()

    def run(self, task):
        job = subprocess.Popen(args=task)
        self.queue.put(job)


pm = ProcessManager()
pm.run('/bin/false')
pm.wait()

