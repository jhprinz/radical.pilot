#!/usr/bin/env python

from subprocess      import Popen
import multiprocessing as mp

class A(mp.Process):

    def __init__(self):

        mp.Process.__init__(self)

        self.q = mp.Queue()
        def b(q):
            C = q.get()
            exit_code = C.poll()
            assert(exit_code == 1)
        B = mp.Process(target = b, args=[self.q])
        B.start ()

    def run(self):
        C = Popen(args  = '/bin/false')
        self.q.put(C)

a = A()
a.start()
a.join()

