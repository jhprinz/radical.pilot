#!/usr/bin/env python

import zmq
import sys
import time

# optional rate limiting
delay = 0.0
if len(sys.argv) > 1:
    delay = float(sys.argv[1])

def bridge():
    context        = zmq.Context()
    socket_src     = context.socket(zmq.PULL)
    socket_src.hwm = 10
    socket_src.bind("tcp://127.0.0.1:5000")

    context         = zmq.Context()
    socket_sink     = context.socket(zmq.REP)
    socket_sink.hwm = 10
    socket_sink.bind("tcp://127.0.0.1:5001")

    while True:
        req = socket_sink.recv()
        msg = socket_src.recv_json()
        print 'fwd %s' % msg
        socket_sink.send_json(msg)

        if delay:
            time.sleep (delay)

bridge()

