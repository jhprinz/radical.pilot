#!/usr/bin/env python

import os
import sys
import zmq
import time
import multiprocessing as mp

cfg = {
    'source' : {
        'n'  : 1000,
        't'  : 0.1
        },
    'bridge' : {
        't'  : 0.1
        },
    'sink'   : {
        't'  : 0.1
        }
    }


def source(host):
    context        = zmq.Context()
    socket_src     = context.socket(zmq.PUSH)
    socket_src.hwm = 10
    socket_src.connect("tcp://%s:5000" % host)

    n = cfg['source']['n']
    t = cfg['source']['t']

    pid = os.getpid()

    for num in range(n):
        msg = {pid:num}
        socket_src.send_json(msg)
        print 'snd %s' % msg
        time.sleep (t)


def bridge(host):
    context        = zmq.Context()
    socket_src     = context.socket(zmq.PULL)
    socket_src.hwm = 10
    socket_src.bind("tcp://%s:5000" % host)

    context         = zmq.Context()
    socket_sink     = context.socket(zmq.REP)
    socket_sink.hwm = 10
    socket_sink.bind("tcp://127.0.0.1:5001")

    t = cfg['bridge']['t']

    while True:
        req = socket_sink.recv()
        msg = socket_src.recv_json()
        print 'fwd %s' % msg
        socket_sink.send_json(msg)

        if t:
            time.sleep (t)


def sink(host):
    context         = zmq.Context()
    socket_sink     = context.socket(zmq.REQ)
    socket_sink.hwm = 10
    socket_sink.connect("tcp://127.0.0.1:5001")

    t = cfg['sink']['t']
    
    while True:
        socket_sink.send('request')
        msg = socket_sink.recv_json()
        print 'rcv %s' % msg
        time.sleep (t)

if len(sys.argv) < 3:
    print """

    usage: %s <host> <type> [<type>]

    """
    sys.exit(-1)


host = sys.argv[1]

if host in ['local', 'localhost']:
    host = '127.0.0.1'

procs = list()
for arg in sys.argv[2:]:
    if arg == 'source': procs.append (mp.Process(target=source, args=[host]))
    if arg == 'bridge': procs.append (mp.Process(target=bridge, args=[host]))
    if arg == 'sink'  : procs.append (mp.Process(target=sink  , args=[host]))

# Run processes
for p in procs:
    p.start()

# Exit the completed processes
for p in procs:
    p.join()

        
