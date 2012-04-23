# -*- coding: utf-8 -*-
# Open Source Initiative OSI - The MIT License (MIT):Licensing
#
# The MIT License (MIT)
# Copyright (c) 2012 DotCloud Inc (opensource@dotcloud.com)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import gevent
from nose.tools import assert_raises

import zerorpc
from testutils import teardown, random_ipc_endpoint

def test_simple_context():
    endpoint = random_ipc_endpoint()

    class LittleContext:
        def echo(self, msg):
            return msg

    class MySrv(zerorpc.Server):

        @zerorpc.context
        def open_context(self):
            yield LittleContext()

    srv = MySrv()
    srv.bind(endpoint)
    gevent.spawn(srv.run)

    client = zerorpc.Client()
    client.connect(endpoint)

    context = client.open_context()
    assert context.echo(42) == 42

def test_recursive_context():
    endpoint = random_ipc_endpoint()

    class MySrv:
        def __init__(self, id_ = 0):
            self._id = id_

        @zerorpc.context
        def open_context(self):
            yield MySrv(self._id + 1)

        def get_id(self):
            return self._id

    srv = zerorpc.Server(MySrv())
    srv.bind(endpoint)
    gevent.spawn(srv.run)

    client = zerorpc.Client()
    client.connect(endpoint)

    assert client.get_id() == 0
    context = client.open_context()
    assert context.get_id() == 1
    for x in xrange(2, 32):
        context = context.open_context()
        print context.get_id(), x
        assert context.get_id() == x

def test_lost_server_then_request():
    endpoint = random_ipc_endpoint()

    class LittleContext:
        def something(self):
            pass

    class MySrv:
        @zerorpc.context
        def open_context(self):
            yield LittleContext()

    srv = zerorpc.Server(MySrv(), heartbeat=1)
    srv.bind(endpoint)
    gevent.spawn(srv.run)

    client = zerorpc.Client(heartbeat=1)
    client.connect(endpoint)

    print 'open context'
    context = client.open_context()
    print 'close server'
    srv.close()
    print 'client call'
    with assert_raises(zerorpc.LostRemote):
        context.something()
    print 'done'

def test_lost_server_then_request_recursive():
    endpoint = random_ipc_endpoint()

    class MySrv:
        def __init__(self, id_ = 0):
            self._id = id_

        @zerorpc.context
        def open_context(self):
            yield MySrv(self._id + 1)

        def get_id(self):
            return self._id

    srv = zerorpc.Server(MySrv(), heartbeat=1)
    srv.bind(endpoint)
    gevent.spawn(srv.run)

    client = zerorpc.Client(heartbeat=1)
    client.connect(endpoint)

    print 'open context'
    context = client.open_context()
    print 'recurse 10 times'
    for x in xrange(9):
        context = context.open_context()
    assert context.get_id() == 10
    print 'close server'
    srv.close()
    print 'client call'
    with assert_raises(zerorpc.LostRemote):
        context.get_id()
    print 'done'

def test_lost_server_while_idle():
    pass

def test_lost_client_then_request():
    pass

def test_lost_client_while_idle():
    pass
