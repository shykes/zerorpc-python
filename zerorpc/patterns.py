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

import types

import core
from .events import WrappedEvents


class ReqRep():

    def process_call(self, context, bufchan, event, functor):
        result = context.middleware_call_procedure(functor, *event.args)
        bufchan.emit('OK', (result,), context.middleware_get_task_context())

    def accept_answer(self, event):
        return True

    def process_answer(self, context, bufchan, event, method,
            raise_remote_error):
        result = event.args[0]
        if event.name == 'ERR':
            raise_remote_error(event)
        bufchan.close()
        bufchan.channel.close()
        bufchan.channel.channel.close()
        return result


class ReqStream():

    def process_call(self, context, bufchan, event, functor):
        xheader = context.middleware_get_task_context()
        for result in iter(context.middleware_call_procedure(functor,
                *event.args)):
            bufchan.emit('STREAM', result, xheader)
        bufchan.emit('STREAM_DONE', None, xheader)

    def accept_answer(self, event):
        return event.name in ('STREAM', 'STREAM_DONE')

    def process_answer(self, context, bufchan, event, method,
            raise_remote_error):
        def iterator(event):
            while event.name == 'STREAM':
                yield event.args
                event = bufchan.recv()
            if event.name == 'ERR':
                raise_remote_error(event)
            bufchan.close()
            bufchan.channel.channel.close()
        return iterator(event)


class ReqContext():

    def process_call(self, context, bufchan, event, functor):
        context_gen =context.middleware_call_procedure(functor, *event.args)
        methods = context_gen.next()
        wchannel = WrappedEvents(bufchan)
        server = core.ServerBase(wchannel, methods, heartbeat=None,
                allow_remote_stop=True)
        bufchan.emit('CTX', (None,))
        try:
            server.run()
        except Exception as e:
            try:
                context_gen.throw(e)
            except StopIteration:
                pass
        else:
            try:
                context_gen.next()
            except StopIteration:
                pass
        finally:
            server.close()
            wchannel.close()

    def accept_answer(self, event):
        return event.name == 'CTX'

    def process_answer(self, context, bufchan, event, method,
            raise_remote_error):
        if event.name == 'ERR':
            raise_remote_error(event)
        wchannel = WrappedEvents(bufchan)

        class ContextClient(core.ClientBase):
            def __init__(self, channel):
                self._closed = False
                super(ContextClient, self).__init__(channel, heartbeat=None)

            def close(self):
                if self._closed:
                    return
                self('_zerorpc_stop')
                super(ContextClient, self).close()
                wchannel.close()
                bufchan.close()
                bufchan.channel.close()
                bufchan.channel.channel.close()
                self._closed = True

            def __call__(self, method, *args, **kargs):
                if self._closed:
                    raise ValueError('I/O operation on closed context')
                return super(ContextClient, self).__call__(method, *args, **kargs)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.close()

        return ContextClient(wchannel)


##
## AnyType: a pattern which combines ReqContext, ReqStream and ReqRep,
## and automatically chooses the best one based on a method's return value.
##
## This means AnyType can safely be used for all methods, and will always work.
##

class AnyType:

    def process_call(self, context, bufchan, event, functor):
        xheader = context.middleware_get_task_context()
        result = context.middleware_call_procedure(functor, *event.args)
        if isinstance(result, types.GeneratorType):
            # is a generator
            for item in iter(result):
                bufchan.emit('STREAM', item, xheader)
            bufchan.emit('STREAM_DONE', None, xheader)
        else:
            if isinstance(result, RemoteObject) or isinstance(result, core.ClientBase):
                # remote object
                wchannel = WrappedEvents(bufchan)
                server = core.ServerBase(wchannel, result.obj, heartbeat=None, allow_remote_stop=True)
                bufchan.emit('CTX', (None,))
                try:
                    server.run()
                except Exception as e:
                    if callable(result.error):
                        result.error(e)
                else:
                    if callable(result.cleanup):
                        result.cleanup()
                finally:
                    server.close()
                    wchannel.close()
            else:
                # not a generator
                bufchan.emit('OK', (result,), xheader)

    def accept_answer(self, event):
        return True

    def process_answer(self, context, bufchan, event, method, raise_remote_error):
        if event.name == 'ERR':
            raise_remote_error(event)
        else:
            if event.name == 'OK':
                bufchan.close()
                bufchan.channel.close()
                bufchan.channel.channel.close()
                return result
            else:
                if event.name == 'STREAM':
                    def iterator(event):
                        while event.name == 'STREAM':
                            yield event.args
                            event = bufchan.recv()
                        if event.name == 'ERR':
                            raise_remote_error(event)
                        bufchan.close()
                        bufchan.channel.channel.close()
                    return iterator(event)
                else:
                    if event.name == 'CTX':

                        wchannel = WrappedEvents(bufchan)

                        class ContextClient(core.ClientBase):
                            def __init__(self, channel):
                                self._closed = False
                                super(ContextClient, self).__init__(channel, heartbeat=None)

                            def close(self):
                                if self._closed:
                                    return
                                self('_zerorpc_stop')
                                super(ContextClient, self).close()
                                wchannel.close()
                                bufchan.close()
                                bufchan.channel.close()
                                bufchan.channel.channel.close()
                                self._closed = True

                            def __call__(self, method, *args, **kargs):
                                if self._closed:
                                    raise ValueError('I/O operation on closed context')
                                return super(ContextClient, self).__call__(method, *args, **kargs)

                            def __enter__(self):
                                return self

                            def __exit__(self, *args):
                                self.close()

                        return ContextClient(wchannel)


class RemoteObject:
    """ A convenience object which zerorpc methods can use as a return value to return a remote marshalled object. """

    def __init__(self, obj, error=None, success=None):
        self.obj = obj
        self.error = error
        self.success = success


patterns_list = [ReqContext(), ReqStream(), ReqRep(), AnyType()]
