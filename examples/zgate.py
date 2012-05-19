
import zerorpc

class Number:

    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value

    @zerorpc.context
    def add(self, value):
        self.value += value
        yield self


from zerorpc.decorators import DecoratorBase
from zerorpc.events import WrappedEvents


class RemoteObject:

    def __init__(self, obj, error=None, success=None):
        self.obj = obj
        self.error = error
        self.success = success


class AnyType:

    def process_call(self, context, bufchan, event, functor):
        xheader = context.middleware_get_task_context()
        result = context.middleware_call_procedure(functor, *event.args)
        if isinstance(result, types.GeneratorType):
            print "is a generator!"
            for item in iter(result):
                bufchan.emit('STREAM', item, xheader)
            bufchan.emit('STREAM_DONE', None, xheader)
        else:
            if isinstance(result, RemoteObject) or isinstance(result, zerorpc.core.ClientBase):
                print "remote object!"
                wchannel = WrappedEvents(bufchan)
                server = zerorpc.core.ServerBase(wchannel, result.obj, heartbeat=None, allow_remote_stop=True)
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
                print "not a generator"
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

                        class ContextClient(zerorpc.core.ClientBase):
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


class anytype(DecoratorBase):
    pattern = AnyType()




class Users:

    @anytype
    def get_all_users(self):
        return zerorpc.Client('tcp://:4242').get_all_users()

    @anytype
    def subscribe_all_users(self):
        return zerorpc.Client('tcp://:4242').subscribe_all_users()

    @anytype
    def number(self, value):
        return RemoteObject(Number(value))

    @anytype
    def users(self):
        return RemoteObject(ZeroProxy('tcp://:4242'))

    @anytype
    def self(s):
        return RemoteObject(s)

class Gate:

    @zerorpc.context
    def number(self, value):
        yield Number(value)

    @zerorpc.context
    def users(self):
        yield ZeroProxy('tcp://:4242')


if __name__ == '__main__':
    s = zerorpc.Server(Users())
    s.bind(sys.argv[1])
    s.run()
