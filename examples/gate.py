
import zerorpc
from zerorpc import anytype, RemoteObject


class ZeroProxy:

    def __init__(self, addr):
        self.client = zerorpc.Client(addr)
        methods = dict((key, getattr(self.client, key)) for key in self.client._zerorpc_list())
        for (name, f) in methods.items():
            setattr(self, name, anytype(f))

class Gate:

    @anytype
    def proxy(self, addr):
        if addr == "self":
            return RemoteObject(self)
        return RemoteObject(ZeroProxy(addr))

    @anytype
    def self(_self):
        return RemoteObject(_self)
