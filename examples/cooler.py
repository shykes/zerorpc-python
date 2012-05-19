import zerorpc

class Cooler:
    """ Various convenience methods to make things cooler. """

    @zerorpc.anytype
    def add_man(self, sentence):
        """ End a sentence with ", man!" to make it sound cooler, and
        return the result. """
        return sentence + ", man!"

    @zerorpc.anytype
    def self(_self):
        return zerorpc.RemoteObject(_self)

    @zerorpc.anytype
    def add_mans(self):
        yield "man!"
        yield "man!"
        yield "man!"

    @zerorpc.anytype
    def add_42(self, n):
        """ Add 42 to an integer argument to make it cooler, and return the
        result. """
        return n + 42

    @zerorpc.anytype
    def boat(self, sentence):
        """ Replace a sentence with "I'm on a boat!", and return that,
        because it's cooler. """
        return "I'm on a boat!"

if __name__ == '__main__':
    import zerorpc

    s = zerorpc.Server(Cooler())
    s.bind("tcp://0.0.0.0:4242")
    s.run()

