import sys
'''
This example demonstrates how to do dynamic instantiation
of classes in Python much like the Java Reflection API
allows.
'''
class One(object):
    name = 'one'

class Two(object):
    name = 'two'

class Three(object):
    name = 'three'


class Loader(object):

    def printGlobals(self):
        print globals()

    def getCommand(self, name):
        mod = sys.modules[__name__]
        class_ = getattr(mod, name)
        instance = class_()
        return instance

if __name__ == '__main__':
    l = Loader()
    cmd = l.getCommand('Two')
    print cmd.name
    #l.printGlobals()
