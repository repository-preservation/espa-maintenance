from chain import Command, Chain

class Test1(Command):

    def __init__(self):
        pass

    def execute(self, context=None):
        print "Running test1 command"
        if context is not None:     
            context.update({"test1":"complete"})
            #return false to keep the chain moving
            return False
            
        else:
            pass
            #return true to halt the chain from advancing
            return True


class Test2(Command):
    def __init__(self):
        pass

    def execute(self, context=None):
        print "Running test2 command"
        if context is not None:            
            context.update({"test2":"complete"})
            return False
        else:
            return True

class Test3(Command):
    def __init__(self):
        pass

    def execute(self, context=None):
        print "Running test3 command"
        if context is not None:
            context.update({"test3":"complete"})
            return False
        else:
            return True



if __name__ == '__main__':

    chain = Chain()

    chain.addCommand(Test1())
    chain.addCommand(Test2())
    chain.addCommand(Test3())

    context = {}
    
    chain.execute(context)

    for (k,v) in context.iteritems():
        print "Found an entry: %s:%s" % (k,v)

    print "Done"
