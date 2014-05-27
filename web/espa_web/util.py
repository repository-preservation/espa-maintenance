import time

class Timer:
    '''Class to support closures, allowing Python code to be timed using the 
    "with" statement.
    
    Example:
    with Timer() as t:
        do_something()
        do_something_else()
        
    print("Something and something_else took %f seconds" % t.interval)
    '''
    
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start