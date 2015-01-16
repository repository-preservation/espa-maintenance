import time
import multiprocessing
import fcntl

class Lock(object):
    
    def __init__(self, filename):
        self.filename = filename
        self.handle = open(filename, 'w')
        
    def acquire(self):
        fcntl.flock(self.handle, fcntl.LOCK_EX)
        
    def release(self):
        fcntl.flock(self.handle, fcntl.LOCK_UN)
        
    def __del__(self):
        self.handle.close()
             

def test_lock(who):
    
    lock = Lock('/tmp/mylock.tmp')
    try:
        lock.acquire()
    
        print("Doing stuff with %s..." % who)
        time.sleep(10)
        print("Done doing stuff with %s..." % who)
    finally:
        lock.release()
        
        
if __name__ == '__main__':
    pool = multiprocessing.Pool(5)
    inputs = ['a', 'b', 'c', 'd', 'e']
    print(pool.map(test_lock, inputs))


