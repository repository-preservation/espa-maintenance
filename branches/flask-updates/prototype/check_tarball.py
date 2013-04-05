#!/usr/bin/env python
#
#
#
#
import sys
import gzip
import tarfile
import os
import commands

global BUFFSIZE

BUFFSIZE = 10240000

def print_usage():
    print
    print " Usage: ", sys.argv[0], "/path/to/tarball.tar.gz"
    print
    sys.exit(1)
    
def is_valid_tarball_1(file):
    global BUFFSIZE
    
    #First check gzip compression
    try:
        g = gzip.GzipFile(file, 'rb')
        
        buffer = g.read(BUFFSIZE)
    except:
        # Not a gzip file Error Catch
        return False
    
    try:
        #n=0
        while buffer:
            #n += 1
            #print "gzip buffer", int(n)
            for l in buffer:
                continue
            buffer = g.read(BUFFSIZE)
    except IOError, e:
        # IO/CRC Error Catch
        return False
    except:
        # Other Error Catch
        return False
        
    try:
        tarfile.is_tarfile(file)
        
        t = tarfile.open(file,'r:gz')
        #n = 0
        while True:
            #n += 1
            #print "tar loop ", n
            t = self.next()
            if t is None:
                raise StopIteration
        #buffer = t.read(BUFFSIZE)
        #n = 0
        #while buffer:
        #    n += 1
        #    print "tar buffer", int(n)
        #    for l in buffer:
        #        continue
        #    buffer = t.read(BUFFSIZE)
    except tarfile.TarError, e:
        # Catch-all Tar Error Catch
        #print "Tarerror:", e
        return False
    except tarfile.HeaderError, e:
        # Bad Tar Header Error Catch
        #print "headererror:", e
        return False
    except tarfile.ReadError, e:
        # Tar Read Error Catch
        #print "readerror:", e
        return False
    except:
        # Other Error Catch
        return False

    # If we make it through gzip/tar interrogation,
    # we're good to go
    return True

def is_valid_tarball_2(file):
    
    (retval, output) = commands.getstatusoutput("/usr/bin/gzip -t " + file)

    if int(retval) != 0:
        # Bad retval from gzip interrogation
        return False
    
    try:
        tarfile.is_tarfile(file)
        
        t = tarfile.open(file,'r:gz')
        #n = 0
        while True:
            #n += 1
            #print "tar loop ", n
            t = self.next()
            if t is None:
                raise StopIteration
    except tarfile.TarError, e:
        # Catch-all Tar Error Catch
        #print "Tarerror:", e
        return False
    except tarfile.HeaderError, e:
        # Bad Tar Header Error Catch
        #print "headererror:", e
        return False
    except tarfile.ReadError, e:
        # Tar Read Error Catch
        #print "readerror:", e
        return False
    except:
        # Other Error Catch
        return False

    # If we make it through gzip/tar interrogation,
    # we're good to go
    return True

def main():
    
    if len(sys.argv) - 1 == 1:
        tarball = sys.argv[1]
        
        if os.path.isfile(tarball):
            
            #if is_valid_tarball_1(tarball):
            #    print "It's good, capitan for option 1!\n"
            #else:
            #    print "Bad things for option 1, man!\n",
                
                
            if is_valid_tarball_2(tarball):
                print "It's good capitan for option 2!\n"
            else:
                print "Bad things for option 2, man!\n"
        else:
            print "Not a file, chump!"
    else:
        print_usage()   
    
if __name__ == '__main__':
    main()
