#!/usr/bin/env python
import sys

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print ("Usage: makeGLSInputFile.py glsYear inputfilename")
    else:

        year = sys.argv[1]
        inputfile = sys.argv[2]
        
        inhandle = open(inputfile, 'r+')
        for line in inhandle.readlines():
            line = line.replace('\n', '').strip()    
            outval = ("%s\tgls-%s\t%s\n") % (line, year, year)
            print outval
        inhandle.close()
