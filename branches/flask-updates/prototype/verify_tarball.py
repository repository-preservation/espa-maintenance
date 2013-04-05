#!/usr/bin/env python
import sys, os
import commands

__author__ = 'D. Hill'
__date__ = 'February 8, 2013'

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: verify_tarballs.py directory_to_scan"
        sys.exit(1)
        
    tardir = sys.argv[1]
    if not os.path.isdir(tardir):
        print "Cannot read %s... exiting" % tardir
        sys.exit(1)
        
    os.chdir(tardir)
        
    for t in [x for x in os.listdir(os.getcwd()) if x.endswith('tar') or x.endswith('gz')]:
        cmd = 'tar -tf %s' % t
       
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            print ("%s=bad" % t)
    sys.exit(0)
    
