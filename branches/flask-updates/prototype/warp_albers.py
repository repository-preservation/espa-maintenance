#!/usr/bin/env python
import commands, sys
from StringIO import StringIO

def usage():
    usage_string = "warp_albers.py source_file destination_file.tif"
    print usage_string

if __name__ == "__main__":
    if len(sys.argv) != 3:
        usage()
        sys.exit(-1)
 
    sourcefile = sys.argv[1]
    destfile =   sys.argv[2]
    
        
    proj_str='+proj=aea +lat_0=23 +lon_0=-96 +lat_1=29.3 +lat_2=45.3'
    cmd = 'gdalwarp -wm 2048 -t_srs "%s" %s %s' % (proj_str, sourcefile, destfile)
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        print ("An error occurred warping %s to albers" % sourcefile)
        print output
        sys.exit(-1)
    print ("%s ok" % destfile)
    sys.exit(status)
