#!/usr/bin/env python
import commands
import json
from cStringIO import StringIO

__author__ = "David V. Hill"

if __name__ == '__main__':
    cmd = "./espa.py --scene LT50290302007097PAC01 --source_host espa@edclpdsftp.cr.usgs.gov --destination_directory /tmp/bam --sr_ndvi --destination_host localhost"
    status,output = commands.getstatusoutput(cmd)
    print ("Status:%s" % status)
    print ("Output:%s" % output)
    b = StringIO(output)
    status_line = [f for f in b.readlines() if f.startswith("espa.result")]

    print ("status_line:%s" % status_line)
    print ("status_line_len:%i" % len(status_line))
  
    if len(status_line) == 1:
        print ("Loading %s into JSON" % (status_line[0]))

        myjson = status_line[0].split('=')[1]
        print ("MyJSON:%s" % myjson)        

        data = json.loads(myjson)
        print data
        
    
