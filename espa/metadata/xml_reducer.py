#!/usr/bin/env python

__author__ = "David V. Hill"

import sys
import cStringIO
import xml.etree.ElementTree as xml


if __name__ == '__main__':
    output = set()
 
    for line in sys.stdin:
        output.add(line.strip())
        #print line.strip()
        
    for item in output:
        print item.strip()

    

        

    

    
