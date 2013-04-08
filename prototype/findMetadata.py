#!/usr/bin/env python

import os

if __name__ == '__main__':
    for root,dirs,files in os.walk('/home/dhill'):
        for f in files:
	    if f.startswith("REANALYSIS_"):
                parts = f.split("REANALYSIS_")
                year = parts[1][0:4]
                day = parts[1][4:7]
                print ("%s\t%s\t%s/%s") % (year,day,root,f)
