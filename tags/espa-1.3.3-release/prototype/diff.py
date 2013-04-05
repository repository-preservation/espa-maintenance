#!/usr/bin/env python
'''
This is a script to assist in generating the 
gls scene list that needs to be run.  
full.txt in this case is the complete listing
of all gls scenes for a given year as collected
from the landsat online cache.  complete.txt is 
a file matching the format of full.txt of the 
scenes that have already been run.  

The results of this script should be redirected
to a file off the command line:
python ./diff.py > newscenelist.txt

DVH - May 10, 2012
'''
complete_handle = open('complete.txt', 'rb+')
complete_data = complete_handle.readlines()
complete_handle.close()

full_handle = open('full.txt', 'rb+')
full_data = full_handle.readlines()
full_handle.close()


for f in full_data:
    try:
        complete_data.index(f)
        #print "%s was found" % f.strip()
    except:
        #print "%s NOT found" % f.strip()
        print f.strip()
