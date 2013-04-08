#!/usr/bin/env python
import os
import sys

basedir = '/home/dhill/imgstack/grid'


#results = list()
#xy = [(x, y) for x in range(-89,90) for y in range(-179,180)]
def latList():

    latlist = list()

    for x in range(-89,90):
        if x == 0:
            latlist.append('0N')
            latlist.append('0S')
            continue

        NS = 'S' if x < 1 else 'N'
        val = ('%i%s') % (abs(x), NS)
        latlist.append(val)
        
    return latlist

def lonList():

    lonlist = list()

    for y in range(-179,180):
        if y == 0:
            lonlist.append('0E')
            lonlist.append('0W')
            continue
        EW = 'W' if y < 1 else 'E'
        val = ('%i%s') % (abs(y), EW)
        lonlist.append(val)
        
    return lonlist

for x in latList():
    for y in lonList():
        out = ("%s%s") % (x,y)
        os.makedirs(os.path.join(basedir,out))
        #print out

#[os.makedirs(os.path.join(basedir,x)) for x in results]'''
