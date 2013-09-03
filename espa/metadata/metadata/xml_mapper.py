#!/usr/bin/env python

__author__ = "David V. Hill"

import cStringIO
import xml.etree.ElementTree as xml
import sys

def cleanResult(element):
    result = None
    if element is not None:
        result = element.text
        result = result.strip()
    else:
        result = ""
    return result

def process(val):
    #sys.stderr.write(val)
    
    #tree = xml.fromstring(val)
    #root = tree.getroot()
    root = xml.fromstring(val)
    
    sceneID = cleanResult(root.find('sceneID'))     
    browseURL = cleanResult(root.find('browseURL'))
    collectDate = cleanResult(root.find('acquisitionDate'))
    collectDate = collectDate + "T00:00:01Z"

    #this isn't always there.. ignore
    #updateDate = cleanResult(root.find('dateUpdated'))
    #this isn't always there.. ignore
    #updateDate = updateDate + "T00:00:01Z"
    
    sensor = cleanResult(root.find('sensor'))
    path = cleanResult(root.find('path'))
    row = cleanResult(root.find('row'))
    ulLat = cleanResult(root.find('upperLeftCornerLatitude'))
    ulLon = cleanResult(root.find('upperLeftCornerLongitude'))
    urLat = cleanResult(root.find('upperRightCornerLatitude'))
    urLon = cleanResult(root.find('upperRightCornerLongitude'))
    llLat = cleanResult(root.find('lowerLeftCornerLatitude'))
    llLon = cleanResult(root.find('lowerLeftCornerLongitude'))
    lrLat = cleanResult(root.find('lowerRightCornerLatitude'))
    lrLon = cleanResult(root.find('lowerRightCornerLongitude'))
    scLat = cleanResult(root.find('sceneCenterLatitude'))
    scLon = cleanResult(root.find('sceneCenterLongitude'))
    cc = cleanResult(root.find('cloudCover'))
    dOn = cleanResult(root.find('dayOrNight'))
    sElev = cleanResult(root.find('sunElevation'))
    sAzim = cleanResult(root.find('sunAzimuth'))
    station = cleanResult(root.find('receivingStation'))
    qual1 = cleanResult(root.find('imageQuality1'))
    qual2 = cleanResult(root.find('imageQuality2'))

    ul = ("%s,%s") % (ulLat,ulLon)
    ur = ("%s,%s") % (urLat,urLon)
    ll = ("%s,%s") % (llLat,llLon)
    lr = ("%s,%s") % (lrLat,lrLon)
    sc = ("%s,%s") % (scLat,scLon)
    
    #add url for full res GIS ready browse
    #add field for collections this scene is a part of
    #consider pulling the browse now and storing it instead of indexing and storing just the url
    #nevermind, need to do that before we do the store operation in solr, not during index construction
    
    returnval = ("%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s") % (sceneID,browseURL,collectDate,sensor,path,row,ul,ur,ll,lr,sc,cc,dOn,sElev,sAzim,station,qual1,qual2,'EOL')
    return returnval.strip()
    #return ("%s\t%s\t%s\t%s") % (sceneID, browseURL, path, row)

if __name__ == '__main__':

    buff = None
    intext = False
    #buff = cStringIO.StringIO()    
    for line in sys.stdin:
        line = line.strip()
        if line.find("<metaData>") != -1:
            intext = True
            buff = cStringIO.StringIO()
            #buff.write("###")
            buff.write(line)
            #buff.write("\n")
        elif line.find("</metaData>") != -1:
            intext = False
            buff.write(line)
            #buff.write("####")
            #buff.write("\n")
            val = buff.getvalue()
            buff.close()
            #print val
            buff = None
            #try:

            print process(val)

            #except err:
            #    print err
            
        else:
            if intext:
                buff.write(line)
                #buff.write("\n")

    

        

    

    
