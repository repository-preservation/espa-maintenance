#!/usr/bin/env python
import json
from urllib2 import urlopen
import os, sys
from StringIO import StringIO

def frange(start,end,step):
    return map(lambda x: x*step, range(int(start*1./step),int(end*1./step)))


def getPoints(startX, stopX, startY, stopY, step):
    
    retVal = []
    for x in frange(startY, stopY, step):
        for y in frange(startX, stopX, step):
            retVal.append(("%f,%f") % (round(x,6), round(y,6)))
    retVal.append(("%f,%f") % (round(startY,6), round(startX,6)))
    retVal.append(("%f,%f") % (round(stopY,6), round(stopX,6)))
    return retVal

def buildMatrix(yx1, yx2, yx3, yx4):
    y1,x1 = yx1.split(',')
    y2,x2 = yx2.split(',')
    y3,x3 = yx3.split(',')
    y4,x4 = yx4.split(',')
    
    xlist = [float(x1),float(x2),float(x3),float(x4)]
    ylist = [float(y1),float(y2),float(y3),float(y4)]
    xmin = min(xlist)
    xmax = max(xlist)
    ymin = min(ylist)
    ymax = max(ylist)

    #the last value determines how many points will be created for the matrix.  For a typical
    #landsat scene .05 (of latitude/longitude) gives us about 1700 points per scene.  If it's not
    #sufficient make this a smaller number and rebuild the index
    result = getPoints(xmin, xmax, ymin, ymax, 0.05)
    return result

def getCurrentIndex():
    url = "http://espa.cr.usgs.gov/solr/select?q=*:*&rows=25000&wt=json"
    data = None
    if not os.path.exists('solr_index.py'):
        handle = urlopen(url)
        fhandle = open('solr_index.py', 'wb+')
        data = handle.read()
        fhandle.write(data)
        handle.close()
        fhandle.close()
    else:
        fhandle = open('solr_index.py', 'rb+')
        data = fhandle.read()
        fhandle.close()
    return data




def writeNewIndexRecord(doc):

    
    matrix = buildMatrix(doc['upperRightCornerLatLong'],doc['upperLeftCornerLatLong'], doc['lowerLeftCornerLatLong'], doc['lowerRightCornerLatLong']) 
    sb = StringIO()
    sb.write("<add><doc>\n")
    sb.write("<field name='sceneid'>%s</field>\n" % doc['sceneid'])
    sb.write("<field name='path'>%s</field>\n" % doc['path'])
    sb.write("<field name='row'>%s</field>\n" % doc['row'])
    sb.write("<field name='sensor'>%s</field>\n" % doc['sensor'])
    sb.write("<field name='sunElevation'>%s</field>\n" % doc['sunElevation'])
    sb.write("<field name='sunAzimuth'>%s</field>\n" % doc['sunAzimuth'])
    sb.write("<field name='groundStation'>%s</field>\n" % doc['groundStation'])
    sb.write("<field name='acquisitionDate'>%s</field>\n" % doc['acquisitionDate'])
    sb.write("<field name='collection'>%s</field>\n" % doc['collection'])
    sb.write("<field name='upperRightCornerLatLong'>%s</field>\n" % doc['upperRightCornerLatLong'])
    sb.write("<field name='upperLeftCornerLatLong'>%s</field>\n" % doc['upperLeftCornerLatLong'])
    sb.write("<field name='lowerLeftCornerLatLong'>%s</field>\n" % doc['lowerLeftCornerLatLong'])
    sb.write("<field name='lowerRightCornerLatLong'>%s</field>\n" % doc['lowerRightCornerLatLong'])
    sb.write("<field name='sceneCenterLatLong'>%s</field>\n" % doc['sceneCenterLatLong'])

    for m in matrix:
        sb.write("<field name='latitude_longitude'>%s</field>\n" % m)
    sb.write("</doc></add>")
    

    dirname = 'new_indices'

    if not os.path.exists('./new_indices'):
        os.mkdir('./new_indices')
    
    filename = ("%s.xml") % doc['sceneid']
    fullpath = os.path.join(dirname, filename)

    sb.flush()

    fh = open(fullpath, 'wb+')
    fh.write(sb.getvalue())
    fh.flush()
    fh.close()
    sb.close()


    
if __name__ == '__main__':

    data = getCurrentIndex()
    if data is None:
        print "No current index was returned"
        exit
        
    index = json.loads(getCurrentIndex())
    doclist = index['response']['docs']
    #writeNewIndexRecord(doclist[0])
    for doc in doclist:
        writeNewIndexRecord(doc)
        #doc['sunElevation']
        #doc['path']
        #doc['row']
        #doc['sensor']
        #doc['groundStation']
        #doc['acquisitionDate']
        #doc['sunAzimuth']
        #doc['collection']
        #doc['sceneid']
        #doc['upperRightCornerLatLong']
        #doc['upperLeftCornerLatLong']
        #doc['sceneCenterLatLong']
        #doc['lowerRightCornerLatLong']
        #doc['lowerLeftCornerLatLong']
        #doc['query_points'] = buildMatrix

        
    #print index
