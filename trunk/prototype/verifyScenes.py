#!/usr/bin/env python

import urllib2
from cStringIO import StringIO

if __name__ == '__main__':
    #base_url = 'http://eedev.cr.usgs.gov/OrderWrapperServicedevsys/resources/'
    base_url = 'http://eedevmast.cr.usgs.gov/OrderWrapperServicedevmast/resources/'

    operation = 'verifyScenes'

    request_url = base_url + operation
    
    sb = StringIO()
    sb.write("<?xml version ='1.0' encoding='UTF-8' ?>")
    sb.write("<sceneList xmlns='http://earthexplorer.usgs.gov/schema/sceneList' ")
    sb.write("xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' ")
    sb.write("xsi:schemaLocation='http://earthexplorer.usgs.gov/schema/sceneList ")
    sb.write("http://earthexplorer.usgs.gov/EE/sceneList.xsd'>")
    sb.write("<sceneId sensor='LANDSAT_ETM'>LE70290302003142EDC00</sceneId>")
    sb.write("<sceneId sensor='LANDSAT_ETM'>LE70290302003046EDC00</sceneId>")
    sb.write("<sceneId sensor='LANDSAT_ETM'>LE70290302003030EDC01</sceneId>")
    sb.write("<sceneId sensor='LANDSAT_ETM'>LE70290302003014EDC00</sceneId>")
    sb.write("<sceneId sensor='LANDSAT_ETM'>LE70290302003126EDC0</sceneId>")
    sb.write("</sceneList>")
    
    request_body = sb.getvalue()

    headers = dict()
    headers['Content-Type'] = 'application/xml'
    headers['Content-Length'] = len(request_body)

    request = urllib2.Request(request_url, request_body, headers)

    h = urllib2.urlopen(request)
    
    code = h.getcode()
    response = None
    if code == 200:
        response = h.read()
    else:
        print code

    h.close()

    print ("Response code:%s" % str(code))
    print ("Response:")
    print response
    
    
              
