#!/usr/local/bin/python

import json
import xmlrpclib
import commands
import socket
import sys

if __name__ == '__main__':
    processing_location = socket.gethostname()
    server = None
    sceneid = None   
    for line in sys.stdin:
        try:
            print ("Processing input...")
            print (line)
            line = str(line).replace("#", '')
            line = json.loads(line)
            orderid = line['orderid']
            sceneid = line['scene']
        
            options = json.loads(line['options'])
            xmlrpcurl = line['xmlrpcurl']
        
            if (not sceneid.startswith('L')): 
                print("sceneid did not start with L")
                continue;
        
            print "Processing %s" % sceneid
        
            server = xmlrpclib.ServerProxy(xmlrpcurl)
            server.updateStatus(sceneid, orderid,processing_location, 'processing')
       
            cmd = './espa.py ' 
            cmd = cmd + '--scene %s ' % sceneid
            cmd = cmd + '--order %s ' % orderid
            
            if options.has_key('include_sr') and options['include_sr'] == True:
                cmd = cmd + '--include_surface_reflectance ' 
            if options.has_key('include_sr_browse') and options['include_sr_browse'] == True:
                cmd = cmd + '--sr_browse '
                if options.has_key('browse_resolution'):
                    cmd = cmd + '--browse_resolution %s ' % options['browse_resolution']
                    
            if options.has_key('include_sr_ndvi') and options['include_sr_ndvi'] == True:
                cmd = cmd + '--sr_ndvi ' 
            if options.has_key('include_solr_index') and options['include_solr_index'] == True:
                cmd = cmd + '--solr ' 
            if options.has_key('include_sr_thermal') and options['include_sr_thermal'] == True:
                cmd = cmd + '--band6thermal ' 
            if options.has_key('include_sr_toa') and options['include_sr_toa'] == True:
                cmd = cmd + '--toa ' 
            if options.has_key('include_sourcefile') and options['include_sourcefile'] == True:
                cmd = cmd + '--include_sourcefile '
            if options.has_key('include_source_metadata') and options['include_source_metadata'] == True:
                cmd = cmd + '--include_sourcefile_metadata '
            if options.has_key('include_cfmask') and options['include_cfmask'] == True:
                cmd = cmd + '--cfmask '
            cmd = cmd + '--source_host edclpdsftp.cr.usgs.gov '
            cmd = cmd + '--destination_host edclpdsftp.cr.usgs.gov ' 

            print ("Running command:%s" % cmd)    
            h = open("/tmp/cmd_debug.txt", "wb+")
            h.write(cmd)
            h.flush()
            h.close()

            status,output = commands.getstatusoutput(cmd)
            if status != 0:
                print ("Error occurred processing %s" % sceneid)
                if server is not None:
                    server.setSceneError(sceneid, orderid, processing_location, output)
            else:
                print ("Processing complete for %s" % sceneid)
                #where the hell do i get the completed_scene_location and source_l1t_location from?
                if server is not None:
                    completed_scene_location = '/data2/LSRD/orders/%s/%s-sr.tar.gz' % (orderid,sceneid)
                    #source_l1t_location = '/data2/LSRD/orders/%s/%s.tar.gz' % (orderid, sceneid)
                    source_l1t_location = 'not distributed'
                    server.markSceneComplete(sceneid,orderid,processing_location,completed_scene_location,source_l1t_location,"")

        except Exception, e:
            print ("An error occurred processing %s" % sceneid)
            print e
            h = open('/tmp/jobdebug.txt', 'wb+')
            h.write("An error occurred processing %s" % sceneid)
            h.write(str(e))
            h.flush()
            h.close()
            if server is not None: 
                server.setSceneError(sceneid, orderid, processing_location, e)
        
        
     
    


