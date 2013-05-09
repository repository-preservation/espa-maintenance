#!/usr/local/bin/python

import json
import xmlrpclib
import commands
import socket
import sys
from cStringIO import StringIO


def logger(value):
    print("Mapper.py:%s" % value)

if __name__ == '__main__':
    processing_location = socket.gethostname()
    server = None
    sceneid = None   
    for line in sys.stdin:
        try:
            logger ("Processing input...")
            logger (line)
            line = str(line).replace("#", '')
            line = json.loads(line)
            orderid = line['orderid']
            sceneid = line['scene']

            if type(line['options']) == str or type(line['options']) == unicode:
                options = json.loads(line['options'])
            else:
                options = line['options']

            if line.has_key('xmlrpcurl'):    
                xmlrpcurl = line['xmlrpcurl']
            else:
                xmlrpcurl = None
        
            if (not sceneid.startswith('L')): 
                logger("sceneid did not start with L")
                continue;
        
            logger ("Processing %s" % sceneid)

            if xmlrpcurl is not None:
                server = xmlrpclib.ServerProxy(xmlrpcurl)
                server.updateStatus(sceneid, orderid,processing_location, 'processing')
       
            cmd = './espa.py ' 
            cmd = cmd + '--scene %s ' % sceneid
            cmd = cmd + '--order %s ' % orderid
            
            if options.has_key('include_sr') and options['include_sr'] == True:
                cmd = cmd + '--surface_reflectance ' 
            if options.has_key('include_sr_browse') and options['include_sr_browse'] == True:
                cmd = cmd + '--sr_browse '
                if options.has_key('browse_resolution'):
                    cmd = cmd + '--browse_resolution %s ' % options['browse_resolution']     
            if options.has_key('include_sr_ndvi') and options['include_sr_ndvi'] == True:
                cmd = cmd + '--sr_ndvi ' 
            if options.has_key('include_solr_index') and options['include_solr_index'] == True:
                cmd = cmd + '--solr ' 
            if options.has_key('include_sr_thermal') and options['include_sr_thermal'] == True:
                cmd = cmd + '--band6 ' 
            if options.has_key('include_sr_toa') and options['include_sr_toa'] == True:
                cmd = cmd + '--toa ' 
            if options.has_key('include_sourcefile') and options['include_sourcefile'] == True:
                cmd = cmd + '--sourcefile '
            if options.has_key('include_source_metadata') and options['include_source_metadata'] == True:
                cmd = cmd + '--source_metadata '
            if options.has_key('include_cfmask') and options['include_cfmask'] == True:
                cmd = cmd + '--cfmask '
            if options.has_key('source_host'):
                cmd = cmd + '--source_host %s ' % options['source_host']
            else:
                cmd = cmd + '--source_host edclpdsftp.cr.usgs.gov '
            if options.has_key('destination_host'):
                cmd = cmd + '--destination_host %s ' % options['destination_host']
            else:
                cmd = cmd + '--destination_host edclpdsftp.cr.usgs.gov ' 
            if options.has_key('source_type'):
                cmd = cmd + '--source_type %s ' % options['source_type']
            else:
                cmd = cmd + '--source_type level1 '
            if options.has_key('source_directory'):
                cmd = cmd + '--source_directory %s ' % options['source_directory']
            if options.has_key('destination_directory'):
                cmd = cmd + '--destination_directory %s ' % options['destination_directory']

            logger ("Running command:%s" % cmd)    
            h = open("/tmp/cmd_debug.txt", "wb+")
            h.write(cmd)
            h.flush()
            h.close()

            status,output = commands.getstatusoutput(cmd)
            if status != 0:
                logger ("Error occurred processing %s" % sceneid)
                logger ("%s returned code:%s" % (sceneid, status))
                if server is not None:
                    server.setSceneError(sceneid, orderid, processing_location, output)
                else:
                    logger(output)
            else:
                logger ("Processing complete for %s" % sceneid)
                #where the hell do i get the completed_scene_location and source_l1t_location from?
                #04-27-13 - From the standard out
                #espa:result=[/tmp/bam/LT50290302007097PAC01-sr.tar.gz, /tmp/bam/LT50290302007097PAC01-sr.cksum]
                
                if server is not None:
                    b = StringIO(output)
                    status_line = [f for f in b.readlines() if f.startswith("espa.result")]
                                        
                    #logger ("status_line%s" % status_line)
                    #logger ("status_line_len:%i" % len(status_line))

                    if len(status_line) == 1:
                        #print ("Loading %s into JSON" % (status_line[0]))

                        myjson = status_line[0].split('=')[1]
                        #print ("MyJSON:%s" % myjson)        

                        data = json.loads(myjson)
                        #print data

                        #completed_scene_location = '/data2/LSRD/orders/%s/%s-sr.tar.gz' % (orderid,sceneid)
                        completed_scene_location = data['destination_file']
                        cksum_file_location = data['destination_cksum_file']
                        
                        server.markSceneComplete(sceneid,orderid,processing_location,completed_scene_location,cksum_file_location,"")
                    else:
                        raise Exception("Did not receive a distribution location or cksum file location for:%s.  Status line was:%s\n.  Log:%s" % (sceneid,status_line, output))

        except Exception, e:
            logger ("An error occurred processing %s" % sceneid)
            logger (e)
            h = open('/tmp/%s-jobdebug.txt' % (sceneid), 'wb+')
            h.write("An error occurred processing %s" % sceneid)
            h.write(str(e))
            h.flush()
            h.close()
            if server is not None: 
                server.setSceneError(sceneid, orderid, processing_location, e)
        
        
     
    


