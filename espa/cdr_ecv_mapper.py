#!/usr/local/bin/python

'''
    FILE: cdr_ecv_mapper.py

    PURPOSE: Hadoop 'mapper' script used to map inputs to execution and processing of those
             inputs.

    PROJECT: Land Satellites Data Systems Science Research and Development (LSRD) at the
             USGS EROS

    LICENSE: NASA Open Source Agreement 1.3

    ORIGINAL AUTHOR:  David V. Hill
    
    NOTES:

'''

import os
import datetime
import json
import xmlrpclib
import commands
import socket
import sys
import random
import commands
import util
from cStringIO import StringIO

def get_logfile(sceneid):
    return '/tmp/%s-jobdebug.txt' % sceneid

def init_logfile(sceneid):
    f = get_logfile(sceneid)
    if os.path.isfile(f):
        os.unlink(f)

def logger(sceneid, value):
    with open(get_logfile(sceneid), 'a+') as h:
        h.write(util.build_log_msg('CDR_ECV_MAPPER', value))
        h.flush()
    
def get_cache_hostname():
    '''Poor mans load balancer for accessing the online cache over the private network'''
    hostlist = ['edclxs67p', 'edclxs140p']
    hostname = random.choice(hostlist)

    #Check that the host is up before returning
    cmd = "ping -q -c 1 %s" % hostname
    status,output = commands.getstatusoutput(cmd)

    #This looks nice but it might blow up if both hosts are down    
    if status == 0:
        return hostname
    else:
        return [x for x in hostlist if x is not hostname][0]    
    
    

if __name__ == '__main__':
    processing_location = socket.gethostname()
    server,sceneid = None, None
    
    for line in sys.stdin:
        try:
            line = str(line).replace("#", '')
            line = json.loads(line)
            orderid, sceneid = line['orderid'], line['scene']

            #get a fresh logfile in /tmp
            init_logfile(sceneid)
            
            if type(line['options']) in (str, unicode):
                options = json.loads(line['options'])
            else:
                options = line['options']

            if line.has_key('xmlrpcurl'):    
                xmlrpcurl = line['xmlrpcurl']
            else:
                xmlrpcurl = None
        
            if (not sceneid.startswith('L')): 
                logger(sceneid, "sceneid did not start with L")
                continue;
        
            logger ("Processing %s" % sceneid)

            if xmlrpcurl is not None:
                server = xmlrpclib.ServerProxy(xmlrpcurl)
                server.updateStatus(sceneid, orderid,processing_location, 'processing')
       
            cmd = './cdr_ecv.py ' 
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

            if options.has_key('include_sr_ndmi') and options['include_sr_ndmi'] == True:
                cmd = cmd + '--sr_ndmi '

            if options.has_key('include_sr_nbr') and options['include_sr_nbr'] == True:
                cmd = cmd + '--sr_nbr '

            if options.has_key('include_sr_nbr2') and options['include_sr_nbr2'] == True:
                cmd = cmd + '--sr_nbr2 '

            if options.has_key('include_sr_savi') and options['include_sr_savi'] == True:
                cmd = cmd + '--sr_savi '

            if options.has_key('include_sr_evi') and options['include_sr_evi'] == True:
                cmd = cmd + '--sr_evi '

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
                cmd = cmd + '--source_host %s ' % get_cache_hostname()

            if options.has_key('destination_host'):
                cmd = cmd + '--destination_host %s ' % options['destination_host']
            else:
                cmd = cmd + '--destination_host %s ' % get_cache_hostname()

            if options.has_key('source_type'):
                cmd = cmd + '--source_type %s ' % options['source_type']
            else:
                cmd = cmd + '--source_type level1 '

            if options.has_key('source_directory'):
                cmd = cmd + '--source_directory %s ' % options['source_directory']

            if options.has_key('destination_directory'):
                cmd = cmd + '--destination_directory %s ' % options['destination_directory']

            #logger ("Running command:%s" % cmd)    
            #h = open("/tmp/cmd_debug.txt", "wb+")
            #h.write(cmd)
            #h.flush()
            #h.close()

            status,output = commands.getstatusoutput(cmd)
            if status != 0:
                logger (sceneid, "Error occurred processing %s" % sceneid)
                logger (sceneid, "%s returned code:%s" % (sceneid, status))
                if server is not None:
                    server.setSceneError(sceneid, orderid, processing_location, output)
                else:
                    logger(sceneid, output)
            else:
                logger (sceneid, "Processing complete for %s" % sceneid)
                #where the hell do i get the completed_scene_location and source_l1t_location from?
                #04-27-13 - From the standard out
                #espa:result=[/tmp/bam/LT50290302007097PAC01-sr.tar.gz, /tmp/bam/LT50290302007097PAC01-sr.cksum]
                
                if server is not None:
                    b = StringIO(output)
                    status_line = [f for f in b.readlines() if f.startswith("espa.result")]
                                        
                    if len(status_line) >= 1:
                        myjson = status_line[0].split('=')[1]
                        data = json.loads(myjson)
                        completed_scene_location = data['destination_file']
                        cksum_file_location = data['destination_cksum_file']
                        server.markSceneComplete(sceneid,orderid,processing_location,completed_scene_location,cksum_file_location,"")
                    else:
                        raise Exception("Did not receive a distribution location or cksum file location for:%s.  Status line was:%s\n.  Log:%s" % (sceneid,status_line, output))

        except Exception, e:
            logger (sceneid, "An error occurred processing %s" % sceneid)
            logger (sceneid, str(e))
            if server is not None: 
                server.setSceneError(sceneid, orderid, processing_location, e)
