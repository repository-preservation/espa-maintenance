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
        h.write("\n")
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
    

def build_albers_proj_string(std_parallel_1, std_parallel_2, origin_lat, central_meridian, false_easting, false_northing, datum):
    '''
    Builds a proj.4 string for albers equal area
    Example:
    +proj=aea +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs 
    '''
    
    proj_str = '+proj=aea +lat_1=%f +lat_2=%f +lat_0=%f +lon_0=%f +x_0=%f +y_0=%f +ellps=GRS80 +datum=%s +units=m +no_defs ' \
               % (std_parallel_1, std_parallel_2, origin_lat, central_meridian, false_easting, false_northing, datum)
    
    return proj_str


def build_utm_proj_string(utm_zone, utm_north_south):
    '''
    Builds a proj.4 string for utm
    Example:
    +proj=utm +zone=60 +ellps=WGS84 +datum=WGS84 +units=m +no_defs
    +proj=utm +zone=39 +south +ellps=WGS72 +towgs84=0,0,1.9,0,0,0.814,-0.38 +units=m +no_defs 
    '''
    proj_str = ''
    if str(utm_north_south).lower() == 'north':
        proj_str = '+proj=utm +zone=%i +ellps=WGS84 +datum=WGS84 +units=m +no_defs' % utm_zone
    elif str(utm_north_south).lower() == 'south':
        proj_str = '+proj=utm +zone=%i +south +ellps=WGS72 +towgs84=0,0,1.9,0,0,0.814,-0.38 +units=m +no_defs' % utm_zone
    else:
        raise ValueError("Invalid urm_north_south argument[%s] Argument must be one of 'north' or 'south'" % utm_north_south)
    return proj_str       
    
    

def build_sinu_proj_string(central_meridian, false_easting, false_northing):
    '''
    Builds a proj.4 string for sinusoidal (modis)
    Example:
    +proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +ellps=WGS84 +datum=WGS84 +units=m +no_defs 
    '''
    proj_str = '+proj=sinu +lon_0=%f +x_0=%f +y_0=%f +a=6371007.181 +b=6371007.181 +ellps=WGS84 +datum=WGS84 +units=m +no_defs' \
               % (central_meridian, false_easting, false_northing)
    
    return proj_str


def build_geographic_proj_string():
    '''
    Builds a proj.4 string for geographic
    Example:
    
    '''
    return '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'



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
        
            logger (sceneid, "Processing %s" % sceneid)

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
                
            if options.has_key('include_sr_msavi') and options['include_sr_msavi'] == True:
                cmd = cmd + '--sr_msavi '

            if options.has_key('include_sr_evi') and options['include_sr_evi'] == True:
                cmd = cmd + '--sr_evi '

            if options.has_key('include_solr_index') and options['include_solr_index'] == True:
                cmd = cmd + '--solr ' 

            if options.has_key('include_sr_thermal') and options['include_sr_thermal'] == True:
                cmd = cmd + '--band6 ' 

            if options.has_key('include_sr_toa') and options['include_sr_toa'] == True:
                cmd = cmd + '--toa ' 

            if options.has_key('include_swe') and options['include_swe'] == True:
                cmd = cmd + '--surface_water_extent ' 

            if options.has_key('include_sca') and options['include_sca'] == True:
                cmd = cmd + '--snow_covered_area ' 

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
                
            if options.has_key('reproject') and options['reproject'] == True:
                target_proj = str(options['target_projection']).lower()
            
                if target_proj == "sinu":
                    proj = build_sinu_proj_string(float(options['central_meridian']),
                                           float(options['false_easting']),
                                           float(options['false_northing']))
                    cmd += " --projection '%s' " % proj
                    
                elif target_proj == "aea":
                    proj = build_albers_proj_string(float(options['std_parallel_1']),
                                                    float(options['std_parallel_2']),
                                                    float(options['origin_lat']),
                                                    float(options['central_meridian']),
                                                    float(options['false_easting']),
                                                    float(options['false_northing']),
                                                    options['datum'])
                    cmd += " --projection '%s' " % proj
                    
                elif target_proj == "utm":
                    proj = build_utm_proj_string(int(options['utm_zone']),
                                                 options['utm_north_south'])
                    cmd += " --projection '%s' " % proj
                elif target_proj == "lonlat":
                    cmd += " --projection '%s' " % build_geographic_proj_string()
                else:
                    logger(sceneid, "Unknown projection requested:%s" % target_proj)
            
            if options.has_key('image_extents') and options['image_extents'] == True:
                minx,miny,maxx,maxy = options['minx'], options['miny'], options['maxx'], options['maxy']
                cmd += ' --set_image_extent %s,%s,%s,%s ' % (minx,miny,maxx,maxy)
            
            pixel_size, pixel_unit = None, None
            #See if the user requested a pixel size. If so set and use it.
            if (options.has_key('resize') and options['resize'] == True):
                pixel_size = options['pixel_size']
                pixel_unit = options['pixel_size_units']

            #somebody asked for reproject or extents but didn't specify a pixel size.
            #default to 30 meters or dd equivalent.  Everything will default to 30 meters
            #except if they chose geographic projection.  Then its dd equivalent.
            elif (options.has_key('reproject') and options['reproject'] == True) or \
                 (options.has_key('image_extents') and options['image_extents'] == True):
                if options.has_key('target_projection') and options['target_projection']:
                    if str(options['target_projection']).lower() == 'lonlat':
                        pixel_size = .0002695
                        pixel_unit = 'dd'
                    else:
                        pixel_size = 30.0
                        pixel_unit = 'meters'
                else:
                    pixel_size = 30.0
                    pixel_unit = 'meters'
                    
            if pixel_size and pixel_unit:
                  cmd += ' --pixel_size %s --pixel_unit %s' % (pixel_size, pixel_unit)

            if options.has_key('resample_method'):
                cmd += ' --resample_method %s ' % options['resample_method']
            
            
            #this is for debugging only
            #import sys
            #print "Command"
            #print cmd
            #print "Quitting"
            #sys.exit(1)
            #end debugging

            logger(sceneid, "Running command:%s" % cmd)    
            #h = open("/tmp/%s-cmd_debug.txt" % sceneid, "wb+")
            #h.write(cmd)
            #h.flush()
            #h.close()

            #right here is where we are not picking up the fact that there is an error, probably because cdr_ecv.py is returning a tuple
            status,output = commands.getstatusoutput(cmd)

            logger(sceneid, "Status return from cdr_ecv:%s" % status)
            
            if status != 0 and status != 256:
                logger (sceneid, "Error occurred processing %s" % sceneid)
                logger (sceneid, "%s returned code:%s" % (sceneid, status))
                if server is not None:
                    if os.path.exists(get_logfile(sceneid)):
                        with open(get_logfile(sceneid), "r+") as h:
                            data = h.read()                            
                        os.unlink(get_logfile(sceneid))
                        output = output + "\n" + data
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
                        server.markSceneComplete(sceneid, orderid, processing_location, completed_scene_location, cksum_file_location, "")
                    else:
                        raise Exception("Did not receive a distribution location or cksum file location for:%s.\n Status code:%s\n  Status line:%s\n.  Log:%s" % (sceneid,status, status_line, output))
                
                if os.path.exists(get_logfile(sceneid)):
                    os.unlink(get_logfile(sceneid))
                
        except Exception, e:
            logger (sceneid, "An error occurred processing %s" % sceneid)
            logger (sceneid, str(e))
            if server is not None: 
                server.setSceneError(sceneid, orderid, processing_location, e)
