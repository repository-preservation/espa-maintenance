#!/usr/local/bin/python

'''
    FILE: cdr_ecv.py

    PURPOSE: Integration script for the EROS Science Processing Architecture (ESPA)

    PROJECT: Land Satellites Data Systems Science Research and Development (LSRD) at the
             USGS EROS

    LICENSE: NASA Open Source Agreement 1.3

    ORIGINAL AUTHOR:  David V. Hill

    NOTES:

'''

import time
import commands
import os
import sys
import socket
import datetime
import random
import json
from osgeo import gdal
from optparse import OptionParser
from cStringIO import StringIO
import frange
import util

##############################################################################
class cdr_ecv_exit_codes:
    '''
    Description:
      A simple class/structure to organize exit return values.
    '''
    exit_success              =  0
    exit_failure              =  1
    environment               =  2
    creating_staging_dir      =  3
    creating_working_dir      =  4
    creating_output_dir       =  5
    file_transfer             =  6
    unpacking                 =  7
    ledaps                    =  8
    browse                    =  9
    spectral_indices          = 10
    solr                      = 11
    cfmask                    = 12
    cfmask_append             = 13
    warping                   = 14
    purging                   = 15
    packaging_distribution    = 16
    cleanup_output_dir        = 17
    dem                       = 18
    swe                       = 19
    sca                       = 20
# END class cdr_ecv_exit_codes


########################################################################################################################
# Utilities to control filenames
########################################################################################################################
def get_sr_filename(scene):
    """Return name of current sr product file"""
    return "lndsr.%s.hdf" % scene

def get_toa_filename(scene):
    """Return name of current sr-cal product file"""
    return "lndcal.%s.hdf" % scene

def get_b6thermal_filename(scene):
    """Return name of current sr-thermal product file"""
    return "lndth.%s.hdf" % scene

def get_sca_filename(scene):
    """Return name of current sca product file"""
    return "sca.%s.hdf" % scene

def get_cfmask_filename(scene):
    """Return name of current cfmask product file"""
    return "fmask.%s.hdf" % scene

########################################################################################################################
# parses metadata and puts it into an in memory dictionary
########################################################################################################################
def getMetaData(work_dir, debug=False):
    """Returns the scene metadata as a python dictionary"""

    #find the metadata file
    mtl_file = ''
    items = os.listdir(work_dir)
    for i in items:
        if not i.startswith('lnd') and (i.find('_MTL') > 0) and not (i.find('old') > 0):
            mtl_file = i
            util.log("CDR_ECV", "Located MTL file:%s" % mtl_file)
            break

    if mtl_file == '':
        util.log("CDR_ECV", "Could not locate the landsat MTL file in %s" % work_dir)
        return None
         
    current_dir = os.getcwd()
    os.chdir(work_dir)
    f = open(mtl_file, 'r')
    data = f.readlines()
    f.close()
    
    #this will fix the problem ledaps has with binary characters at the end
    #of some of the gls metadata files
    length = len(data)
    buff = StringIO()
    
    count = 1
    for d in data:
        if count < length:
            buff.write(d)
            count = count + 1
    
    #fix the stupid error where the metadata txt file is named TIF
    mtl_file = mtl_file.replace('.TIF', '.txt')
            
    f = open(mtl_file, 'w+')
    fixedmeta = buff.getvalue()
    f.write(fixedmeta)
    f.flush()
    f.close()
    buff.close()
    os.chdir(current_dir)
        
    #now we are going to read all the metadata into the context{} as
    #a dictionary.  Needed later for generating the solr index et. al.
    metadata = {}
    fixedmeta = fixedmeta.split('\n')
    for line in fixedmeta:
        line = line.strip()
        if not line.startswith('END') and not line.startswith('GROUP'):
            parts = line.split('=')
            if len(parts) == 2:
                metadata[parts[0].strip()] = parts[1].strip().replace('"', '')

    metadata['mtl_file'] = mtl_file
         
    return metadata


########################################################################################################################
# Generates browse from surface reflectance
########################################################################################################################
def makeBrowse(work_dir,metadata, scene_name,resolution=50,debug=False):
    """Creates a browse image for the current scene"""
    
    util.log("CDR_ECV", "Executing MakeBrowse()")
                    
    try:
        extrasdir = work_dir
        output_file = "%s-sr-browse.tif" % scene_name
        output_file = os.path.join(extrasdir, output_file)
        
        if not os.path.exists(extrasdir):
            os.makedirs(extrasdir)

        util.convertHDFToGTiff("%s/lndsr*hdf" % work_dir, "%s/out.tiff" % extrasdir)
        
        cmds = []
        #scale the images to 8 bit range
        cmds.append(('gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF %s/out.tiff5 %s/browse.tiff5') % (extrasdir, extrasdir))
        cmds.append(('gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF %s/out.tiff4 %s/browse.tiff4') % (extrasdir,extrasdir))
        cmds.append(('gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF %s/out.tiff3 %s/browse.tiff3') % (extrasdir,extrasdir))

        #create the 3 band composite
        cmds.append(('gdal_merge_simple -in %s/browse.tiff5 -in %s/browse.tiff4 -in %s/browse.tiff3 -out %s/final.tif') % (extrasdir,extrasdir,extrasdir,extrasdir))

        #project to geographic
        cmds.append(('gdalwarp -dstalpha -srcnodata 0 -t_srs EPSG:4326 %s/final.tif %s/warped.tif') % (extrasdir,extrasdir))

        #resize and rename
        cmds.append(('gdal_translate -co COMPRESS=DEFLATE -co PREDICTOR=2 -outsize %s%% %s%% -a_nodata -9999 -of GTIFF %s/warped.tif %s') % (resolution,resolution,extrasdir, output_file))

        #cleanup        
        cmds.append(('rm -rf %s/warped.tif') % (extrasdir))
        cmds.append(('rm -rf %s/*tiff*') % (extrasdir))
        cmds.append(('rm -rf %s/*out*') % (extrasdir))
        cmds.append(('rm -rf %s/final.tif') % (extrasdir))
                
        for cmd in cmds:
            if debug:
                util.log("CDR_ECV", "Running:%s" % cmd)
            status,output = commands.getstatusoutput(cmd)
            if status != 0:
                util.log("CDR_ECV", "Error occurred running:%s" % cmd)
                util.log("CDR_ECV", output)
                return status
                                        
        util.log("CDR_ECV", "MakeBrowse() complete...")
    except Exception,e:
        util.log("CDR_ECV", e)
        return -1
    finally:
        pass
    return 0

########################################################################################################################
# Builds a solr index
########################################################################################################################
def makeSolrIndex(metadata, scene, work_dir, collection_name,debug=False):
    """Generates a solr index for the current scene"""
    
    try:
        util.log("CDR_ECV", "Executing MakeSolrIndex() for %s" % scene)
            
                            
        #get the acquisition date... account for landsat changes
        acquisitionDate = None    
        if metadata.has_key('DATE_ACQUIRED'):
            acquisitionDate = metadata['DATE_ACQUIRED'] + "T00:00:01Z"
        else:
            acquisitionDate = metadata['ACQUISITION_DATE'] + "T00:00:01Z"

        #this is a fix for the changes to landsat metadata... currently have mixed versions on the cache
        row = None        
        if metadata.has_key("WRS_ROW"):
            row = metadata['WRS_ROW']
        else:
            row = metadata['STARTING_ROW']
                          
        sensor = metadata['SENSOR_ID']
        path = metadata['WRS_PATH']

        #deal with the landsat metadata fieldname changes
        if metadata.has_key('CORNER_UL_LAT_PRODUCT'):
            upper_left_LL = "%s,%s" % (metadata['CORNER_UL_LAT_PRODUCT'], metadata['CORNER_UL_LON_PRODUCT'])
            upper_right_LL = "%s,%s" % (metadata['CORNER_UR_LAT_PRODUCT'], metadata['CORNER_UR_LON_PRODUCT'])
            lower_left_LL = "%s,%s" % (metadata['CORNER_LL_LAT_PRODUCT'], metadata['CORNER_LL_LON_PRODUCT'])
            lower_right_LL = "%s,%s" % (metadata['CORNER_LR_LAT_PRODUCT'], metadata['CORNER_LR_LON_PRODUCT'])
            
        else:
            upper_left_LL = "%s,%s" % (metadata['PRODUCT_UL_CORNER_LAT'], metadata['PRODUCT_UL_CORNER_LON'])
            upper_right_LL = "%s,%s" % (metadata['PRODUCT_UR_CORNER_LAT'], metadata['PRODUCT_UR_CORNER_LON'])
            lower_left_LL = "%s,%s" % (metadata['PRODUCT_LL_CORNER_LAT'], metadata['PRODUCT_LL_CORNER_LON'])
            lower_right_LL = "%s,%s" % (metadata['PRODUCT_LR_CORNER_LAT'], metadata['PRODUCT_LR_CORNER_LON'])
            
        sun_elevation = metadata['SUN_ELEVATION']
        sun_azimuth = metadata['SUN_AZIMUTH']
        ground_station = metadata['STATION_ID']
        collection = collection_name
        
        #matrix = buildMatrix(doc['upperRightCornerLatLong'],doc['upperLeftCornerLatLong'], doc['lowerLeftCornerLatLong'], doc['lowerRightCornerLatLong'])
        matrix = util.buildMatrix(upper_right_LL, upper_left_LL, lower_left_LL, lower_right_LL)
        
        sb = StringIO()
        sb.write("<add><doc>\n")
        sb.write("<field name='sceneid'>%s</field>\n" % scene)
        sb.write("<field name='path'>%s</field>\n" % path)
        sb.write("<field name='row'>%s</field>\n" % row)
        sb.write("<field name='sensor'>%s</field>\n" % sensor)
        sb.write("<field name='sunElevation'>%s</field>\n" % sun_elevation)
        sb.write("<field name='sunAzimuth'>%s</field>\n" % sun_azimuth)
        sb.write("<field name='groundStation'>%s</field>\n" % ground_station)
        sb.write("<field name='acquisitionDate'>%s</field>\n" % acquisitionDate)
        sb.write("<field name='collection'>%s</field>\n" % collection)
        sb.write("<field name='upperRightCornerLatLong'>%s</field>\n" % upper_right_LL)
        sb.write("<field name='upperLeftCornerLatLong'>%s</field>\n" % upper_left_LL)
        sb.write("<field name='lowerLeftCornerLatLong'>%s</field>\n" % lower_left_LL)
        sb.write("<field name='lowerRightCornerLatLong'>%s</field>\n" % lower_right_LL)
        #sb.write("<field name='sceneCenterLatLong'>%s</field>\n" % doc['sceneCenterLatLong'])

        for m in matrix:
            sb.write("<field name='latitude_longitude'>%s</field>\n" % m)
        sb.write("</doc></add>")
       
        index_file = ("%s-index.xml") % scene
        index_file = os.path.join(work_dir, index_file)
    
        f = open(index_file, 'w')
        sb.flush()
        f.write(sb.getvalue())
        f.flush()
        f.close()
        sb.close()
    except Exception,e:
        util.log("CDR_ECV", e)
        return -1
    
    util.log("CDR_ECV", "MakeSolrIndex() complete...")
    return 0

########################################################################################################################
# Runs cfmask
########################################################################################################################
def make_cfmask(workdir):
    """Runs the routines necessary to apply cfmask processing against the current scene"""

    #go look for the toa reflectance file for input
    try:
        toa_file = None
        for f in os.listdir(workdir):
            if f.startswith('lndcal') and f.endswith('.hdf'):
                toa_file = f
                break
        if toa_file is None:
            raise IOError("Could not find LEDAPS TOA reflectance file in %s" % workdir)
        
        #status,output = commands.getstatusoutput("cd %s;cfmask --verbose --metadata=%s" % (workdir, metafile))
        status,output = commands.getstatusoutput("cd %s;cfmask --verbose --toarefl=%s" % (workdir, toa_file))
        status = status >> 8
        if status != 0:
            util.log("CDR_ECV", "Error producing cfmask for %s with status %s" % (toa_file, status))
            util.log("CDR_ECV", "CFMask output:%s" % output)
            util.log("CDR_ECV", "End of CFMask output")
            
            return (status, output)
    finally:
        pass
    return (0, '')


def get_hdf_global_metadata(workdir, hdf_file):
    cmd = "gdalinfo %s/%s" % (workdir,hdf_file)
    status,output = commands.getstatusoutput(cmd)
    sb = StringIO()
    
    if status == 0:
        intext = False
        for line in output.split("\n"):
            if str(line).strip().lower().startswith("metadata"):
                intext = True
                continue
            if (str(line).strip().lower().startswith("subdatasets") or str(line).strip().lower().startswith("corner")):
                intext = False
                break
            if intext:
                sb.write(line.strip())
                sb.write("\n")
            
        sb.flush()
        val  = sb.getvalue()
        sb.close()
        return val
            
            
########################################################################################################################
# Alters product extents, projections and pixel sizes
########################################################################################################################
def warp_outputs(workdir, projection=None, image_extents=None, pixel_size=None, pixel_unit=None, resample_method=None):
    '''
     projection = a valid proj.4 projection string
     image_extents = minx,miny,maxx,maxy
     pixel_size = value in units specified by pixel_units (meters or dd)
     pixel_units = meters or dd
     resample_method = near (default), bilinear, cubic, cubicspline, lanczos
    '''
    resample_methods = ('near', 'bilinear', 'cubic', 'cubicspline', 'lanczos')
    pixel_units = ('meters', 'dd')
   
 
    #local scope function to build a warp command for each item we need to warp
    def build_warp_command(sourcefile, outfile):
        cmd = "gdalwarp -wm 2048 -multi"
        
        if image_extents:
            minx,miny,maxx,maxy = image_extents.split(',')
            cmd += " -te %f %f %f %f" % (float(minx.strip()), float(miny.strip()), float(maxx.strip()), float(maxy.strip()))
        if pixel_size:
            cmd += " -tr %s %s" % (pixel_size,pixel_size)
        if projection:
            cmd += " -t_srs '%s'" % projection
        if resample_method:
            cmd += " -r %s" % resample_method
        cmd += " %s %s" % (sourcefile, outfile)
        return cmd

    #determines if there are hdf sds in an hdf
    def has_hdf_subdatasets(hdf_file):
        cmd = "gdalinfo %s" % hdf_file
        status,output = commands.getstatusoutput(cmd)
        if status >> 8 == 0:
            for line in output.split("\n"):
                if str(line).strip().lower().startswith("subdataset"):
                    return True
            return False
                    
    #finds all the subdataset names in an hdf file
    def parse_hdf_subdatasets(hdf_file):
        cmd = "gdalinfo %s" % hdf_file
        print cmd
        status,output = commands.getstatusoutput(cmd)
        if status >> 8 == 0:
            for line in output.split("\n"):
                if str(line).strip().lower().startswith("subdataset") and str(line).strip().lower().find("_name") != -1:
                    parts = line.split("=")
                    yield parts[0],parts[1]
    
    def run_warp(item, outitem):
        cmd = build_warp_command(item, outitem)
        util.log("CDR_ECV", "Warping %s in %s with %s" % (item, workdir, cmd))
        status,output = commands.getstatusoutput(cmd)
        if status != 0 and status != 256:
            util.log("CDR_ECV", "Error detected (status %s) warping output product[%s]:%s" % (status,item,output))
            return (1, item, output)
        else:
            return (0, item, outitem)
                
    try:
        if resample_method and resample_method not in resample_methods:
            raise ValueError("Resample method [%s] unsupported. Must be one of near, bilinear, cubic, cubicspline or lanczos" % resample_method)
        
        if pixel_unit and pixel_unit not in pixel_units:
            raise ValueError("Pixel unit [%s] unsupported.  Must be one of meters or dd" % pixel_unit)

        if image_extents:
            util.log("DEBUGGING", "Image extents:%s" % image_extents)
            try:
                minx,miny,maxx,maxy = image_extents.split(',')
                if float(minx.strip()) >= float(maxx.strip()):
                    raise ValueError("minx must be less than maxx")
                if float(miny.strip()) >= float(maxy.strip()):
                    raise ValueError("miny must be less than maxy")
            except:
                raise ValueError("image_extents must be specified as minx,miny,maxx,maxy")
               
        what_to_warp = []
        os.chdir(workdir)
        contents = os.listdir(workdir)
        
        what_to_warp = [x for x in contents if str(x).lower().endswith('hdf') or str(x).lower().find('tif') != -1]
        
        for item in what_to_warp:

            if item.lower().endswith('hdf'):

                hdfname = item.split(".hdf")[0]

                #copy over the global hdf metadata into a separate file
                util.log("CDR_ECV", "Retrieving global hdf metadata")
                md = get_hdf_global_metadata(workdir, item)
                if md is not None and len(md) > 0:
                    md_filename = "%s.txt" % hdfname
                    util.log("CDR_ECV", "Writing global metadata to %s" % md_filename)
                    h = open(md_filename, 'w+')
                    h.write(str(md))
                    h.flush()
                    h.close()

                
                
                if has_hdf_subdatasets(item):
                    #handle warping if the file is hdf    
                    for sds_desc,sds_name in parse_hdf_subdatasets(item):
                        sds_parts = sds_name.split(":")
                        outfilename = "%s-%s.tif" % (hdfname,sds_parts[len(sds_parts) - 1])
                        code,sds_item,out = run_warp(sds_name, outfilename)
                        if code >> 8 != 0:
                            util.log("CDR_ECV", "Error warping %s.  Error was %s" % (sds_item, out))
                            raise Exception("Error warping %s.  Error was %s" % (sds_item, out))
                else:
                    outfilename = "%s.tif" % hdfname
                    code, hdf_item, out = run_warp(item, outfilename)
                    if code >> 8 != 0:
                        util.log("CDR_ECV", "Error warping %s.  Error was %s" % (item, out))
                        raise Exception("Error warping %s.  Error was %s" % (item, out))
                    

                #wipe out the hdf's, don't need them anymore
                util.log("CDR_ECV", "Deleting intermediate raster products following warp")
                if os.path.exists(os.path.join(workdir,item)):
                    util.log("CDR_ECV", "Located intermediate raster:%s... deleting" % os.path.join(workdir,item))
                    os.unlink(os.path.join(workdir, item))
                else:
                    util.log("CDR_ECV", "Did not find intermediate product:%s... skipping delete" % os.path.join(workdir, item))

                hdr_file = ("%s.hdr" % os.path.join(workdir, item))
                util.log("CDR_ECV", "Deleting intermediate hdf header file:%s" % hdr_file)
                
                if os.path.exists(hdr_file):
                    util.log("CDR_ECV", "Located intermediate hdr file:%s... deleting" % hdr_file)
                    os.unlink(hdr_file)
                else:
                    util.log("CDR_ECV", "Did not find intermediate hdr file:%s... skipping delete" % hdr_file)
            else:
                #assume if its not hdf then it must be geotiff
                
                outfilename = "tmp-%s.tif" % item.split(".TIF")[0].lower()
                code,item,out = run_warp(item, outfilename)
                if code >> 8 != 0:
                    util.log("CDR_ECV", "Error warping %s.  Error was %s" % (item, out))
                    raise Exception("Error warping %s.  Error was %s" % (item, out))

                if os.path.exists(item):
                    util.log("CDR_ECV", "Located intermediate geotiff file:%s... deleting" % item)
                    os.unlink(item)
                else:
                    util.log("CDR_ECV", "Did not find intermediate geotiff file:%s... skipping delete" % item)

                os.rename(outfilename, item)
            
        return (0,'')
    except Exception, e:
        util.log("CDR_ECV", "Exception in warp_outputs()")
        util.log("CDR_ECV", e)
        util.log("CDR_ECV", "Error in warp_outputs():%s" % e)
        
        return (2, e)

########################################################################################################################
# package_product
# Purpose: builds a tar product file from all the requested outputs
########################################################################################################################
def package_product(product_dir, output_dir, product_filename):
    """Creates the CDR_ECV product file from the specified product directory."""
    
    product_file_full_path = os.path.join(outputdir, product_filename)

    #delete any old ones if they are there
    for l in os.listdir(outputdir):
        if l.startswith(product_filename):
            full_path = os.path.join(outputdir, l)
            os.remove(full_path)
                                  
    
    util.log("CDR_ECV", "Packaging completed product to %s.tar.gz" % (product_file_full_path))
    cmd = ("tar -cf %s.tar *") % (product_file_full_path)
    status, output = commands.getstatusoutput(cmd)
    os.chdir(orig_cwd)
    if status != 0 and status != 256:
        util.log("CDR_ECV", "Error packaging finished product to %s.tar" % (product_file_full_path))
        util.log("CDR_ECV", output)
        return (1,)

    os.chdir(output_dir)

    #this is now the product filename with path
    product_file_full_path = "%s.tar" % product_file_full_path
    
    #COMPRESS THE PRODUCT FILE
    cmd = ("gzip %s") % (product_file_full_path)
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        util.log("CDR_ECV", "Error compressing final product file:%s" % (product_file_full_path))
        util.log("CDR_ECV", output)
        return(2,)

    #it now has a .gz extension
    product_file_full_path = "%s.gz" % product_file_full_path
    
    #CHANGE FILE PERMISSIONS 
    util.log("CDR_ECV", "Changing file permissions on %s to 0644" % (product_file_full_path))
    os.chmod(product_file_full_path, 0644)
        
    #VERIFY THAT THE ARCHIVE IS GOOD
    cmd = ("tar -tf %s") % (product_file_full_path)
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        util.log("CDR_ECV", "Packaged product is an invalid tgz...")
        util.log("CDR_ECV", output)
        return(3,)

    #if it was good then create a checksum
    cmd = ("cksum %s")% (product_file_full_path)
    status, output = commands.getstatusoutput(cmd)
    if status != 0 and status != 256:
        util.log("CDR_ECV", "Couldn't generate cksum against:%s" % (product_file_full_path))
        util.log("CDR_ECV", output)
        return (4,)                   
    else:
        cksum_file = "%s.cksum" % product_filename
        prod_file = product_file_full_path.split('/')
        cksum_prod_filename = prod_file[len(prod_file) - 1]
        output = output.split()
        cksum_val = str("%s %s %s") % (str(output[0]), str(output[1]), str(cksum_prod_filename))
        util.log("CDR_ECV", "Generating cksum:%s" % cksum_val)
        h = open(cksum_file, 'wb+')
        h.write(cksum_val)
        h.flush()
        cksum_file_full_path = h.name
        h.close()
        return (0, cksum_val, product_file_full_path, cksum_file_full_path)  
    

########################################################################################################################
# distribute_product
# Purpose: Transfers the output product to the distribution location
########################################################################################################################
def distribute_product(product_file_full_path, cksum_file_full_path, destination_host, destination_file, destination_cksum_file):
    """Utility method to be called by do_distribution."""
    
    #MAKE DISTRIBUTION DIRECTORIES
    util.log("CDR_ECV", "Creating destination directories at %s" % destination_dir)
    cmd = "ssh -o StrictHostKeyChecking=no %s mkdir -p %s" % (destination_host, destination_dir)
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        util.log("CDR_ECV", "Error creating destination directory %s on %s" % (destination_dir,destination_host))
        util.log("CDR_ECV", output)
        return (1,)
    
    #DISTRIBUTE THE PRODUCT FILE
    util.log("CDR_ECV", "Transferring %s to %s:%s" % (product_file_full_path, destination_host, destination_file))   
    cmd = "scp -o StrictHostKeyChecking=no -c arcfour -pC %s %s:%s" % (product_file_full_path, destination_host, destination_file)       
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        util.log("CDR_ECV", "Error transferring %s to %s:%s..." % (product_file_full_path, destination_host, destination_file))
        util.log("CDR_ECV", output)
        return (2,)

    #DISTRIBUTE THE CHECKSUM
    util.log("CDR_ECV", "Transferring %s to %s:%s" % (cksum_file_full_path, destination_host, destination_cksum_file))
    cmd = "scp -o StrictHostKeyChecking=no -c arcfour -pC %s %s:%s" % (cksum_file_full_path, destination_host, destination_cksum_file)       
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        util.log("CDR_ECV", "Error transferring %s to %s:%s..." % (cksum_file_full_path, destination_host,destination_cksum_file))
        util.log("CDR_ECV", output)
        return (3,)
                    
    #CHECK DISTRIBUTED PRODUCT CHECKSUM
    cmd = "ssh -o StrictHostKeyChecking=no -q %s cksum %s" % (destination_host, destination_file)
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        #problem getting checksum
        util.log("CDR_ECV", "Error generating remote checksum for %s:%s... " % (destination_host, destination_file))
        util.log("CDR_ECV", output)
        return (4,)
    else:
        return (0,output)

########################################################################################################################
# do_distribution
# Purpose: Wrapper function to execute distribute_product x number of times (for retries)
########################################################################################################################
def do_distribution(outputdir, product_filename, destination_host, destination_file, destination_cksum_file):
    """Distributes a packaged product file to the specified location"""
    attempt = 0
    pstatus = None
    local_chksum = None
    cksum_file_full_path = None
    product_file_full_path = None

    while (attempt < 3 and pstatus != 0):
        pstatus, local_chksum, product_file_full_path, cksum_file_full_path = package_product(workdir, outputdir, product_filename)
        if pstatus == 0:
            break
        attempt = attempt + 1

    attempt = 0
    dstatus = None
    remote_chksum = None

    while (attempt < 3 and dstatus != 0):
        dstatus,remote_chksum = distribute_product(product_file_full_path, cksum_file_full_path, destination_host, destination_file, destination_cksum_file)
        if dstatus == 0:
            break
        attempt = attempt + 1

    return (local_chksum, remote_chksum)

         
#==============================================================
#Runs the script
#==============================================================
if __name__ == '__main__':
    parser = OptionParser(usage="usage: %prog [options] scenename")

    parser.add_option("--scene",
                      action="store",
                      dest="scene",
                      help="The scene id to process")

    parser.add_option("--source_type",
                      action="store",
                      dest="source_type",
                      default="level1",
                      help="level1,sr,toa,th")

    parser.add_option("--source_metadata",
                      action="store_true",
                      dest="sourcefile_metadata_flag",
                      default=False,
                      help="Include sourcefile metadata in output product")

    parser.add_option("--sourcefile",
                      action="store_true",
                      dest="sourcefile_flag",
                      default=False,
                      help="Include sourcefiles in output product")

    parser.add_option("--level1_browse",
                      action="store_true",
                      dest="level1_browse_flag",
                      default=False,
                      help="Create browse image for level 1 source product")

    parser.add_option("--sr_browse",
                      action="store_true",
                      dest="sr_browse_flag",
                      default=False,
                      help="Create a 3 band browse image for the resulting product")

    parser.add_option("--browse_resolution",
                      dest="browse_resolution",
                      action="store",
                      default=50,
                      help="Resolution (in percent) of output browse image")

    parser.add_option("--surface_reflectance",
                      action="store_true",
                      dest="sr_flag",
                      default=False,
                      help="Includes surface reflectance in output product")

    parser.add_option("--sr_ndvi",
                      action="store_true",
                      dest="sr_ndvi_flag",
                      default=False,
                      help="Create ndvi for this scene")

    parser.add_option("--sr_ndmi",
                      action="store_true",
                      dest="sr_ndmi_flag",
                      default=False,
                      help="Create ndmi for this scene")

    parser.add_option("--sr_nbr",
                      action="store_true",
                      dest="sr_nbr_flag",
                      default=False,
                      help="Create nbr for this scene")

    parser.add_option("--sr_nbr2",
                      action="store_true",
                      dest="sr_nbr2_flag",
                      default=False,
                      help="Create nbr2 for this scene")

    parser.add_option("--sr_savi",
                      action="store_true",
                      dest="sr_savi_flag",
                      default=False,
                      help="Create savi for this scene")
    
    parser.add_option("--sr_msavi",
                      action="store_true",
                      dest="sr_msavi_flag",
                      default=False,
                      help="Create msavi for this scene")

    parser.add_option("--sr_evi",
                      action="store_true",
                      dest="sr_evi_flag",
                      default=False,
                      help="Create evi for this scene")
       
    
    parser.add_option("--dem",
                      action="store_true",
                      dest="dem_flag",
                      default=False,
                      help="Create a DEM for this scene")
    
    parser.add_option("--surface_water_extent",
                      action="store_true",
                      dest="swe_flag",
                      default=False,
                      help="Create surface water extent for this scene")
    
    parser.add_option("--snow_covered_area",
                      action="store_true",
                      dest="sca_flag",
                      default=False,
                      help="Create snow covered area for this scene")
    
    parser.add_option("--toa",
                      action="store_true",
                      dest="toa_flag",
                      default=False,
                      help="Include Top of Atmosphere in output product")

    parser.add_option("--band6",
                      action="store_true",
                      dest="b6thermal_flag",
                      default=False,
                      help="Include Band 6 Thermal in output product")

    parser.add_option("--cfmask",
                      action="store_true",
                      dest="cfmask_flag",
                      default=False,
                      help="Include CFMask in output product")

    parser.add_option("--solr",
                      action="store_true",
                      dest="solr_flag",
                      default=False,
                      help="Create a solr index for the product")

    parser.add_option("--pixel_size",
                      action="store",
                      dest="pixel_size",
                      help="Desired pixel size for output products.  If specified --pixel_unit must also be provided.")

    parser.add_option("--pixel_unit",
                       action="store",
                       dest="pixel_unit",
                       choices=['meters', 'dd'],
                       help="Units the supplied pixel_size is specified in.  Meters or decimal degrees.")

    parser.add_option("--projection",
                       action="store",
                       dest="projection",
                       help="Proj.4 string for desired output product projection")
    
    parser.add_option("--set_image_extent",
                      action="store",
                      dest="image_extent",
                      help="Specify desired output image extents as minx,miny,maxx,maxy (comma seperated) \
                           Be careful to ensure these coordinates correspond to the output projection.")
    
    parser.add_option("--resample_method",
                      action="store",
                      dest="resample_method",
                      default="near",
                      help="One of near, bilinear, cubic, cubicspline, or lanczos")
    
    parser.add_option("--order",
                      action="store",
                      dest="ordernum",
                      help="Includes this scene as part of an order (controls where the file is distributed to)")

    parser.add_option("--collection",
                      action="store",
                      dest="collection_name",
                      help="Includes this scene as part of a collection (controls solr index values)")

    parser.add_option("--source_host",
                      action="store",
                      dest="source_host",
                      default="localhost",
                      help="The host were espa should look for the scene (--scene) to process")

    parser.add_option("--source_directory",
                      action="store",
                      dest="source_directory",
                      help="Directory on source host where the scene is located")

    parser.add_option("--destination_host",
                      action="store",
                      dest="destination_host",
                      default="localhost",
                      help="Host where completed products should be distributed to.")

    parser.add_option("--destination_directory",
                      action="store",
                      dest="destination_directory",
                      help="Directory on the host where the completed product file should be distributed to")

    parser.add_option("--debug",
                      action="store_true",
                      dest="debug",
                      help="Print debug messages while running")
    
    (options,args) = parser.parse_args()

    #util.log("CDR_ECV", str(options))
    if options.debug is not None:
        util.log("CDR_ECV", "--- Script options ---")
        optdict = vars(options)        
        for k,v in optdict.iteritems():
            util.log("CDR_ECV", "%s : %s" % (k,v))
        
    if options.scene is None:
        util.log("CDR_ECV", "\n You must specify a scene to process\n")
        parser.print_help()        
        sys.exit(cdr_ecv_exit_codes.exit_failure,)

    if options.ordernum is None and options.collection_name is None and options.destination_directory is None:
        util.log("CDR_ECV", "\n Either an ordernumber,collection name or destination directory is required \n")
        parser.print_help()
        sys.exit(cdr_ecv_exit_codes.exit_failure,)

    if options.solr_flag and options.collection_name is None:
        options.collection_name = "DEFAULT_COLLECTION"
        util.log("CDR_ECV", "\n A collection name was not provided but is required when generating a solr index \n")
        util.log("CDR_ECV", "\n collection_name is being set to 'DEFAULT_COLLECTION' \n")


        
    #WE WON'T RUN ANYTHING WITHOUT HAVING THE WORKING DIRECTORY SET
    if not os.environ.has_key("ESPA_WORK_DIR") or \
    len(os.environ.get("ESPA_WORK_DIR")) < 1:
        util.log("CDR_ECV", "$ESPA_WORK_DIR not set... exiting")
        sys.exit(cdr_ecv_exit_codes.environment, "ESPA_WORK_DIR not set")

    if os.environ.get("ESPA_WORK_DIR") == ".":
        BASE_WORK_DIR = os.getcwd()
    else:
        BASE_WORK_DIR = os.environ.get("ESPA_WORK_DIR")
    
    if not os.path.exists(BASE_WORK_DIR):
        util.log("CDR_ECV", "%s doesn't exist... creating" % BASE_WORK_DIR)
        os.makedirs(BASE_WORK_DIR, mode=0755)
        
    #MOVE MOST OF THESE INTO A CONFIG FILE
    base_source_path = '/data/standard_l1t'
    base_output_path = '/data2/LSRD'
    
    #processing_level = 'sr'
    scene = options.scene
    path = util.getPath(scene)
    row = util.getRow(scene)
    sensor = util.getSensor(scene)
    sensor_code = util.getSensorCode(scene)
    year = util.getYear(scene)
    doy = util.getDoy(scene)
    source_host=options.source_host
    destination_host=options.destination_host
    dem_prefix = 'dem.envi' # used to create the DEM filename and for cleanup
    dem_filename = dem_prefix + '.%s.bin' % scene

    if options.source_directory is not None:
        source_directory = options.source_directory
    else:
        source_directory = ("%s/%s/%s/%s/%s") % (base_source_path, sensor, path, row, year)

    if options.source_type == 'level1':
        source_filename = "%s.tar.gz" % scene
    elif options.source_type in ('sr','toa','th'):
        source_filename = "%s-sr.tar.gz" % scene
    else:
        errmsg = 'Did not recognize source file type:%s' % options.source_type
        util.log("CDR_ECV", errmsg)
        sys.exit(cdr_ecv_exit_codes.exit_failure, errmsg)
        
    source_file = ("%s/%s") % (source_directory,source_filename)
    
    #PRODUCT FILENAME NEEDS TO LOOK LIKE THIS: LE70300302004234-SC20130325164523
    ts = datetime.datetime.today()
    product_filename = ("%s%s%s%s%s") % (sensor_code,path.zfill(3),row.zfill(3),year,doy)
    product_filename = ("%s-SC%s%s%s%s%s%s") % (product_filename, ts.year,str(ts.month).zfill(2),str(ts.day).zfill(2),str(ts.hour).zfill(2),str(ts.minute).zfill(2),str(ts.second).zfill(2))
    
    destination_dir = None
    if options.destination_directory is not None:
        destination_dir = options.destination_directory
    elif options.ordernum is not None:
        destination_dir = ("%s/orders/%s") % (base_output_path, options.ordernum)
    else:
        errmsg = "Error determining if scene should be distributed as an order or to a directory"
        util.log("CDR_ECV", errmsg)
        sys.exit(cdr_ecv_exit_codes.exit_failure, errmsg)
    
    destination_file = ("%s/%s.tar.gz") % (destination_dir,product_filename)
    destination_cksum_file = ("%s/%s.cksum") % (destination_dir, product_filename)
    randdir = str(int(random.random() * 100000))
    stagedir = ("%s/%s/%s/stage") % (BASE_WORK_DIR,randdir,scene)
    workdir = ("%s/%s/%s/work") % (BASE_WORK_DIR,randdir,scene)
    outputdir=("%s/%s/%s/output") % (BASE_WORK_DIR,randdir,scene)
    localhostname = socket.gethostname()

    #PREPARE LOCAL STAGING DIRECTORY
    try:
        if os.path.exists(stagedir):
            cmd = "rm -rf %s" % stagedir
            status,output = commands.getstatusoutput(cmd)
            if status != 0:
                raise Exception(output)
        os.makedirs(stagedir, mode=0755)
    except Exception,e:
        util.log("CDR_ECV", "Error cleaning & creating stagedir:%s... exiting" % (stagedir))
        util.log("CDR_ECV",  e)
        sys.exit(cdr_ecv_exit_codes.creating_staging_dir, e)

    #PREPARE LOCAL WORK DIRECTORY
    try:
        if os.path.exists(workdir):
            cmd = "rm -rf %s" % workdir
            status,output = commands.getstatusoutput(cmd)
            if status != 0:
                raise Exception(output)
        os.makedirs(workdir, mode=0755)
    except Exception,e:
        util.log("CDR_ECV", "Error cleaning & creating workdir:%s... exiting" % (workdir))
        util.log("CDR_ECV",  e)
        sys.exit(cdr_ecv_exit_codes.creating_working_dir, e)
    
    #PREPARE LOCAL OUTPUT DIRECTORY
    try:
        if os.path.exists(outputdir):
            cmd = "rm -rf %s" % outputdir
            status,output = commands.getstatusoutput(cmd)
            if status != 0:
                raise Exception(output)
        os.makedirs(outputdir, mode=0755)
    except Exception, e:
        util.log("CDR_ECV", "Error cleaning & creating outputdir:%s... exiting" % (outputdir))
        util.log("CDR_ECV", e)
        sys.exit(cdr_ecv_exit_codes.creating_output_dir, e)
    
    #TRANSFER THE SOURCE FILE TO THE LOCAL MACHINE
    util.log("CDR_ECV", "Transferring %s from %s to %s" % (source_file,source_host,localhostname))
    cmd = ("scp -o StrictHostKeyChecking=no -c arcfour -C %s:%s %s") % (source_host, source_file, stagedir)
    (status,output) = commands.getstatusoutput(cmd)
    if status != 0:
        util.log("CDR_ECV", "Error transferring %s:%s to %s... exiting" % (source_host, source_file, stagedir))
        util.log("CDR_ECV", output)
        sys.exit(cdr_ecv_exit_codes.file_transfer, output)
    
    #UNPACK THE SOURCE FILE
    util.log("CDR_ECV", "Unpacking %s to %s" % (source_filename, workdir))
    cmd = ("tar --directory %s -xvf %s/%s") % (workdir, stagedir, source_filename)
    (status,output) = commands.getstatusoutput(cmd)
    if status != 0:
        util.log("CDR_ECV", "Error unpacking source file to %s/%s" % (workdir,source_filename))
        util.log("CDR_ECV", output)
        sys.exit(cdr_ecv_exit_codes.unpacking, output)

    metadata = getMetaData(workdir)
    if options.debug is not None:
        util.log("CDR_ECV", "--- Source Product Metadata ---")
        for m in metadata.iterkeys():
            util.log("CDR_ECV", "%s : %s" % (m, metadata[m]))
           
    #MAKE THE PRODUCT
    #ONLY RUN THIS IF THE SOURCE FILE IS A LEVEL 1
    #SKIP IF ITS AN SR,TOA,OR TH
    if options.source_type == 'level1' and \
         (options.sr_browse_flag
         or options.sr_ndvi_flag
         or options.sr_ndmi_flag
         or options.sr_nbr_flag
         or options.sr_nbr2_flag
         or options.sr_savi_flag
         or options.sr_evi_flag
         or options.sr_flag
         or options.b6thermal_flag
         or options.swe_flag
         or options.toa_flag
         or options.cfmask_flag):
        cmd = ("cd %s; do_ledaps.py --metafile %s") % (workdir, metadata['mtl_file'])
        if options.debug is not None:
            util.log("CDR_ECV", "LEDAPS COMMAND:%s" % cmd)
        util.log("CDR_ECV", "Running LEDAPS against %s with metafile %s" % (scene,metadata['mtl_file']))
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            util.log("CDR_ECV", "LEDAPS error detected... exiting")
            util.log("CDR_ECV", output)
            sys.exit(cdr_ecv_exit_codes.ledaps, output)

     
    #MAKE BROWSE IMAGE
    if options.sr_browse_flag:
        status = makeBrowse(workdir, metadata, scene, options.browse_resolution)
        if status != 0:
            util.log("CDR_ECV", "Error generating browse... exiting")
            sys.exit(cdr_ecv_exit_codes.browse, output)

    
    #MAKE SPECTRAL INDICIES
    if (options.sr_ndvi_flag or options.sr_ndmi_flag or options.sr_nbr_flag
        or options.sr_nbr2_flag or options.sr_savi_flag or options.sr_evi_flag):
        index_string = ""
        if options.sr_ndvi_flag:
            index_string = index_string + " --ndvi"
        if options.sr_ndmi_flag:
            index_string = index_string + " --ndmi"
        if options.sr_nbr_flag:
            index_string = index_string + " --nbr"
        if options.sr_nbr2_flag:
            index_string = index_string + " --nbr2"
        if options.sr_savi_flag:
            index_string = index_string + " --savi"
        if options.sr_msavi_flag:
            index_string = index_string + " --msavi"
        if options.sr_evi_flag:
            index_string = index_string + " --evi"

        cmd = ("cd %s; do_spectral_indices.py %s -i %s") % (workdir, index_string, get_sr_filename(scene))
        util.log("CDR_ECV", "SPECTRAL INDICES COMMAND:%s" % cmd)
        util.log("CDR_ECV", "Running Spectral Indices")
        status,output = commands.getstatusoutput(cmd)
        if status >> 8 != 0:
            util.log("CDR_ECV", "Spectral Index error detected... exiting")
            util.log("CDR_ECV", output)
            sys.exit(cdr_ecv_exit_codes.spectral_indices, output)


    #CREATE DEM for DEM or SWE or SCA
    if (options.dem_flag or options.swe_flag or options.sca_flag):
        cmd = ("cd %s; do_create_dem.py --metafile %s --demfile %s") \
            % (workdir, metadata['mtl_file'], dem_filename)

        if options.debug is not None:
            util.log("CDR_ECV", "CREATE DEM COMMAND:%s" % cmd)

        util.log("CDR_ECV", "Running CREATE DEM against %s with metafile %s" \
            % (scene,metadata['mtl_file']))

        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            util.log("CDR_ECV", "CREATE DEM error detected... exiting")
            util.log("CDR_ECV", output)
            sys.exit(cdr_ecv_exit_codes.dem, output)
    # END CREATE DEM


    #MAKE SOLR INDEX
    if options.solr_flag:
        util.log("CDR_ECV", "Running Solr Index")
        status = makeSolrIndex(metadata, scene, workdir, options.collection_name)
        if status != 0:
            util.log("CDR_ECV", "Error creating solr index... exiting")
            sys.exit(cdr_ecv_exit_codes.solr, output)

    if options.cfmask_flag or options.sr_flag:
        util.log("CDR_ECV", "Running CFMask")
        status,output = make_cfmask(workdir)
        if status != 0:
            util.log("CDR_ECV",
                "Error creating cfmask (status %s)... exiting" % status)
            sys.exit(cdr_ecv_exit_codes.cfmask, output)

    #IF THIS WAS JUST AN SR REQUEST THEN APPEND THE CFMASK RESULTS INTO
    #THE SR OUTPUT.  IF CFMASK WAS ALSO REQUESTED THEN WE WILL PROVIDE IT
    #SEPARATELY
    if options.sr_flag and not options.cfmask_flag:
        util.log("CDR_ECV", "Running Append CFMask")
        cmd = ("cd %s; do_append_cfmask.py --sr_infile %s --cfmask_infile %s" % (workdir, get_sr_filename(scene), get_cfmask_filename(scene)))
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            util.log("CDR_ECV", "Error appending cfmask to sr output... exiting")
            sys.exit(cdr_ecv_exit_codes.cfmask_append, output)


    #CREATE SURFACE WATER EXTENT
    if options.swe_flag:
        cmd = "cd %s; do_surface_water_extent.py --metafile %s" \
            " --reflectance %s --dem %s" % (workdir, metadata['mtl_file'],
            get_sr_filename(scene), dem_filename)

        if options.debug is not None:
            util.log("CDR_ECV", "CREATE SWE COMMAND:%s" % cmd)

        util.log("CDR_ECV",
            "Running CREATE SWE against %s with metafile %s and dem %s" \
            % (scene,metadata['mtl_file'], dem_filename))

        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            util.log("CDR_ECV", "CREATE SWE error detected... exiting")
            util.log("CDR_ECV", output)
            sys.exit(cdr_ecv_exit_codes.swe, output)
    # END CREATE SURFACE WATER EXTENT


    #CREATE SNOW COVERED AREA
    if options.sca_flag:
        cmd = "cd %s; do_snow_cover.py --metafile %s" \
            " --toa_infile %s --btemp_infile %s --sca_outfile %s --dem %s" \
            % (workdir, metadata['mtl_file'], get_toa_filename(scene),
            get_b6thermal_filename(scene), get_sca_filename(scene),
            dem_filename)

        if options.debug is not None:
            util.log("CDR_ECV", "CREATE SCA COMMAND:%s" % cmd)

        util.log("CDR_ECV",
            "Running CREATE SCA against %s with metafile %s, toa %s,"
            " btemp %s, and dem %s" \
            % (scene,metadata['mtl_file'], get_toa_filename(scene),
            get_b6thermal_filename(scene), dem_filename))

        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            util.log("CDR_ECV", "CREATE SCA error detected... exiting")
            util.log("CDR_ECV", output)
            sys.exit(cdr_ecv_exit_codes.sca, output)
    # END CREATE SNOW COVERED AREA


    #DELETE UNNEEDED FILES FROM PRODUCT DIRECTORY
    util.log("CDR_ECV", "Purging unneeded files from %s" % workdir)
    util.log("CDR_ECV", "Workdir contents:%s" % os.listdir(workdir))
    orig_cwd = os.getcwd()
    os.chdir(workdir)
    
    sb = StringIO()
    
    #always remove these
    sb.write(" *sixs* *metadata* LogReport* README* fmask*txt ")
    
    if not options.sourcefile_flag:
        sb.write(" *TIF *gap_mask* ")
    if not options.sourcefile_metadata_flag:
        sb.write(" *MTL* *VER* *GCP* ")
    if not options.b6thermal_flag:
        sb.write(" lndth.%s.hdf " % scene)
        sb.write(" lndth.%s.hdf.hdr " % scene)
        sb.write(" lndth.%s.txt " % scene)
    if not options.toa_flag:
        sb.write(" lndcal.%s.hdf " % scene)
        sb.write(" lndcal.%s.hdf.hdr " % scene)
        sb.write(" lndcal.%s.txt " % scene)
    if not options.dem_flag: # The DEM files are not to be delivered
        sb.write(' ' + dem_prefix + '.* ')
    if not options.swe_flag:
        sb.write(" *swe* ")
    if not options.sr_flag:
        sb.write(" lndsr.%s.hdf " % scene)
        sb.write(" lndsr.%s.hdf.hdr " % scene)
        sb.write(" lndsr.%s.txt " % scene)
    if not options.sr_browse_flag:
        sb.write(" *browse* ")
    if not options.solr_flag:
        sb.write(" *index* ")
    if not options.cfmask_flag:
        sb.write(" *fmask* ")
        
    sb.flush()
    cmd = "rm -rf %s " % sb.getvalue()
    sb.close()
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        util.log("CDR_ECV", "Error purging files from %s... exiting" % workdir)
        util.log("CDR_ECV",  output)
        sys.exit(cdr_ecv_exit_codes.purging, output)

    #warp outputs if necessary
    if options.projection or options.image_extent or options.pixel_size:
        util.log("CDR_ECV", "Warping output products")
        status, output = warp_outputs(workdir, options.projection, options.image_extent, options.pixel_size, options.pixel_unit, options.resample_method)
        if status != 0:
            util.log("CDR_ECV", "Error warping products (status %s)... exiting" % status)
            sys.exit(cdr_ecv_exit_codes.warping, output)


    #PACKAGE THE PRODUCT    
    attempt = 0
    success = False
    while (attempt < 3):
        local,remote = do_distribution(outputdir, product_filename, destination_host, destination_file, destination_cksum_file)
        util.log("CDR_ECV", "Comparing checksums")
        util.log("CDR_ECV", "Local  cksum:%s  type:%s" % (local, type(local)))
        util.log("CDR_ECV", "Remote cksum:%s  type:%s" % (remote, type(remote)))
        
        if local.split()[0] == remote.split()[0]:
            util.log("CDR_ECV", "Distribution complete for %s:%s" % (destination_host, destination_file))
            success = True
            break
        else:
            util.log("CDR_ECV", "Checksums do not match for %s:%s... retrying" % (destination_host, destination_file))
            util.log("CDR_ECV", "Local cksum:%s" % local.split()[0])
            util.log("CDR_ECV", "Remote cksum:%s" % remote.split()[0])
            attempt = attempt + 1

    if not success:
        util.log("CDR_ECV", "Packaging and distribution for %s:%s failed after 3 attempts" % (destination_host,destination_file))
        msg = "Failed to distribute %s" % destination_file
        sys.exit(cdr_ecv_exit_codes.packaging_distribution, msg)
        
              
    
    #CLEAN UP THE LOCAL FILESYSTEM
    status,output = commands.getstatusoutput("cd /tmp")
    util.log("CDR_ECV", "Cleaning local directories:%s %s %s" % (outputdir,workdir,stagedir))
    cmd = "rm -rf %s %s %s" % (outputdir,workdir,stagedir)
    status,output = commands.getstatusoutput(cmd)

    if status != 0:
        util.log("CDR_ECV", "Error cleaning output:%s work:%s stage:%s directories... exiting" % (outputdir,workdir,stagedir))
        util.log("CDR_ECV",  output)
        sys.exit(cdr_ecv_exit_codes.cleanup_output_dir, output)
    
    util.log("CDR_ECV", "ESPA Complete")
    #return 0 and the file we distributed
    #{u'f1': u'aaa', u'f2': u'adsf'}
    #do NOT util.log the next line.  It must be printed to stdout as the return value for this script
    print ('espa.result={"destination_file":"%s", "destination_cksum_file":"%s"}' % (destination_file, destination_cksum_file))
    sys.exit(cdr_ecv_exit_codes.exit_success,)

