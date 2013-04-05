#!/usr/local/bin/python

import gc, time, commands, os, sys, socket
import numpy as np
from osgeo import gdal
from optparse import OptionParser
from cStringIO import StringIO
from frange import frange
import json
import random


#TODO: 
#      
#      
#      
#      Test this script with hadoop
#      Create new collection creation script to extract the browse & index
#      and put them where they belong
#      

#==============================================================
# recursively removes zeros off the supplied string and returns
# the cleansed value
#==============================================================
def stripZeros(value):
    
    while value.startswith('0'):
        value = value[1:len(value)]
        
    return value

#==============================================================
#Cooresponding path for this scene
#==============================================================
def getPath(scene_name):
    return stripZeros(scene_name[3:6])

#==============================================================
#Corresponding row for this scene
#==============================================================
def getRow(scene_name):
    return stripZeros(scene_name[6:9])

#==============================================================
#Scene collection year
#==============================================================
def getYear(scene_name):
    return scene_name[9:13]

#==============================================================
#Scene collection julian date
#==============================================================
def getDoy(scene_name):
    return scene_name[13:16]

#==============================================================
#return scene sensor
#==============================================================
def getSensor(scene_name):
    if scene_name[0:3] =='LT5':
        return 'tm'
    elif scene_name[0:3] == 'LE7':
        return 'etm'

#==============================================================
#returns the station this scene was acquired from
#==============================================================
def getStation(scene_name):
    return scene_name[16:21]

#==============================================================
#return xy coordinates for the given line from gdalinfo
#==============================================================
def getXY(value):
    '''Returns the xy coordinates for the given line from gdalinfo'''
    parts = value.split('(')    
    p = parts[1].split(')')
    p = p[0].split(',')
    return (p[1].strip(),p[0].strip())


#==============================================================
#parse gdal coordinates from gdalinfo
#==============================================================
def parseGdalInfo(gdalFile, debug=False):

    cmd = "gdalinfo %s |grep \(" % (gdalFile)
    
    status,output = commands.getstatusoutput(cmd)
    contents = output

    if debug:
        print ("Parse GDAL Info")
        print contents

    results = dict()
        
    lines = contents.split('\n')
    for l in lines:
        if l.startswith('Upper Left'):
            results['browse.ul'] = getXY(l)
        elif l.startswith('Lower Left'):
            results['browse.ll'] = getXY(l)
        elif l.startswith('Upper Right'):
            results['browse.ur'] = getXY(l)
        elif l.startswith('Lower Right'):
            results['browse.lr'] = getXY(l)
            

    return results

#==============================================================
#return scene metadata as a dictionary
#==============================================================
def getMetaData(work_dir, debug=False):
        #find the metadata file
        mtl_file = ''
        items = os.listdir(work_dir)
        for i in items:
            if not i.startswith('lnd') and (i.find('_MTL') > 0) and not (i.find('old') > 0):
                mtl_file = i
                print ("Located MTL file:%s" % mtl_file)
                break

        if mtl_file == '':
            print("Could not locate the landsat MTL file in %s" % work_dir)
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
            #print ('Meta line:%s' % line)
            if not line.startswith('END') and not line.startswith('GROUP'):
                parts = line.split('=')
                if len(parts) == 2:
                    metadata[parts[0].strip()] = parts[1].strip().replace('"', '')

        metadata['mtl_file'] = mtl_file
         
        return metadata

#==============================================================
#expands an HDF to Geotiff bands
#==============================================================
def convertHDFToGTiff(hdf_file, target_filename):
    status = 0
    output = None
    try:
        cmd = ('gdal_translate -a_nodata -9999 -a_nodata 12000 -of GTiff -sds %s %s') % (hdf_file, target_filename)
        print ("Running %s" % cmd)
        status,output = commands.getstatusoutput(cmd)
        #if status != 0:
        #    print ("=== Error converting HDF to Geotiff ===")
        #    print output
    except Exception,e:
        print output
        print e
        return -1
    return 0
        

#==============================================================
#create a browse image for the product
#==============================================================
def makeBrowse(work_dir,metadata, scene_name,resolution=50,debug=False):
    print("Executing MakeBrowse()")
                    
    try:
        extrasdir = work_dir #os.path.join(work_dir, 'extras')
        output_file = "%s-sr-browse.tif" % scene_name
        output_file = os.path.join(extrasdir, output_file)
        
        if not os.path.exists(extrasdir):
            os.makedirs(extrasdir)

        convertHDFToGTiff("%s/lndsr*hdf" % work_dir, "%s/out.tiff" % extrasdir)
        
        cmds = []
        #cmds.append(('gdal_translate -of GTIFF -sds %s/lndsr*hdf %s/out.tiff') % (context['work.dir'], browsedir))
        cmds.append(('gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF %s/out.tiff5 %s/browse.tiff5') % (extrasdir, extrasdir))
        cmds.append(('gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF %s/out.tiff4 %s/browse.tiff4') % (extrasdir,extrasdir))
        cmds.append(('gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF %s/out.tiff3 %s/browse.tiff3') % (extrasdir,extrasdir))
        cmds.append(('gdal_merge_simple -in %s/browse.tiff5 -in %s/browse.tiff4 -in %s/browse.tiff3 -out %s/final.tif') % (extrasdir,extrasdir,extrasdir,extrasdir))

        #deproject into geographic
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
                print "Running:%s" % cmd
            status,output = commands.getstatusoutput(cmd)
            if status != 0:
                print ("Error occurred running:%s" % cmd)
                
                print output
                return status
            
        #add the browse cornerpoints to the context here
        #need to pull these from the level 1 metadata (IF it's already in longlat that is) instead so we have actual data cornerpoints instead of
        #scene cornerpoints
        #coords = parseGdalInfo(output_file)
        #metadata['BROWSE_UL_CORNER_LAT'] = coords['browse.ul'][0]
        #metadata['BROWSE_UL_CORNER_LON'] = coords['browse.ul'][1]
        #metadata['BROWSE_UR_CORNER_LAT'] = coords['browse.ur'][0]
        #metadata['BROWSE_UR_CORNER_LON'] = coords['browse.ur'][1]
        #metadata['BROWSE_LL_CORNER_LAT'] = coords['browse.ll'][0]
        #metadata['BROWSE_LL_CORNER_LON'] = coords['browse.ll'][1]
        #metadata['BROWSE_LR_CORNER_LAT'] = coords['browse.lr'][0]
        #metadata['BROWSE_LR_CORNER_LON'] = coords['browse.lr'][1]          
                                    
        print("MakeBrowse() complete...")
    except Exception,e:
        print e
        return -1
    finally:
        pass
    return 0
                
#==============================================================
#
#==============================================================
def getPoints(startX, stopX, startY, stopY, step):
    
    retVal = []
    for x in frange(startY, stopY, step):
        for y in frange(startX, stopX, step):
            retVal.append(("%f,%f") % (round(x,6), round(y,6)))
    retVal.append(("%f,%f") % (round(startY,6), round(startX,6)))
    retVal.append(("%f,%f") % (round(stopY,6), round(stopX,6)))
    return retVal

#==============================================================
#
#==============================================================
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

#==============================================================
#create a solr index file for the current scene
#==============================================================
def makeSolrIndex(metadata, scene, work_dir, collection_name,debug=False):
    try:
        print("Executing MakeSolrIndex() for %s" % scene)
            
                            
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
        matrix = buildMatrix(upper_right_LL, upper_left_LL, lower_left_LL, lower_right_LL)
        
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
        print e
        return -1
    
    print("MakeSolrIndex() complete...")
    return 0

#==============================================================
#create NDVI product for current scene
#==============================================================
def makeNDVI(work_directory,scene_name,debug=False):
    print("Executing NDVI()")
                       
    try:
        ndviDir = "%s" % work_directory
        ndvi_output_file = "%s-sr-ndvi.tif" % scene_name
        ndvi_output_file = os.path.join(ndviDir, ndvi_output_file)
        
        #start with a clean slate
        if not os.path.exists(ndviDir):
            os.makedirs(ndviDir)

        status = convertHDFToGTiff("%s/lndsr*hdf" % work_directory, "%s/out.tiff" % ndviDir)
        if status != 0:
            print ("Status %s:Error converting lndsr to Geotiff" % str(status))
            return status

        gc.collect()
            
        # load the proper geotiff bands into GDAL 
        red_file = ("%s/out.tiff3") % (ndviDir)
        in_ds = gdal.Open(red_file) 
        red = in_ds.ReadAsArray()
        geo = in_ds.GetGeoTransform()  
        proj = in_ds.GetProjection()   
        shape = red.shape          
        in_ds = None

        nir_file = ("%s/out.tiff4") % (ndviDir)
        in_ds = gdal.Open(nir_file)
        nir = in_ds.ReadAsArray()
        in_ds = None


        # NDVI = (nearInfrared - red) / (nearInfrared + red)
        nir = np.array(nir, dtype = float)  # change the array data type from integer to float to allow decimals
        red = np.array(red, dtype = float)

        np.seterr(divide='ignore')
                
        numerator = np.subtract(nir, red) 
        denominator = np.add(nir, red)
        nir = None
        red = None
        gc.collect()

        ndvi = np.divide(numerator,denominator)
        numerator = None
        denominator = None
        gc.collect()

        #put this into 10000 range
        ndvi = np.multiply(ndvi, 10000)
        gc.collect()
                
        #set all negative values to 0
        np.putmask(ndvi, ndvi < 0, 0)
                
        #set all values greater than 10000 to 10000
        np.putmask(ndvi, ndvi > 10000, 10000)
                
        driver = gdal.GetDriverByName('GTiff')

      
        ndvifile = ('%s/ndvi.tif') % (ndviDir)
        dst_ds = driver.Create( ndvifile, shape[1], shape[0], 1, gdal.GDT_Float32)

        # here we set the variable dst_ds with 
        # destination filename, number of columns and rows
        # 1 is the number of bands we will write out
        # gdal.GDT_Float32 is the data type - decimals
        dst_ds.SetGeoTransform(geo)
        dst_ds.SetProjection(proj) 
        dst_ds.GetRasterBand(1).WriteArray(ndvi)  
        stat = dst_ds.GetRasterBand(1).GetStatistics(1,1)
        dst_ds.GetRasterBand(1).SetStatistics(stat[0], stat[1], stat[2], stat[3])
        dst_ds = None

        gc.collect()

        in_ds = None
        dst_ds = None

        cmd = ('gdal_translate -ot UInt16 -scale 0 10000 0 10000 -of GTiff %s %s') % (ndvifile, ndvi_output_file)
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            print ("Error converting ndvi.tif to %s" % ndvi_output_file)
            print output
            return status
                
        cmd = ('rm -rf %s/out.tiff* %s/ndvi.tif') % (ndviDir, ndviDir)
        status,output = commands.getstatusoutput(cmd)
    except Exception, e:
        print e
        return -1
    finally:
        gc.collect()

    print ("NDVI() complete...")
    return 0



def make_cfmask(workdir):
    try:
        metafile = None
        for f in os.listdir(workdir):
            if f.find('metadata.txt') != -1:
                metafile = f
                break
        if metafile is None:
            raise IOError("Could not find LEDAPS metadata.txt in %s" % workdir)
        
        meta_file_with_path = os.path.join(workdir, metafile)
        status,output = commands.getstatusoutput("cfmask --metadata=%s" % meta_file_with_path)
        #if status != 0:
        #    print ("Error producing cfmask for %s" % meta_file_with_path)
        #    print output
        #    return status
        print ("CFMask returned code:%s" % status)
        print ("CFMask output:%s" % output)
    finally:
        pass
    return 0
        
#==============================================================
#Runs the script
#==============================================================
if __name__ == '__main__':
    parser = OptionParser(usage="usage: %prog [options] scenename")
    parser.add_option("--scene",
                      action="store",
                      dest="scene",
                      help="The scene id to process")
    parser.add_option("--include_sourcefile_metadata",
                      action="store_true",
                      dest="sourcefile_metadata_flag",
                      default=False,
                      help="Include sourcefile metadata in output product")
    parser.add_option("--include_sourcefile",
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
    parser.add_option("--include_surface_reflectance",
                      action="store_true",
                      dest="sr_flag",
                      default=False,
                      help="Includes surface reflectance in output product")
    parser.add_option("--sr_ndvi",
                      action="store_true",
                      dest="sr_ndvi_flag",
                      default=False,
                      help="Create ndvi for this scene")
    parser.add_option("--toa",
                      action="store_true",
                      dest="toa_flag",
                      default=False,
                      help="Include Top of Atmosphere in output product")
    parser.add_option("--band6thermal",
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
    parser.add_option("--convert_to_tile",
                       action="store_true",
                       dest="tiling_flag",
                       default=False,
                       help="Converts this scene to the EROS tiling scheme") 
    parser.add_option("--output_format",
                       action="store",
                       dest="output_format",
                       default="GTiff",
                       choices=['GTiff', 'JPG', 'PNG', 'HDF4','HDF5',],
                       help="Output format for product.  Defaults to HDF4")
    parser.add_option("--projection",
                       action="store",
                       dest="projection",
                       default="None",
                       choices=['None', 'Geographic', 'UTM', 'Albers', 'Sinusoidal', 'Robinson'],
                       help="Projection for the output product.  Defaults to no reprojection.")
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

    #print str(options)
    if options.debug is not None:
        print ("--- Script options ---")
        optdict = vars(options)        
        for k,v in optdict.iteritems():
            print ("%s : %s" % (k,v))
        
    if options.scene is None:
        print ("\n You must specify a scene to process\n")
        parser.print_help()        
        exit(-1)

    if options.ordernum is None and options.collection_name is None and options.destination_directory is None:
        print ("\n Either an ordernumber,collection name or destination directory is required \n")
        parser.print_help()
        exit(-1)

    if options.solr_flag and options.collection_name is None:
        options.collection_name = "DEFAULT_COLLECTION"
        print ("\n A collection name was not provided but is required when generating a solr index \n")
        print ("\n collection_name is being set to 'DEFAULT_COLLECTION' \n")
        
    #WE WON'T RUN ANYTHING WITHOUT HAVING THE WORKING DIRECTORY SET
    if not os.environ.has_key("ESPA_WORK_DIR") or \
    len(os.environ.get("ESPA_WORK_DIR")) < 1:
        print '$ESPA_WORK_DIR not set... exiting'
        sys.exit(1)

    if os.environ.get("ESPA_WORK_DIR") == ".":
        BASE_WORK_DIR = os.getcwd()
    else:
        BASE_WORK_DIR = os.environ.get("ESPA_WORK_DIR")
    
    if not os.path.exists(BASE_WORK_DIR):
        print "%s doesn't exist... creating" % BASE_WORK_DIR
        os.makedirs(BASE_WORK_DIR, mode=0755)
        
    #MOVE MOST OF THESE INTO A CONFIG FILE
    base_source_path = '/data/standard_l1t'
    base_output_path = '/data2/LSRD'
    
    processing_level = 'sr'
    scene = options.scene
    path = getPath(scene)
    row = getRow(scene)
    sensor = getSensor(scene)
    year = getYear(scene)
    doy = getDoy(scene)
    source_host=options.source_host
    destination_host=options.destination_host
    if options.source_directory is not None:
        source_directory = options.source_directory
    else:
        source_directory = ("%s/%s/%s/%s/%s") % (base_source_path, sensor, path, row, year)
    source_filename = "%s.tar.gz" % scene
    source_file = ("%s/%s") % (source_directory,source_filename)
    
    product_filename = ("%s-%s") % (scene,processing_level)

    destination_dir = None
    if options.destination_directory is not None:
        destination_dir = options.destination_directory
    elif options.ordernum is not None:
        destination_dir = ("%s/orders/%s") % (base_output_path, options.ordernum)
    else:
        print ("Error determining if scene should be distributed as an order or to a directory")
        sys.exit(-1)
    
    destination_file = ("%s/%s.tar.gz") % (destination_dir,product_filename)
    randdir = str(int(random.random() * 100000))
    workdir = ("%s/espawork/%s/%s/work") % (BASE_WORK_DIR,randdir,scene)
    outputdir=("%s/espawork/%s/%s/output") % (BASE_WORK_DIR,randdir,scene)
    localhostname = socket.gethostname()

    #PREPARE LOCAL WORK DIRECTORY
    try:
        if os.path.exists(workdir):
            cmd = "rm -rf %s" % workdir
            status,output = commands.getstatusoutput(cmd)
            if status != 0:
                raise Exception(output)
        os.makedirs(workdir, mode=0755)
    except Exception,e:
        print ("Error cleaning & creating workdir:%s... exiting") % (workdir)
        print e
        sys.exit(1)
    
    #PREPARE LOCAL OUTPUT DIRECTORY
    try:
        if os.path.exists(outputdir):
            cmd = "rm -rf %s" % outputdir
            status,output = commands.getstatusoutput(cmd)
            if status != 0:
                raise Exception(output)
        os.makedirs(outputdir, mode=0755)
    except Exception, e:
        print ("Error cleaning & creating outputdir:%s... exiting") % (outputdir)
        print e
        sys.exit(2)
    
    #TRANSFER THE SOURCE FILE TO THE LOCAL MACHINE
    print ("Transferring %s from %s to %s") % (source_file,source_host,localhostname)  
    cmd = ("scp -C %s:%s %s") % (source_host, source_file, outputdir)
    (status,output) = commands.getstatusoutput(cmd)
    if status != 0:
        print ("Error transferring %s:%s to %s... exiting") % (source_host, source_file, outputdir)
        print output
        sys.exit(3)
    
    #UNPACK THE SOURCE FILE
    print ("Unpacking %s.tar.gz to %s") % (scene, workdir)
    cmd = ("tar --directory %s -xvf %s/%s.tar.gz") % (workdir, outputdir, scene)
    (status,output) = commands.getstatusoutput(cmd)
    if status != 0:
        print ("Error unpacking source file to %s/%s.tar.gz") % (outputdir,scene)
        print output
        sys.exit(4)

    metadata = getMetaData(workdir)
    if options.debug is not None:
        print ("--- Source Product Metadata ---")
        for m in metadata.iterkeys():
            print ("%s : %s" % (m, metadata[m]))
           
    #MAKE THE PRODUCT
    if options.sr_browse_flag or options.sr_ndvi_flag or options.sr_flag or options.b6thermal_flag or options.toa_flag or options.cfmask_flag:
        cmd = ("cd %s; do_ledaps.py --metafile %s") % (workdir, metadata['mtl_file'])
        print ("LEDAPS COMMAND:%s" % cmd)
        print ("Running LEDAPS against %s with metafile %s") % (scene,metadata['mtl_file'])
        status,output = commands.getstatusoutput(cmd)
        if status != 0:
            print ("LEDAPS error detected... exiting")
            print output
            sys.exit(5)

     
    #MAKE BROWSE IMAGE
    if options.sr_browse_flag:
        status = makeBrowse(workdir, metadata, scene, options.browse_resolution)
        if status != 0:
            print ("Error generating browse... exiting")
            sys.exit(6)

    #MAKE NDVI
    if options.sr_ndvi_flag:
        status = makeNDVI(workdir, scene)
        if status != 0:
            print ("Error creating NDVI... exiting")
            sys.exit(7)
    
    #MAKE SOLR INDEX
    if options.solr_flag:
        status = makeSolrIndex(metadata, scene, workdir, options.collection_name)
        if status != 0:
            print ("Error creating solr index... exiting")
            sys.exit(8)

    if options.cfmask_flag:
        status = make_cfmask(workdir)
        if status != 0:
            print ("Error creating cfmask (status %s)... exiting" % status)
            sys.exit(9)

    #DELETE UNNEEDED FILES FROM PRODUCT DIRECTORY
    print("Purging unneeded files from %s") % workdir
    orig_cwd = os.getcwd()
    os.chdir(workdir)
    
    sb = StringIO()
    
    #always remove these
    sb.write(" *sixs* LogReport* README* ")
    
    if not options.sourcefile_flag:
        sb.write(" *TIF *gap_mask* ")
    if not options.sourcefile_metadata_flag:
        sb.write(" *MTL* *metadata* *VER* *GCP* ")
    if not options.b6thermal_flag:
        sb.write(" *lndth* ")
    if not options.toa_flag:
        sb.write(" *lndcal* ")
    if not options.sr_flag:
        sb.write(" *lndsr* ")
    
    sb.flush()
    
    cmd = "rm -rf %s " % sb.getvalue()
    
    sb.close()
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        print("Error purging files from %s... exiting") % workdir
        print output
        sys.exit(10)

    
    #PACKAGE THE PRODUCT FILE
    print ("Packaging completed product to %s/%s.tar.gz") % (outputdir,product_filename)
    cmd = ("tar -cvf %s/%s.tar *") % (outputdir, product_filename)
    status,output = commands.getstatusoutput(cmd)
    os.chdir(orig_cwd)
    if status != 0:
        print ("Error packaging finished product to %s/%s.tar") % (outputdir,product_filename)
        print output
        sys.exit(11)
    
    #COMPRESS THE PRODUCT FILE
    cmd = ("gzip %s/%s.tar") % (outputdir,product_filename)
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        print ("Error compressing final product file:%s/%s.tar") % (outputdir,product_filename)
        print output
        sys.exit(12)
    
    
    #MAKE DISTRIBUTION DIRECTORIES
    print ("Creating destination directories at %s" % destination_dir)
    cmd = "ssh %s mkdir -p %s" % (destination_host, destination_dir)
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        print ("Error creating destination directory %s on %s" % (destination_dir,destination_host))
        print output
        sys.exit(13)
    
    
    print ("Changing file permissions on  %s/%s.tar.gz to 0644" % (outputdir,product_filename))
    cmd = "chmod 0644 %s/%s.tar.gz" % (outputdir, product_filename)
    #os.chmod("%s/%s.tar.gz" % (outputdir, product_filename), 644)
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        print ("Error changing permissions on %s/%s.tar.gz to 0644... exiting" % (outputdir,product_filename))
        print output
        sys.exit(14)
    
    #DISTRIBUTE THE PRODUCT FILE
    print ("Transferring %s.tar.gz to %s:%s" % (product_filename,destination_host,destination_file))   
    cmd = "scp -p -C %s/%s.tar.gz %s:%s" % (outputdir, product_filename, destination_host, destination_file)       
    status,output = commands.getstatusoutput(cmd)
    if status != 0:
        print ("Error transferring %s.tar to %s:%s... exiting" % (product_filename, destination_host,destination_file))
        print output
        sys.exit(15)
    
    #print ("Changing file permissions on  %s/%s to 0644" % (outputdir,source_filename))
    #cmd = "chmod 0644 %s/%s" % (outputdir, source_filename)
    #status,output = commands.getstatusoutput(cmd)
    #if status != 0:
    #    print ("Error changing permissions on %s/%s.tar.gz to 0644... exiting" % (outputdir,source_filename))
    #    print output
    #    sys.exit(16)
    
    #DISTRIBUTE THE SOURCE FILE (THIS SHOULD GO AWAY ONCE USERS CAN SELECT WHAT THEY WANT)
    #print ("Transferring %s to %s:%s" % (source_filename,destination_host,destination_dir))   
    #cmd = "scp -p -C %s/%s %s:%s" % (outputdir, source_filename, destination_host,destination_dir)
    #status,output = commands.getstatusoutput(cmd)
    #if status != 0:
    #    print ("Error transferring %s.tar to %s:%s... exiting" % (source_filename, destination_host,destination_dir))
    #    print output
    #    sys.exit(17)    
        
    
    #CLEAN UP THE LOCAL FILESYSTEM
    status,output = commands.getstatusoutput("cd /tmp")
    print ("Cleaning local directories:%s %s" % (outputdir,workdir))
    cmd = "rm -rf %s %s" % (outputdir,workdir)
    status,output = commands.getstatusoutput(cmd)

    if status != 0:
        print("Error cleaning output:%s and work:%s directories... exiting" % (outputdir,workdir))
        print output
        sys.exit(18)
    
    print ("ESPA Complete")
