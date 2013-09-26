"""Utility module for ESPA project.

This is a shared module to hold simple utility functions.

Author:  "David V. Hill"
License: "NASA Open Source Agreement 1.3"

"""
import datetime
import commands
from frange import frange

def build_log_msg(module, msg):
    """Builds a standardized log message"""
    now = datetime.datetime.now()
    return "%s-%s-%s %s:%s.%s [%s] %s" % (now.year,
                                  str(now.month).zfill(2),
                                  str(now.day).zfill(2),
                                  str(now.hour).zfill(2),
                                  str(now.minute).zfill(2),
                                  str(now.second).zfill(2),
                                  module,
                                  msg)

def log(module, msg):
    """Logs a message in the ESPA standard log format"""
    print (build_log_msg(module, msg))
    

def stripZeros(value):
    """Removes all leading zeros from a string"""

    while value.startswith('0'):
        value = value[1:len(value)]
    return value


def getPath(scene_name):
    """Returns the path of a given scene"""
    return stripZeros(scene_name[3:6])


def getRow(scene_name):
    """Returns the row of a given scene"""
    return stripZeros(scene_name[6:9])


def getYear(scene_name):
    """Returns the year of a given scene"""
    return scene_name[9:13]


def getDoy(scene_name):
    """Returns the day of year for a given scene"""
    return scene_name[13:16]


def getSensor(scene_name):
    """Returns the sensor of a given scene"""
    if scene_name[0:3] =='LT5' or scene_name[0:3] == 'LT4':
        return 'tm'
    elif scene_name[0:3] == 'LE7':
        return 'etm'


def getSensorCode(scene_name):
    """Returns the raw sensor code of a given scene"""
    return scene_name[0:3]


def getStation(scene_name):
    """Returns the ground stations and version for a given scene"""
    return scene_name[16:21]


def getPoints(startX, stopX, startY, stopY, step):
    """Generates a list of points that lie within the specified bounding box at the given step (float)"""
    
    retVal = []
    for x in frange(startY, stopY, step):
        for y in frange(startX, stopX, step):
            retVal.append(("%f,%f") % (round(x,6), round(y,6)))
    retVal.append(("%f,%f") % (round(startY,6), round(startX,6)))
    retVal.append(("%f,%f") % (round(stopY,6), round(stopX,6)))
    return retVal


def buildMatrix(yx1, yx2, yx3, yx4):
    """Builds a matrix of points that lie within yx1, yx2, yx3, and yx4 where
       yxN is a latitude,longitude pair
    """
    
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


def parseGdalInfo(gdalFile, debug=False):
    """Runs gdalinfo against a file and returns
    the x,y results"""

    def getXY(value):
        """Inner function to return the xy coordinates for the given line from gdalinfo"""
        parts = value.split('(')    
        p = parts[1].split(')')
        p = p[0].split(',')
        return (p[1].strip(),p[0].strip())
    
    cmd = "gdalinfo %s |grep \(" % (gdalFile)
    
    status,output = commands.getstatusoutput(cmd)
    contents = output

    if debug:
        log("CDR_ECV", "Parse GDAL Info")
        log("CDR_ECV", contents)

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


def convertHDFToGTiff(hdf_file, target_filename):
    """Converts the named hdf file and all its subdatasets to GEOTIFF in separate band files"""
    status = 0
    output = None
    try:
        cmd = ('gdal_translate -a_nodata -9999 -a_nodata 12000 -of GTiff -sds %s %s') % (hdf_file, target_filename)
        log("CDR_ECV", "Running %s" % cmd)
        status,output = commands.getstatusoutput(cmd)
        #if status != 0:
        #    log("CDR_ECV", "=== Error converting HDF to Geotiff ===")
        #    log("CDR_ECV",  output
    except Exception,e:
        log("CDR_ECV", output)
        log("CDR_ECV", e)
        return -1
    return 0
