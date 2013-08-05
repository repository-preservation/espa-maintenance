"""Utility module for ESPA project.

This is a shared module to hold simple utility functions.

Author:  "David V. Hill"
License: "NASA Open Source Agreement 1.3"

"""

def log(module, msg):
    """Logs a message in the ESPA standard log format"""
    now = datetime.datetime.now()
    print("%s-%s-%s %s:%s.%s (%s) %s" % (now.year,                                         now.year,
                                  now.month,
                                  now.day,
                                  now.hour,
                                  now.minute,
                                  now.second,
                                  module,
                                  msg))


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
