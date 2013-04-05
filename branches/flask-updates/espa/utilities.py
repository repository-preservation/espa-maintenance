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
def get_gdal_cornerpoints(gdalFile, debug=False):

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
