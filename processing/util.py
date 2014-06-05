
'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Utility module for ESPA project.
  This is a shared module to hold simple utility functions.

History:
  Original implementation by David V. Hill, USGS/EROS
  Updated Jan/2014 by Ron Dilley, USGS/EROS
'''

import datetime
import calendar
import subprocess
import random


# local objects and methods
import settings


def execute_cmd(cmd):
    '''
    Description:
      Execute a command line and return SUCCESS or ERROR

    Returns:
        output - The stdout and/or stderr from the executed command.
    '''

    output = ''
    proc = None
    try:
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, shell=True)
        output = proc.communicate()[0]

        if proc.returncode < 0:
            message = "Application terminated by signal [%s]" % cmd
            raise Exception(message)

        if proc.returncode != 0:
            message = "Application failed to execute [%s]" % cmd
            raise Exception(message)

        application_exitcode = proc.returncode >> 8
        if application_exitcode != 0:
            message = "Application [%s] returned error code [%d]" \
                      % (cmd, application_exitcode)
            raise Exception(message)

    finally:
        del proc

    return output


def stripZeros(value):
    '''
    Description:
      Removes all leading zeros from a string
    '''

    while value.startswith('0'):
        value = value[1:len(value)]
    return value


def getPath(scene_name):
    '''
    Description:
      Returns the path of a given scene
    '''
    return stripZeros(scene_name[3:6])


def getRow(scene_name):
    '''
    Description:
      Returns the row of a given scene
    '''
    return stripZeros(scene_name[6:9])


def getYear(scene_name):
    '''
    Description:
      Returns the year of a given scene
    '''
    return scene_name[9:13]


def getDoy(scene_name):
    '''
    Description:
      Returns the day of year for a given scene
    '''
    return scene_name[13:16]


def getSensor(scene_name):
    '''
    Description:
      Returns the sensor of a given scene
    '''

    if scene_name[0:3].lower() == 'lt5' or scene_name[0:3].lower() == 'lt4':
        # Landsat TM
        return 'LT'
    elif scene_name[0:3].lower() == 'le7':
        # Landsat ETM+
        return 'LE'
    elif scene_name[0:3].lower() == 'mod':
        # MODIS Terra
        return 'MOD'
    elif scene_name[0:3].lower() == 'myd':
        # MODIS Aqua
        return 'MYD'
    return ''


def getSensorCode(scene_name):
    '''
    Description:
      Returns the raw sensor code of a given scene
    '''
    return scene_name[0:3]


def getStation(scene_name):
    '''
    Description:
      Returns the ground stations and version for a given scene
    '''
    return scene_name[16:21]


def getModisShortName(scene_name):
    '''
    Description:
      Returns the MODIS short name portion of the scene
    '''
    return scene_name.split('.')[0]


def getModisVersion(scene_name):
    '''
    Description:
      Returns the MODIS version portion of the scene
    '''
    return scene_name.split('.')[3]


def getModisHorizontalVertical(scene_name):
    '''
    Description:
      Returns the MODIS horizontal and vertical specifiers of the scene
    '''

    element = scene_name.split('.')[2]
    return (element[0:3], element[3:])


def getModisSceneDate(scene_name):
    '''
    Description:
      Returns the MODIS scene data portion of the scene
    '''

    date_element = scene_name.split('.')[1]
    # Return the (year, doy)
    return (date_element[1:5], date_element[5:8])


def getModisArchiveDate(scene_name):
    '''
    Description:
      Returns the MODIS archive date portion of the scene
    '''
    date_element = scene_name.split('.')[1]

    year = date_element[1:5]
    doy = date_element[5:8]

    # Convert DOY to month and day
    month = 1
    day = int(doy)
    while month < 13:
        month_days = calendar.monthrange(int(year), month)[1]
        if day <= month_days:
            return '%s.%s.%s' % (year.zfill(4), str(month).zfill(2),
                                 str(day).zfill(2))
        day -= month_days
        month += 1

    raise ValueError("Year %s does not have %s days" % (year, doy))


def getCacheHostname():
    '''
    Description:
      Poor mans load balancer for accessing the online cache over the private
      network
    '''

    # 140 is here twice so the load is 2/3 + 1/3.  machines are mismatched
    host_list = settings.espa_cache_host_list

    def check_host_status(hostname):
        cmd = "ping -q -c 1 %s" % hostname
        output = ''
        try:
            output = execute_cmd(cmd)
        except Exception, e:
            return -1
        return 0

    def get_hostname():
        hostname = random.choice(host_list)
        if check_host_status(hostname) == 0:
            return hostname
        else:
            for x in host_list:
                if x == hostname:
                    host_list.remove(x)
            if len(host_list) > 0:
                return get_hostname()
            else:
                raise Exception("No online cache hosts available...")

    return get_hostname()
