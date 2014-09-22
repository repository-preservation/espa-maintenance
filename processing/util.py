
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

import os
import datetime
import calendar
import commands
import random

# imports from espa/espa_common
try:
    import settings
except:
    from espa_common import settings

try:
    import utilities
except:
    from espa_common import utilities


# ============================================================================
def get_path(scene_name):
    '''
    Description:
      Returns the path of a given scene
    '''
    return utilities.strip_zeros(scene_name[3:6])


# ============================================================================
def get_row(scene_name):
    '''
    Description:
      Returns the row of a given scene
    '''
    return utilities.strip_zeros(scene_name[6:9])


# ============================================================================
def get_year(scene_name):
    '''
    Description:
      Returns the year of a given scene
    '''
    return scene_name[9:13]


# ============================================================================
def get_doy(scene_name):
    '''
    Description:
      Returns the day of year for a given scene
    '''
    return scene_name[13:16]


# ============================================================================
def get_sensor(scene_name):
    '''
    Description:
      Returns the sensor of a given scene
    '''

    if scene_name[0:3].lower() == 'lt5' or scene_name[0:3].lower() == 'lt4':
        # Landsat TM
        return 'tm'
    elif scene_name[0:3].lower() == 'le7':
        # Landsat ETM+
        return 'etm'
    elif scene_name[0:3].lower() == 'mod':
        # MODIS Terra
        return 'terra'
    elif scene_name[0:3].lower() == 'myd':
        # MODIS Aqua
        return 'aqua'
    return ''


# ============================================================================
def get_sensor_code(scene_name):
    '''
    Description:
      Returns the raw sensor code of a given scene
    '''
    return scene_name[0:3]


# ============================================================================
def get_station(scene_name):
    '''
    Description:
      Returns the ground stations and version for a given scene
    '''
    return scene_name[16:21]


# ============================================================================
def get_modis_short_name(scene_name):
    '''
    Description:
      Returns the MODIS short name portion of the scene
    '''
    return scene_name.split('.')[0]


# ============================================================================
def get_modis_version(scene_name):
    '''
    Description:
      Returns the MODIS version portion of the scene
    '''
    return scene_name.split('.')[3]


# ============================================================================
def get_modis_horizontal_vertical(scene_name):
    '''
    Description:
      Returns the MODIS horizontal and vertical specifiers of the scene
    '''

    element = scene_name.split('.')[2]
    return (element[0:3], element[3:])


# ============================================================================
def get_modis_scene_date(scene_name):
    '''
    Description:
      Returns the MODIS scene data portion of the scene
    '''

    date_element = scene_name.split('.')[1]
    # Return the (year, doy)
    return (date_element[1:5], date_element[5:8])


# ============================================================================
def get_modis_archive_date(scene_name):
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

    raise ValueError("Year %s does not have day %s" % (year, doy))


# ============================================================================
def get_input_hostname(sensor):
    '''
    Description:
      Determine the input hostname to use for the sensors data.

    Note:
      Today all landsat source products use the landsat online cache which is
      provided by get_cache_hostname.
    '''

    if sensor in ['terra', 'aqua']:
        return settings.MODIS_INPUT_HOSTNAME

    return utilities.get_cache_hostname()


# ============================================================================
def get_output_hostname():
    '''
    Description:
      Determine the output hostname to use for espa products.

    Note:
      Today all output products use the landsat online cache which is provided
      by utilities.get_cache_hostname.
    '''

    return utilities.get_cache_hostname()
