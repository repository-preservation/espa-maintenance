
'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Implements solr index generation.

History:
  Created Jan/2014 by Ron Dilley, USGS/EROS
'''

import numpy as np
from cStringIO import StringIO

# imports from espa_common
from logger_factory import EspaLogging
import settings


# ============================================================================
def do_solr_index(metadata, scene, solr_filename, collection_name,
                  build_points=False):
    '''
    Description:
      Creates the solr index file from the metadata
    '''

    logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)

    logger.info("Executing create_solr_index() for %s using collection %s "
                % (scene, collection_name))

    # deal with the landsat metadata fieldname changes
    if 'CORNER_UL_LAT_PRODUCT' in metadata:
        upper_left_LL = '%s,%s' \
            % (metadata['CORNER_UL_LAT_PRODUCT'],
               metadata['CORNER_UL_LON_PRODUCT'])
        upper_right_LL = '%s,%s' \
            % (metadata['CORNER_UR_LAT_PRODUCT'],
               metadata['CORNER_UR_LON_PRODUCT'])
        lower_left_LL = '%s,%s' \
            % (metadata['CORNER_LL_LAT_PRODUCT'],
               metadata['CORNER_LL_LON_PRODUCT'])
        lower_right_LL = '%s,%s' \
            % (metadata['CORNER_LR_LAT_PRODUCT'],
               metadata['CORNER_LR_LON_PRODUCT'])

        if build_points:
            lat_list = [float(metadata['CORNER_UL_LAT_PRODUCT']),
                        float(metadata['CORNER_UR_LAT_PRODUCT']),
                        float(metadata['CORNER_LL_LAT_PRODUCT']),
                        float(metadata['CORNER_LR_LAT_PRODUCT'])]
            lon_list = [float(metadata['CORNER_UL_LON_PRODUCT']),
                        float(metadata['CORNER_UR_LON_PRODUCT']),
                        float(metadata['CORNER_LL_LON_PRODUCT']),
                        float(metadata['CORNER_LR_LON_PRODUCT'])]
    else:
        upper_left_LL = '%s,%s' \
            % (metadata['PRODUCT_UL_CORNER_LAT'],
               metadata['PRODUCT_UL_CORNER_LON'])
        upper_right_LL = '%s,%s' \
            % (metadata['PRODUCT_UR_CORNER_LAT'],
               metadata['PRODUCT_UR_CORNER_LON'])
        lower_left_LL = '%s,%s' \
            % (metadata['PRODUCT_LL_CORNER_LAT'],
               metadata['PRODUCT_LL_CORNER_LON'])
        lower_right_LL = '%s,%s' \
            % (metadata['PRODUCT_LR_CORNER_LAT'],
               metadata['PRODUCT_LR_CORNER_LON'])

        if build_points:
            lat_list = [float(metadata['PRODUCT_UL_CORNER_LAT']),
                        float(metadata['PRODUCT_UR_CORNER_LAT']),
                        float(metadata['PRODUCT_LL_CORNER_LAT']),
                        float(metadata['PRODUCT_LR_CORNER_LAT'])]
            lon_list = [float(metadata['PRODUCT_UL_CORNER_LON']),
                        float(metadata['PRODUCT_UR_CORNER_LON']),
                        float(metadata['PRODUCT_LL_CORNER_LON']),
                        float(metadata['PRODUCT_LR_CORNER_LON'])]

    solr_buffer = StringIO()

    solr_buffer.write("<add><doc>\n")

    solr_buffer.write("<field name='sceneid'>%s</field>\n" % scene)

    solr_buffer.write("<field name='path'>%s</field>\n"
                      % metadata['WRS_PATH'])

    # this is a fix for the changes to landsat metadata...
    # currently have mixed versions on the cache
    row = None
    if 'WRS_ROW' in metadata:
        row = metadata['WRS_ROW']
    else:
        row = metadata['STARTING_ROW']

    solr_buffer.write("<field name='row'>%s</field>\n" % row)

    solr_buffer.write("<field name='sensor'>%s</field>\n"
                      % metadata['SENSOR_ID'])

    solr_buffer.write("<field name='sunElevation'>%s</field>\n"
                      % metadata['SUN_ELEVATION'])

    solr_buffer.write("<field name='sunAzimuth'>%s</field>\n"
                      % metadata['SUN_AZIMUTH'])

    solr_buffer.write("<field name='groundStation'>%s</field>\n"
                      % metadata['STATION_ID'])

    # get the acquisition date... account for landsat changes
    acquisition_date = None
    if 'DATE_ACQUIRED' in metadata:
        acquisition_date = ''.join([metadata['DATE_ACQUIRED'], 'T00:00:01Z'])
    else:
        acquisition_date = ''.join([metadata['ACQUISITION_DATE'],
                                    'T00:00:01Z'])

    solr_buffer.write("<field name='acquisitionDate'>%s</field>\n"
                      % acquisition_date)

    solr_buffer.write("<field name='collection'>%s</field>\n"
                      % collection_name)

    solr_buffer.write("<field name='upperRightCornerLatLong'>%s</field>\n"
                      % upper_right_LL)
    solr_buffer.write("<field name='upperLeftCornerLatLong'>%s</field>\n"
                      % upper_left_LL)
    solr_buffer.write("<field name='lowerLeftCornerLatLong'>%s</field>\n"
                      % lower_left_LL)
    solr_buffer.write("<field name='lowerRightCornerLatLong'>%s</field>\n"
                      % lower_right_LL)

    if build_points:
        # Build lat and lon list values using the same step hard coded in the
        # original code implementation (util.buildMatrix)
        step = 0.05
        for lat in np.arange(min(lat_list), max(lat_list), step):
            for lon in np.arange(min(lon_list), max(lon_list), step):
                solr_buffer.write("<field name='latitude_longitude'>"
                                  "%f,%f</field>\n" % (round(lat, 6),
                                                       round(lon, 6)))

    solr_buffer.write("</doc></add>")
    solr_buffer.flush()

    with open(solr_filename, 'w') as output_fd:
        output_fd.write(solr_buffer.getvalue())

    solr_buffer.close()
# END - do_solr_index
