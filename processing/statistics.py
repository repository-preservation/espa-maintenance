
'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  This module provides routines to be used for analyzing and manipulating tile
  data.

History:
  Created Jan/2014 by Ron Dilley, USGS/EROS
'''

import os
import sys
import glob
import errno
from cStringIO import StringIO
import numpy as np

# espa-common objects and methods
from espa_constants import EXIT_FAILURE
from espa_constants import EXIT_SUCCESS

# imports from espa_common
from logger_factory import EspaLogging
import settings

# local objects and methods
import espa_exception as ee


# ============================================================================
def get_statistics(file_name, band_type):
    '''
    Description:
      Uses numpy to determine the stats values from the specified file.
      The data is cleaned before stats are generated.  This cleaning is
      intended to remove fill and outliers from the statistics.

    Returns:
      Minimum(float)
      Maximum(float)
      Mean(float)
      Standard Deviation(float)
    '''

    # Figure out the data type based on the band type
    data_type = np.int16
    if band_type == 'LST':
        data_type = np.uint16
    elif band_type == 'EMIS':
        data_type = np.uint8

    # Load the image data into memory
    input_data = np.fromfile(file_name, dtype=data_type)

    # Get the data bounds
    upper_bound = settings.BAND_TYPE_STAT_RANGES[band_type]['UPPER_BOUND']
    lower_bound = settings.BAND_TYPE_STAT_RANGES[band_type]['LOWER_BOUND']

    # Clean the data
    input_data = input_data[((input_data >= lower_bound)
                             & (input_data <= upper_bound))]

    # Calculate the stats
    if input_data.size > 0:
        minimum = np.min(input_data)
        maximum = np.max(input_data)
        mean = np.mean(input_data)
        stddev = np.std(input_data)
        valid = 'yes'
    else:
        minimum = -9999.0
        maximum = -9999.0
        mean = -9999.0
        stddev = -9999.0
        valid = 'no'

    return (float(minimum), float(maximum), float(mean), float(stddev), valid)
# END - get_statistics


# ============================================================================
def generate_statistics(work_directory, files_to_search_for):
    '''
    Description:
      Create the stats output directory and each output stats file for each
      file specified.

    Notes:
      The stats directory is created here because we only want it in the
      product if we need statistics.
    '''

    logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)

    # Change to the working directory
    current_directory = os.getcwd()
    os.chdir(work_directory)

    try:
        stats_output_path = 'stats'
        try:
            os.makedirs(stats_output_path)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(stats_output_path):
                pass
            else:
                raise ee.ESPAException(ee.ErrorCodes.statistics,
                                       str(exc)), None, sys.exc_info()[2]

        try:
            # Build the list of files to process
            file_names = dict()
            for band_type in files_to_search_for:
                file_names[band_type] = list()
                for search in files_to_search_for[band_type]:
                    file_names[band_type].extend(glob.glob(search))

            # Generate the requested statistics for each tile
            for band_type in file_names:
                for file_name in file_names[band_type]:

                    logger.info("Generating statistics for: %s" % file_name)

                    (minimum, maximum, mean, stddev,
                     valid) = get_statistics(file_name, band_type)

                    # Drop the filename extention so we can replace it with
                    # 'stats'
                    base = os.path.splitext(file_name)[0]
                    base_name = '.'.join([base, 'stats'])

                    # Figure out the full path filename
                    stats_output_file = os.path.join(stats_output_path,
                                                     base_name)

                    # Buffer the stats
                    data_io = StringIO()
                    data_io.write("FILENAME=%s\n" % file_name)
                    data_io.write("MINIMUM=%f\n" % minimum)
                    data_io.write("MAXIMUM=%f\n" % maximum)
                    data_io.write("MEAN=%f\n" % mean)
                    data_io.write("STDDEV=%f\n" % stddev)
                    data_io.write("VALID=%s\n" % valid)

                    # Create the stats file
                    with open(stats_output_file, 'w+') as stat_fd:
                        stat_fd.write(data_io.getvalue())
            # END - for tile
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.statistics,
                                   str(e)), None, sys.exc_info()[2]

    finally:
        # Change back to the previous directory
        os.chdir(current_directory)
# END - generate_statistics


# ============================================================================
if __name__ == '__main__':
    '''
    Description:
      This is test code only used during proto-typing.
      It only provides stats for landsat and modis data.
    '''

    # Configure logging
    EspaLogging.configure(settings.PROCESSING_LOGGER, order='test',
                          product='statistics')
    logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)

    # Hold the wild card strings in a type based dictionary
    files_to_search_for = dict()

    # Landsat files
    files_to_search_for['SR'] = ['*_sr_band[0-9].img']
    files_to_search_for['TOA'] = ['*_toa_band[0-9].img']
    files_to_search_for['INDEX'] = ['*_nbr.img', '*_nbr2.img', '*_ndmi.img',
                                    '*_ndvi.img', '*_evi.img', '*_savi.img',
                                    '*_msavi.img']

    # MODIS files
    files_to_search_for['SR'].extend(['*sur_refl_b*.img'])
    files_to_search_for['INDEX'].extend(['*NDVI.img', '*EVI.img'])
    files_to_search_for['LST'] = ['*LST_Day_1km.img', '*LST_Night_1km.img',
                                  '*LST_Day_6km.img', '*LST_Night_6km.img']
    files_to_search_for['EMIS'] = ['*Emis_*.img']

    try:
        generate_statistics('.', files_to_search_for)
    except Exception, e:
        if hasattr(e, 'output'):
            logger.error("Output [%s]" % e.output)
        logger.exception("Processing failed")
        sys.exit(EXIT_FAILURE)

    sys.exit(EXIT_SUCCESS)
# END - __main__
