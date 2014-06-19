#! /usr/bin/env python

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
import traceback
from cStringIO import StringIO

# espa-common objects and methods
from espa_constants import *
from espa_logging import log

# local objects and methods
import espa_exception as ee
import util


# ============================================================================
def get_statistics(file_name):
    '''
    Description:
      Uses gdalinfo to extract the stats values from the specified file.

    Returns:
      Minimum(float)
      Maximum(float)
      Mean(float)
      Standard Deviation(float)
    '''

    minimum = 0
    maximum = 0
    mean = 0
    stddev = 0

    cmd = ['gdalinfo', '-stats', file_name]
    cmd = ' '.join(cmd)
    output = util.execute_cmd(cmd)

    for line in output.split('\n'):
        line_lower = line.strip().lower()

        if line_lower.startswith('statistics_minimum'):
            minimum = line_lower.split('=')[1]  # take the second element
        if line_lower.startswith('statistics_maximum'):
            maximum = line_lower.split('=')[1]  # take the second element
        if line_lower.startswith('statistics_mean'):
            mean = line_lower.split('=')[1]     # take the second element
        if line_lower.startswith('statistics_stddev'):
            stddev = line_lower.split('=')[1]   # take the second element
        if 'no valid pixels found' in line_lower:
            log("Warning: No valid pixels found: Continuing with zeroed stats")

    # Cleanup the gdal generated xml file should be the only file
    # BUT...... *NOT* GUARANTEED
    # So we may be removing other gdal aux files here
    for file_name in glob.glob('*.aux.xml'):
        os.unlink(file_name)

    return (float(minimum), float(maximum), float(mean), float(stddev))
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
                                       str(e)), None, sys.exc_info()[2]

        try:
            file_names = list()
            for search in files_to_search_for:
                file_names.extend(glob.glob(search))

            # Generate the requested statistics for each tile
            for file_name in file_names:
                log("Generating statistics for: %s" % file_name)

                (minimum, maximum, mean, stddev) = get_statistics(file_name)

                # Drop the filename extention so we can replace it with 'stats'
                base = os.path.splitext(file_name)[0]

                # Figure out the filename
                stats_output_file = '%s/%s.stats' % (stats_output_path, base)

                # Buffer the stats
                data_io = StringIO()
                data_io.write("FILENAME=%s\n" % file_name)
                data_io.write("MINIMUM=%f\n" % minimum)
                data_io.write("MAXIMUM=%f\n" % maximum)
                data_io.write("MEAN=%f\n" % mean)
                data_io.write("STDDEV=%f\n" % stddev)

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
      It only provides stats for landsat data.
    '''

    # Landsat files
    files_to_search_for = ['*_sr_band[0-9].img', '*_toa_band[0-9].img',
                           '*_nbr.img', '*_nbr2.img', '*_ndmi.img',
                           '*_ndvi.img', '*_evi.img', '*_savi.img',
                           '*_msavi.img']
    # MODIS files
    files_to_search_for.extend(['*-sur_refl_b*.tif', '*-LST*.tif',
                                '*-Emis_*.tif'])

    try:
        generate_statistics('.', files_to_search_for)
    except Exception, e:
        log("Error: %s" % str(e))
        tb = traceback.format_exc()
        log("Traceback: [%s]" % tb)
        if hasattr(e, 'output'):
            log("Error: Output [%s]" % e.output)
        sys.exit(EXIT_FAILURE)

    sys.exit(EXIT_SUCCESS)
# END - __main__
