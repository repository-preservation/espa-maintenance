
'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Provides routines for creating order directories and staging data to them.

History:
  Created Jan/2014 by Ron Dilley, USGS/EROS
'''

import os
import errno

# imports from espa_common through processing.__init__.py
from processing import EspaLogging
from processing import settings
from processing import utilities


# ============================================================================
def create_directory(directory):
    '''
    Description:
        Create the specified directory with some error checking.
    '''

    # Create/Make sure the directory exists
    try:
        os.makedirs(directory, mode=0755)
    except OSError as ose:
        if ose.errno == errno.EEXIST and os.path.isdir(directory):
            pass
        else:
            raise
# END - create_directory


# ============================================================================
def untar_data(source_file, destination_directory):
    '''
    Description:
      Using tar extract the file contents into a destination directory.

    Notes:
      Works with '*.tar.gz' and '*.tar' files.
    '''

    logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)

    # If both source and destination are localhost we can just copy the data
    cmd = ' '.join(['tar', '--directory', destination_directory,
                    '-xvf', source_file])

    logger.info("Unpacking [%s] to [%s]"
                % (source_file, destination_directory))

    # Unpack the data and raise any errors
    output = ''
    try:
        output = utilities.execute_cmd(cmd)
    except Exception, e:
        logger.error("Failed to unpack data")
        raise e
    finally:
        if len(output) > 0:
            logger.info(output)
# END - untar_data
