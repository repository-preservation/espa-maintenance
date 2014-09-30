
'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Provides routines for retrieving metadata from the various sensor data.

History:
  Created Jan/2014 by Ron Dilley, USGS/EROS
'''

import os
import shutil
from cStringIO import StringIO

# espa-common objects and methods
from espa_constants import *

# imports from espa/espa_common
try:
    from logger_factory import EspaLogging
except:
    from espa_common.logger_factory import EspaLogging


# ============================================================================
def get_landsat_metadata(work_dir):
    '''
    Description:
      Returns the Landsat metadata as a python dictionary
    '''
    logger = EspaLogging.get_logger('espa.processing')

    # Find the metadata file
    metadata_filename = ''
    dir_items = os.listdir(work_dir)

    for dir_item in dir_items:
        if ((dir_item.find('_MTL') > 0) and
                not (dir_item.find('old') > 0) and
                not dir_item.startswith('lnd')):

            # Save the filename and break out of the directory loop
            metadata_filename = dir_item
            logger.info("Located MTL file:%s" % metadata_filename)
            break

    if metadata_filename == '':
        msg = "Could not locate the Landsat MTL file in %s" % work_dir
        raise RuntimeError(msg)

    # Save the current directory and change to the work directory
    current_directory = os.getcwd()
    os.chdir(work_dir)

    try:
        # Backup the original file
        shutil.copy(metadata_filename, ''.join([metadata_filename, '.old']))

        file_data = list()
        # Read in the file and write it back out to get rid of binary
        # characters at the end of some of the GLS metadata files
        with open(metadata_filename, 'r') as metadata_fd:
            file_data = metadata_fd.readlines()

        data_buffer = StringIO()
        for line in file_data:
            data_buffer.write(line)
        fixed_data = data_buffer.getvalue()

        # Fix the stupid error where the filename has a bad extention
        metadata_filename = metadata_filename.replace('.TIF', '.txt')

        # Write the newly formatted file out
        with open(metadata_filename, 'w+') as metadata_fd:
            metadata_fd.write(fixed_data)

    finally:
        # Change back to the original directory
        os.chdir(current_directory)

    metadata = dict()
    # First add the filename to the dictionary
    metadata['metadata_filename'] = metadata_filename

    # Read and add the metadata contents to the dictionary
    for line in fixed_data.split('\n'):
        line = line.strip()
        logger.debug(line)
        if not line.startswith('END') and not line.startswith('GROUP'):
            parts = line.split('=')
            if len(parts) == 2:
                metadata[parts[0].strip()] = parts[1].strip().replace('"', '')

    return metadata
# END - get_landsat_metadata
