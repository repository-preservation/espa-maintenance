
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
from espa_logging import log, debug


# ============================================================================
def get_landsat_metadata(work_dir):
    '''
    Description:
      Returns the Landsat metadata as a python dictionary
    '''

    # Find the metadata file
    metadata_filename = ''
    dir_items = os.listdir(work_dir)

    for dir_item in dir_items:
        if ((dir_item.find('_MTL') > 0) and
                not (dir_item.find('old') > 0) and
                not dir_item.startswith('lnd')):

            # Save the filename and break out of the directory loop
            metadata_filename = dir_item
            log("Located MTL file:%s" % metadata_filename)
            break

    if metadata_filename == '':
        msg = "Could not locate the Landsat MTL file in %s" % work_dir
        raise RuntimeError(msg)

    # Save the current directory and change to the work directory
    current_directory = os.getcwd()
    os.chdir(work_dir)

    try:
        # Backup the original file
        copy_filename = metadata_filename + '.old'
        shutil.copy(metadata_filename, copy_filename)

        # Read in the file and write it back out to get rid of binary
        # characters at the end of some of the GLS metadata files
        file = open(metadata_filename, 'r')
        file_data = file.readlines()
        file.close()

        buffer = StringIO()
        for line in file_data:
            buffer.write(line)

        # Fix the stupid error where the filename has a bad extention
        metadata_filename = metadata_filename.replace('.TIF', '.txt')

        file = open(metadata_filename, 'w+')
        fixed_data = buffer.getvalue()
        file.write(fixed_data)
        file.flush()
        file.close()

    finally:
        # Change back to the original directory
        os.chdir(current_directory)

    metadata = dict()
    # First add the filename to the dictionary
    metadata['metadata_filename'] = metadata_filename

    # Read and add the metadata contents to the dictionary
    for line in fixed_data.split('\n'):
        line = line.strip()
        debug(line)
        if not line.startswith('END') and not line.startswith('GROUP'):
            parts = line.split('=')
            if len(parts) == 2:
                metadata[parts[0].strip()] = parts[1].strip().replace('"', '')

    return metadata
# END - get_landsat_metadata
