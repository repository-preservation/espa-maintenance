
'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Provides routines for retrieving or updating metadata from the various
  sensor data.

History:
  Created Jan/2014 by Ron Dilley, USGS/EROS
'''

import os
import shutil
import glob
from cStringIO import StringIO

# imports from espa_common through processing.__init__.py
from processing import EspaLogging
from processing import settings


# ============================================================================
def get_landsat_metadata(work_dir, product_id):
    '''
    Description:
        Fixes potentially bad MTL file from Landsat and returns the Landsat
        metadata filename to use with the rest of the processing.
    '''
    logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)

    # Find the metadata file
    metadata_filename = ''

    # Save the current directory and change to the work directory
    current_directory = os.getcwd()
    os.chdir(work_dir)

    try:
        for meta_file in glob.glob('%s_MTL.*' % product_id):
            if ('old' not in meta_file
                    and not meta_file.startswith('lnd')):

                # Save the filename and break out of the directory loop
                metadata_filename = meta_file
                break

        if metadata_filename == '':
            msg = "Could not locate the Landsat MTL file in %s" % work_dir
            raise RuntimeError(msg)

        logger.info("Located MTL file:%s" % metadata_filename)

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
        if metadata_filename.endswith('.TIF'):
            metadata_filename = metadata_filename.replace('.TIF', '.txt')

        # Write the newly formatted file out
        with open(metadata_filename, 'w+') as metadata_fd:
            metadata_fd.write(fixed_data)

    finally:
        # Change back to the original directory
        os.chdir(current_directory)

    return metadata_filename
# END - get_landsat_metadata
