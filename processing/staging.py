
'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Provides routines for creating order directories and staging data to them.

History:
  Created Jan/2014 by Ron Dilley, USGS/EROS
'''

import os
import sys
import glob
import errno

# imports from espa_common
from logger_factory import EspaLogging
import settings
import utilities

# local objects and methods
from environment import Environment
import espa_exception as ee
import transfer


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
    except Exception as e:
        logger.error("Failed to unpack data")
        raise e
    finally:
        if len(output) > 0:
            logger.info(output)


# ============================================================================
def stage_local_statistics_data(output_dir, work_dir, order_id):
    '''
    Description:
        Stages the statistics using a local directory path.
    '''

    cache_dir = os.path.join(output_dir, settings.ESPA_LOCAL_CACHE_DIRECTORY)
    cache_dir = os.path.join(cache_dir, order_id)
    cache_dir = os.path.join(cache_dir, 'stats')
    cache_files = os.path.join(cache_dir, '*')

    try:
        stats_files = glob.glob(cache_files)

        transfer.copy_files_to_directory(stats_files, work_dir)
    except Exception as e:
        raise ee.ESPAException(ee.ErrorCodes.staging_data, str(e)), \
            None, sys.exc_info()[2]


# ============================================================================
def stage_remote_statistics_data(stage_dir, work_dir, order_id):
    '''
    Description:
        Stages the statistics using scp from a remote location.
    '''

    cache_host = utilities.get_cache_hostname()
    cache_dir = os.path.join(settings.ESPA_REMOTE_CACHE_DIRECTORY, order_id)
    cache_dir = os.path.join(cache_dir, 'stats')

    # Transfer the directory using scp
    try:
        transfer.scp_transfer_directory(cache_host, cache_dir,
                                        'localhost', stage_dir)
    except Exception as e:
        raise ee.ESPAException(ee.ErrorCodes.staging_data, str(e)), \
            None, sys.exc_info()[2]

    # Move the staged data to the work directory
    try:
        stats_files = glob.glob(os.path.join(stage_dir, 'stats/*'))

        transfer.move_files_to_directory(stats_files, work_dir)
    except Exception as e:
        raise ee.ESPAException(ee.ErrorCodes.unpacking, str(e)), \
            None, sys.exc_info()[2]


# ============================================================================
def stage_statistics_data(output_dir, stage_dir, work_dir, parms):
    '''
    Description:
        Stages the statistics data, either by using scp from a remote location,
        or by just copying them from a local disk path.
    '''

    e = Environment()

    distribution_method = e.get_distribution_method()

    order_id = parms['orderid']
    options = parms['options']

    if distribution_method == 'local':
        stage_local_statistics_data(output_dir, work_dir, order_id)

    else:
        stage_remote_statistics_data(stage_dir, work_dir, order_id)
