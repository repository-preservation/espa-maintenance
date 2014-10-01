
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
import sys
import uuid
import shutil
import glob

# espa-common objects and methods
from espa_constants import *

# imports from espa/espa_common
try:
    from logger_factory import EspaLogging
except:
    from espa_common.logger_factory import EspaLogging

try:
    import utilities
except:
    from espa_common import utilities

# local objects and methods
import espa_exception as ee
import transfer


espa_base_working_dir_envvar = 'ESPA_WORK_DIR'


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

    logger = EspaLogging.get_logger('espa.processing')

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


# ============================================================================
def initialize_processing_directory(orderid, scene):
    '''
    Description:
        Create the procesing directory for a scene along with it's
        sub-directories.  If the environment variable is not set use the
        current working directory as the base starting point.
    '''

    global espa_base_working_dir_envvar

    logger = EspaLogging.get_logger('espa.processing')

    order_directory = ''

    if espa_base_working_dir_envvar not in os.environ:
        logger.warning("Environment variable $%s is not defined"
                       % espa_working_dir_var)
    else:
        order_directory = os.environ.get(espa_base_working_dir_envvar)

    # Get the absolute path to the directory, and default to the current one
    if order_directory == '':
        # If the directory is empty, use the current working directory
        order_directory = os.getcwd()
    else:
        # Get the absolute path
        order_directory = os.path.abspath(order_directory)

    # Specify a specific directory using the orderid
    order_directory = os.path.join(order_directory, str(orderid))

    # Specify the scene sub-directory
    scene_directory = os.path.join(order_directory, scene)
    # Just incase remove it, and we don't care about errors
    shutil.rmtree(scene_directory, ignore_errors=True)

    # Specify the sub-directories of a processing directory
    stage_directory = os.path.join(scene_directory, 'stage')
    work_directory = os.path.join(scene_directory, 'work')
    output_directory = os.path.join(scene_directory, 'output')

    # Create each of the leaf sub-directories
    try:
        create_directory(stage_directory)
    except Exception, e:
        raise ee.ESPAException(ee.ErrorCodes.creating_stage_dir,
                               str(e)), None, sys.exc_info()[2]

    try:
        create_directory(work_directory)
    except Exception, e:
        raise ee.ESPAException(ee.ErrorCodes.creating_work_dir,
                               str(e)), None, sys.exc_info()[2]

    try:
        create_directory(output_directory)
    except Exception, e:
        raise ee.ESPAException(ee.ErrorCodes.creating_output_dir,
                               str(e)), None, sys.exc_info()[2]

    return (scene_directory, stage_directory, work_directory, output_directory)
# END - initialize_processing_directory


# ============================================================================
def stage_landsat_data(scene, source_host, source_directory,
                       destination_host, destination_directory,
                       source_username, source_pw):
    '''
    Description:
      Stages landsat input data and places it on the localhost in the
      specified destination directory
    '''

    filename = '%s.tar.gz' % scene

    source_file = '%s/%s' % (source_directory, filename)
    destination_file = '%s/%s' % (destination_directory, filename)

    try:
        transfer.transfer_file(source_host, source_file,
                               destination_host, destination_file,
                               source_username=source_username,
                               source_pw=source_pw)
    except Exception, e:
        raise ee.ESPAException(ee.ErrorCodes.staging_data,
                               str(e)), None, sys.exc_info()[2]

    return destination_file
# END - stage_landsat_data


# ============================================================================
def stage_modis_data(scene, source_host, source_directory,
                     destination_directory):
    '''
    Description:
      Stages modis input data and places it on the localhost in the
      specified destination directory
    '''

    filename = '%s.hdf' % scene

    source_file = '%s/%s' % (source_directory, filename)
    destination_file = '%s/%s' % (destination_directory, filename)

    try:
        transfer.http_transfer_file(source_host, source_file, destination_file)
    except Exception, e:
        raise ee.ESPAException(ee.ErrorCodes.staging_data,
                               str(e)), None, sys.exc_info()[2]

    return destination_file
# END - stage_modis_data
