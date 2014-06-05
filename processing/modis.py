#! /usr/bin/env python

'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Integration script for the EROS Science Processing Architecture (ESPA)
  Processes MODIS (Terra and Aqua) data.

History:
  Original Development Jan/2014 by Ron Dilley, USGS/EROS
    - Used cdr_ecv.py as the template for this.
'''

import os
import sys
import re
import glob
import json
from time import sleep
from datetime import datetime
from argparse import ArgumentParser
import traceback

# espa-common objects and methods
from espa_constants import *
from espa_logging import log, set_debug, debug

# local objects and methods
import espa_exception as ee
import parameters
import util
import transfer
import staging
import warp
import statistics
import distribution
import settings


# ============================================================================
def build_argument_parser():
    '''
    Description:
      Build the command line argument parser.
    '''

    # Create a command line argument parser
    description = "Processes MODIS (Terra and Aqua) data"
    parser = ArgumentParser(description=description)

    # Parameters
    parameters.add_debug_parameter(parser)

    parameters.add_orderid_parameter(parser)

    parameters.add_scene_parameter(parser)

    parameters.add_output_format_parameter(parser,
                                           parameters.valid_output_formats)

    parameters.add_source_parameters(parser)
    parameters.add_destination_parameters(parser)

    parameters.add_reprojection_parameters(parser,
                                           warp.valid_projections,
                                           warp.valid_ns,
                                           warp.valid_pixel_size_units,
                                           warp.valid_resample_methods,
                                           warp.valid_datums)

    parameters.add_include_statistics_parameter(parser)

    return parser
# END - build_argument_parser


# ============================================================================
def validate_parameters(parms):
    '''
    Description:
      Make sure all the parameter options needed for this and called routines
      is available with the provided input parameters.
    '''

    # Test for presence of top-level parameters
    keys = ['orderid', 'scene', 'options']
    for key in keys:
        if not parameters.test_for_parameter(parms, key):
            raise RuntimeError("Missing required input parameter [%s]" % key)

    # Get a local pointer to the options
    options = parms['options']

    # Validate the reprojection parameters
    parameters.validate_reprojection_parameters(options,
                                                warp.valid_projections,
                                                warp.valid_ns,
                                                warp.valid_pixel_size_units,
                                                warp.valid_resample_methods,
                                                warp.valid_datums)

    # Force these parameters to false if not provided
    keys = ['include_statistics']

    for key in keys:
        if not parameters.test_for_parameter(options, key):
            options[key] = False

    # Extract information from the scene string
    sensor = util.getSensor(parms['scene'])

    if sensor not in parameters.valid_modis_sensors:
        raise NotImplementedError("Data sensor %s is not implemented" % sensor)

    # Add the sensor to the options
    options['sensor'] = sensor

    # Setup the base paths
    if sensor == 'MOD':
        base_source_path = settings.terra_base_source_path
    else:
        base_source_path = settings.aqua_base_source_path

    # Verify or set the source information
    if not parameters.test_for_parameter(options, 'source_host'):
        options['source_host'] = 'localhost'

    if not parameters.test_for_parameter(options, 'source_username'):
        options['source_username'] = None

    if not parameters.test_for_parameter(options, 'source_pw'):
        options['source_pw'] = None

    if not parameters.test_for_parameter(options, 'source_directory'):
        short_name = util.getModisShortName(parms['scene'])
        version = util.getModisVersion(parms['scene'])
        archive_date = util.getModisArchiveDate(parms['scene'])
        options['source_directory'] = '%s/%s.%s/%s' \
            % (base_source_path, short_name, version, archive_date)

    # Verify or set the destination information
    if not parameters.test_for_parameter(options, 'destination_host'):
        options['destination_host'] = 'localhost'

    if not parameters.test_for_parameter(options, 'destination_username'):
        options['destination_username'] = 'localhost'

    if not parameters.test_for_parameter(options, 'destination_pw'):
        options['destination_pw'] = 'localhost'

    if not parameters.test_for_parameter(options, 'destination_directory'):
        options['destination_directory'] = '%s/orders/%s' \
            % (settings.espa_base_output_path, parms['orderid'])
# END - validate_parameters


# ============================================================================
def build_product_name(scene):
    '''
    Description:
      Build the product name from the scene information and current time.
    '''

    # Get the current time information
    ts = datetime.today()

    # Extract stuff from the scene
    short_name = util.getModisShortName(scene)
    (horizontal, vertical) = util.getModisHorizontalVertical(scene)
    (year, doy) = util.getModisSceneDate(scene)

    product_name = '%s%s%s%s%s-SC%s%s%s%s%s%s' \
        % (short_name, horizontal.zfill(3), vertical.zfill(3), year.zfill(4),
           doy.zfill(3), str(ts.year).zfill(4), str(ts.month).zfill(2),
           str(ts.day).zfill(2), str(ts.hour).zfill(2),
           str(ts.minute).zfill(2), str(ts.second).zfill(2))

    return product_name
# END - build_product_name


# ============================================================================
def process(parms):
    '''
    Description:
      Provides the processing for the generation of the science products and
      then processing them through the statistics generation.
    '''

    # Validate the parameters
    validate_parameters(parms)

    scene = parms['scene']

    # Create and retrieve the directories to use for processing
    (scene_directory, stage_directory, work_directory, package_directory) = \
        staging.initialize_processing_directory(parms['orderid'], scene)

    # Keep a local options for those apps that only need a few things
    options = parms['options']
    sensor = options['sensor']

    # Add the work directory to the parameters
    options['work_directory'] = work_directory

    # Figure out the product name
    product_name = build_product_name(scene)

    # Stage the modis data
    filename = staging.stage_modis_data(scene,
                                        options['source_host'],
                                        options['source_directory'],
                                        stage_directory)
    log(filename)

    # Copy the staged data to the work directory
    try:
        transfer.copy_file_to_file(filename, work_directory)
        os.unlink(filename)
    except Exception, e:
        raise ESPAException(ErrorCodes.unpacking, str(e)), \
            None, sys.exc_info()[2]

    # Change to the working directory
    current_directory = os.getcwd()
    os.chdir(options['work_directory'])

    # The format of MODIS data is HDF we are going to process using GeoTIFF
    # for the time being
    hdf_filename = glob.glob('*.hdf')[0]  # Should only be one file
    xml_filename = hdf_filename.replace('.hdf', '.xml')

    # Convert lpgs to espa first
    # Call with deletion of source files
    cmd = ['convert_modis_to_espa',
           '--hdf', hdf_filename,
           '--xml', xml_filename]
    if not options['include_sourcefile']:
        cmd += ['--del_src_files']

    cmd = ' '.join(cmd)
    log('CONVERT MODIS TO ESPA COMMAND:' + cmd)

    output = ''
    try:
        output = util.execute_cmd(cmd)
    except Exception, e:
        raise ee.ESPAException(ee.ErrorCodes.reformat, str(e)), \
            None, sys.exc_info()[2]
    finally:
        if len(output) > 0:
            log(output)
        # Change back to the previous directory
        os.chdir(current_directory)

    # Reproject the data for each science product, but only if necessary
    if (options['reproject'] or options['resize'] or options['image_extents']
            or options['projection'] is not None):

        warp.warp_espa_data(options, xml_filename)

    # Generate the stats for each stat'able' science product
    if options['include_statistics']:
        # Find the files
        files_to_search_for = ['*.sur_refl_b*.img']
        files_to_search_for += ['*.LST_Day_1km.img']
        files_to_search_for += ['*.LST_Night_1km.img']
        files_to_search_for += ['*.LST_Day_6km.img']
        files_to_search_for += ['*.LST_Night_6km.img']
        files_to_search_for += ['*.Emis_*.img']
        files_to_search_for += ['*NDVI.img']
        files_to_search_for += ['*EVI.img']
        # Generate the stats for each file
        statistics.generate_statistics(options['work_directory'],
                                       files_to_search_for)

    # Convert to the user requested output format or leave it in ESPA ENVI
    # We do all of our processing using ESPA ENVI format so it can be
    # hard-coded here
    warp.reformat(xml_filename, work_directory, 'envi',
                  options['output_format'])

    # Deliver the product files
    # Attempt X times sleeping between each attempt
    sleep_seconds = settings.default_sleep_seconds
    max_number_of_attempts = settings.max_distribution_attempts
    attempt = 0
    destination_product_file = 'ERROR'
    destination_cksum_file = 'ERROR'
    while True:
        try:
            # Deliver product will also try each of its parts three times
            # before failing, so we pass our sleep seconds down to them
            (destination_product_file, destination_cksum_file) = \
                distribution.deliver_product(work_directory,
                                             package_directory,
                                             product_name,
                                             options['destination_host'],
                                             options['destination_directory'],
                                             options['destination_username'],
                                             options['destination_pw'],
                                             options['include_statistics'],
                                             sleep_seconds)
        except Exception, e:
            log("An error occurred processing %s" % scene)
            log("Error: %s" % str(e))
            if attempt < max_number_of_attempts:
                sleep(sleep_seconds)  # sleep before trying again
                attempt += 1
                sleep_seconds = int(sleep_seconds * 1.5)  # adjust for next set
                continue
            else:
                raise e  # May already be an ESPAException, don't override that
        break

    # Let the caller know where we put these on the destination system
    return (destination_product_file, destination_cksum_file)
# END - process


# ============================================================================
if __name__ == '__main__':
    '''
    Description:
      Read parameters from the command line and build a JSON dictionary from
      them.  Pass the JSON dictionary to the process routine.
    '''

    # Create the JSON dictionary to use
    parms = dict()

    # Build the command line argument parser
    parser = build_argument_parser()

    # Parse the arguments and place them into a dictionary
    args = parser.parse_args()
    args_dict = vars(args)

    # Setup debug
    set_debug(args.debug)

    # Build our JSON formatted input from the command line parameters
    orderid = args_dict.pop('orderid')
    scene = args_dict.pop('scene')
    options = {k: args_dict[k] for k in args_dict if args_dict[k] is not None}

    # Build the JSON parameters dictionary
    parms['orderid'] = orderid
    parms['scene'] = scene
    parms['options'] = options

    # Call the process routine with the JSON parameters
    try:
        process(parms)
    except Exception, e:
        log("An error occurred processing %s" % scene)
        log("Error: %s" % str(e))
        tb = traceback.format_exc()
        log("Traceback: [%s]" % tb)
        if hasattr(e, 'output'):
            log("Error: Output [%s]" % e.output)
        sys.exit(EXIT_FAILURE)

    sys.exit(EXIT_SUCCESS)
