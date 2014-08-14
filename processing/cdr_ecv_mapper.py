#! /usr/bin/env python

'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Read all lines from STDIN and process them.

History:
  Created Jan/2014 by Ron Dilley, USGS/EROS
'''

import os
import sys
import socket
import json
import xmlrpclib
import traceback
from argparse import ArgumentParser

# espa-common objects and methods
from espa_constants import *
from espa_logging import open_log_handler, close_log_handler, log, set_debug

# local objects and methods
import espa_exception as ee
import parameters
import util
import cdr_ecv
import modis


# ============================================================================
if __name__ == '__main__':
    '''
    Description:
      Read all lines from STDIN and process them.  Each line is converted to a
      JSON dictionary of the parameters for processing.  Validation is
      performed on the JSON dictionary to test if valid for this mapper.
      After validation the generation of cdr_ecv products is performed.
    '''

    # Grab our only command line parameter
    parser = ArgumentParser(
        description="Processes a list of scenes from stdin")
    parser.add_argument('--keep-log', action='store_true', dest='keep_log',
                        default=False, help="keep the generated log file")
    args = parser.parse_args()

    processing_location = socket.gethostname()

    # Process each line from stdin
    for line in sys.stdin:
        if not line or len(line) < 1 or not line.strip().startswith('{'):
            continue

        # Reset these for each line
        (server, orderid, sceneid) = (None, None, None)
        # Default to the command line value
        scene_keep_log = args.keep_log

        log_filename = None
        try:
            line = line.replace('#', '')
            parms = json.loads(line)

            if not parameters.test_for_parameter(parms, 'options'):
                raise ValueError("Error missing JSON 'options' record")

            (orderid, sceneid, options) = (parms['orderid'], parms['scene'],
                                           parms['options'])

            # If the command line option is True don't use the scene option
            if not scene_keep_log:
                if not parameters.test_for_parameter(options, 'keep_log'):
                    options['keep_log'] = False

                scene_keep_log = options['keep_log']

            # Create the log file
            log_filename = util.get_logfile(orderid, sceneid)
            status = open_log_handler(log_filename)
            if status != SUCCESS:
                raise Exception("Error failed to create log handler")

            if parameters.test_for_parameter(options, 'debug'):
                set_debug(options['debug'])

            log("Processing %s:%s" % (orderid, sceneid))

            sensor = util.get_sensor(parms['scene'])

            # Update the status in the database
            if parameters.test_for_parameter(parms, 'xmlrpcurl'):
                if parms['xmlrpcurl'] != 'skip_xmlrpc':
                    server = xmlrpclib.ServerProxy(parms['xmlrpcurl'])
                    if server is not None:
                        status = server.update_status(sceneid, orderid,
                                                      processing_location,
                                                      'processing')
                        if not status:
                            log("Failed processing xmlrpc call"
                                " to update_status to processing")

            # Make sure we can process the sensor
            if sensor not in parameters.valid_sensors:
                raise ValueError("Invalid Sensor %s" % sensor)

            # Make sure we have a valid output format
            if not parameters.test_for_parameter(options, 'output_format'):
                log("Warning: 'output_format' parameter missing defaulting"
                    " to envi")
                options['output_format'] = 'envi'

            if (options['output_format']
                    not in parameters.valid_output_formats):

                raise ValueError("Invalid Output format %s"
                                 % options['output_format'])

            # -----------------------------------------------------------------
            # NOTE:
            #   The first thing process does is validate the input parameters
            # -----------------------------------------------------------------

            # Generate the command line that can be used with the specified
            # application
            cmd_line_options = \
                parameters.convert_to_command_line_options(parms)

            destination_product_file = 'ERROR'
            destination_cksum_file = 'ERROR'
            # Process the landsat sensors
            if sensor in parameters.valid_landsat_sensors:
                log("Processing cdr_ecv with [%s]"
                    % ' '.join(cmd_line_options))
                (destination_product_file, destination_cksum_file) = \
                    cdr_ecv.process(parms)
            # Process the modis sensors
            elif sensor in parameters.valid_modis_sensors:
                log("Processing modis with [%s]" % ' '.join(cmd_line_options))
                (destination_product_file, destination_cksum_file) = \
                    modis.process(parms)

            # -----------------------------------------------------------------
            # NOTE: Else process using another sensors processor
            # -----------------------------------------------------------------

            # Everything was successfull so mark the scene complete
            if server is not None:
                status = server.mark_scene_complete(sceneid, orderid,
                                                    processing_location,
                                                    destination_product_file,
                                                    destination_cksum_file, "")
                if not status:
                    log("Failed processing xmlrpc call to mark_scene_complete")

            # Always log where we placed the files
            log("Delivered product to %s at location %s and cksum location %s"
                % (processing_location, destination_product_file,
                   destination_cksum_file))

            # Cleanup the log file
            close_log_handler()
            if not scene_keep_log and os.path.exists(log_filename):
                os.unlink(log_filename)

        except ee.ESPAException, e:

            log_data = ''
            if server is not None:

                # Only close if we have a server to give the log to
                close_log_handler()

                # Grab the log file information
                if log_filename is not None and os.path.exists(log_filename):
                    with open(log_filename, "r") as log_fd:
                        log_data = log_fd.read()

            # Add the exception text
            log_data = ''.join([log_data, '\n', str(e)])

            # Log the error information to the server
            # Depending on the error_code do something different
            # TODO - Today we are failing everything, but some things could be
            #        made recovereable in the future.
            #        So this code seems a bit ridiculous.
            status = False
            if (e.error_code == ee.ErrorCodes.creating_stage_dir
                    or e.error_code == ee.ErrorCodes.creating_work_dir
                    or e.error_code == ee.ErrorCodes.creating_output_dir):

                if server is not None:
                    status = server.set_scene_error(sceneid, orderid,
                                                    processing_location,
                                                    log_data)

            elif (e.error_code == ee.ErrorCodes.staging_data
                  or e.error_code == ee.ErrorCodes.unpacking):

                if server is not None:
                    status = server.set_scene_error(sceneid, orderid,
                                                    processing_location,
                                                    log_data)

            elif (e.error_code == ee.ErrorCodes.metadata
                  or e.error_code == ee.ErrorCodes.ledaps
                  or e.error_code == ee.ErrorCodes.browse
                  or e.error_code == ee.ErrorCodes.spectral_indices
                  or e.error_code == ee.ErrorCodes.create_dem
                  or e.error_code == ee.ErrorCodes.solr
                  or e.error_code == ee.ErrorCodes.cfmask
                  or e.error_code == ee.ErrorCodes.cfmask_append
                  or e.error_code == ee.ErrorCodes.swe
                  or e.error_code == ee.ErrorCodes.sca
                  or e.error_code == ee.ErrorCodes.cleanup_work_dir
                  or e.error_code == ee.ErrorCodes.remove_products):

                if server is not None:
                    status = server.set_scene_error(sceneid, orderid,
                                                    processing_location,
                                                    log_data)

            elif e.error_code == ee.ErrorCodes.warping:

                if server is not None:
                    status = server.set_scene_error(sceneid, orderid,
                                                    processing_location,
                                                    log_data)

            elif e.error_code == ee.ErrorCodes.reformat:

                if server is not None:
                    status = server.set_scene_error(sceneid, orderid,
                                                    processing_location,
                                                    log_data)

            elif e.error_code == ee.ErrorCodes.statistics:

                if server is not None:
                    status = server.set_scene_error(sceneid, orderid,
                                                    processing_location,
                                                    log_data)

            elif (e.error_code == ee.ErrorCodes.packaging_product
                  or e.error_code == ee.ErrorCodes.distributing_product
                  or e.error_code == ee.ErrorCodes.verifying_checksum):

                if server is not None:
                    status = server.set_scene_error(sceneid, orderid,
                                                    processing_location,
                                                    log_data)

            else:
                # Catch all remaining errors
                if server is not None:
                    status = server.set_scene_error(sceneid, orderid,
                                                    processing_location,
                                                    log_data)

            if server is not None:
                if not status:
                    status = open_log_handler(log_filename)
                    if status == SUCCESS:
                        log("Failed processing xmlrpc call to set_scene_error")
                else:
                    # Cleanup the log file
                    if not scene_keep_log and os.path.exists(log_filename):
                        os.unlink(log_filename)
            else:
                # Log the error information
                log("An error occurred processing %s" % sceneid)
                log("Error: %s" % str(e))
                if hasattr(e, 'output'):
                    log("Error: Code [%s]" % str(e.error_code))
                if hasattr(e, 'output'):
                    log("Error: Output [%s]" % e.output)
                tb = traceback.format_exc()
                log("Error: Traceback [%s]" % tb)

        except Exception, e:

            if server is not None:
                close_log_handler()

                log_data = ''
                # Grab the log file information
                if log_filename is not None and os.path.exists(log_filename):
                    with open(log_filename, "r") as log_fd:
                        log_data = log_fd.read()

                # Add the exception text
                log_data = ''.join([log_data, '\n', str(e)])

                status = server.set_scene_error(sceneid, orderid,
                                                processing_location, log_data)

                if not status:
                    status = open_log_handler(log_filename)
                    if status == SUCCESS:
                        log("Failed processing xmlrpc call to set_scene_error")
                else:
                    # Cleanup the log file
                    if not scene_keep_log and os.path.exists(log_filename):
                        os.unlink(log_filename)
            else:
                # Log the error information
                log("An error occurred processing %s" % sceneid)
                log("Error: %s" % str(e))
                if hasattr(e, 'output'):
                    log("Error: Output [%s]" % e.output)
                tb = traceback.format_exc()
                log("Error: Traceback [%s]" % tb)
                log("Error: Line [%s]" % line)

        finally:
            close_log_handler()
    # END - for line in STDIN

    sys.exit(EXIT_SUCCESS)
