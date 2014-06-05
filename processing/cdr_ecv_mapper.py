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
import traceback

# espa-common objects and methods
from espa_constants import *
from espa_logging import log, debug, set_debug

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

    processing_location = socket.gethostname()

    # make stdin a non-blocking file

    # Have to read stdin all at once because something else is trashing it
    # later on during one or more of the shell out executions.
    bytes_read = 0
    for line in sys.stdin:
        # Reset these for each line
        (server, orderid, sceneid) = (None, None, None)

        bytes_read += len(line)
        debug("#### BYTES READ ####################### %d ####" % bytes_read)
        debug(line)

        try:
            line = line.replace('#', '')
            parms = json.loads(line)

            if not parameters.test_for_parameter(parms, 'options'):
                log("Error missing JSON 'options' record")
                sys.exit(EXIT_FAILURE)

            (orderid, sceneid) = (parms['orderid'], parms['scene'])

            if parameters.test_for_parameter(parms['options'], 'debug'):
                set_debug(parms['options']['debug'])

            log("Processing %s:%s" % (orderid, sceneid))

            sensor = util.getSensor(parms['scene'])

            # Update the status in the database
            if parameters.test_for_parameter(parms, 'xmlrpcurl'):
                if parms['xmlrpcurl'] != 'dev':
                    server = xmlrpclib.ServerProxy(parms['xmlrpcurl'])
                    server.updateStatus(sceneid, orderid, processing_location,
                                        'processing')

            # Make sure we can process the sensor
            if sensor not in parameters.valid_sensors:
                raise ValueError("Invalid Sensor %s" % sensor)

            # Make sure we have a valid output format
            if (parms['options']['output_format']
                    not in parameters.valid_output_formats):

                raise ValueError("Invalid Sensor %s" % sensor)

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
                server.markSceneComplete(sceneid, orderid, processing_location,
                                         destination_product_file,
                                         destination_cksum_file, "")
            else:
                print ("Delivered product to %s at location %s and cksum"
                       " location %s" % (processing_location,
                                         destination_product_file,
                                         destination_cksum_file))

        except ee.ESPAException, e:
            # Log the error information
            # Depending on the error_code do something different
            # TODO - Today we are failing everything, but some things could be
            #        made recovereable in the future.
            if (e.error_code == ee.ErrorCodes.creating_stage_dir
                    or e.error_code == ee.ErrorCodes.creating_work_dir
                    or e.error_code == ee.ErrorCodes.creating_output_dir):

                if server is not None:
                    server.setSceneError(sceneid, orderid,
                                         processing_location, e)

            elif (e.error_code == ee.ErrorCodes.staging_data
                  or e.error_code == ee.ErrorCodes.unpacking):

                if server is not None:
                    server.setSceneError(sceneid, orderid,
                                         processing_location, e)

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
                    server.setSceneError(sceneid, orderid,
                                         processing_location, e)

            elif e.error_code == ee.ErrorCodes.warping:

                if server is not None:
                    server.setSceneError(sceneid, orderid,
                                         processing_location, e)

            elif e.error_code == ee.ErrorCodes.reformat:

                if server is not None:
                    server.setSceneError(sceneid, orderid,
                                         processing_location, e)

            elif e.error_code == ee.ErrorCodes.statistics:

                if server is not None:
                    server.setSceneError(sceneid, orderid,
                                         processing_location, e)

            elif (e.error_code == ee.ErrorCodes.packaging_product
                  or e.error_code == ee.ErrorCodes.distributing_product
                  or e.error_code == ee.ErrorCodes.verifying_checksum):

                if server is not None:
                    server.setSceneError(sceneid, orderid,
                                         processing_location, e)

            else:
                if server is not None:
                    server.setSceneError(sceneid, orderid,
                                         processing_location, e)

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
                server.setSceneError(sceneid, orderid, processing_location, e)

            # Log the error information
            log("An error occurred processing %s" % sceneid)
            log("Error: %s" % str(e))
            if hasattr(e, 'output'):
                log("Error: Output [%s]" % e.output)
            tb = traceback.format_exc()
            log("Error: Traceback [%s]" % tb)
            log("Error: Line [%s]" % line)

    # END - for line in STDIN

    sys.exit(EXIT_SUCCESS)
