#! /usr/bin/env python

'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Read all lines from STDIN and process them.

History:
  Created Jan/2014 by Ron Dilley, USGS/EROS

    Date              Programmer               Reason
    ----------------  ------------------------ -------------------------------
    Jan/2014          Ron Dilley               Initial implementation
    Sept/2014         Ron Dilley               Updated to use espa_common and
                                               our python logging setup
                                               Updated to use Hadoop
    Oct/2014          Ron Dilley               Renamed to ondemand and updated
                                               to perform all of our ondemand
                                               map operations
'''

import sys
import socket
import json
import xmlrpclib
from argparse import ArgumentParser

# imports from espa_common through processing.__init__.py
from processing import EspaLogging
from processing import settings
from processing import sensor

# local objects and methods
import espa_exception as ee
import parameters
import processor


# ============================================================================
def set_product_error(server, order_id, product_id, processing_location):

    logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)
    logged_contents = EspaLogging.read_logger_file(settings.PROCESSING_LOGGER)

    if server is not None:
        try:
            status = server.set_scene_error(product_id, order_id,
                                            processing_location,
                                            logged_contents)

            if not status:
                logger.critical("Failed processing xmlrpc call to"
                                " set_scene_error")
                return False

        except Exception:
            logger.critical("Failed processing xmlrpc call to"
                            " set_scene_error")
            logger.exception("Exception encountered and follows")

            return False

    return True


# ============================================================================
def process(args):
    '''
    Description:
      Read all lines from STDIN and process them.  Each line is converted to
      a JSON dictionary of the parameters for processing.  Validation is
      performed on the JSON dictionary to test if valid for this mapper.
      After validation the generation of the products is performed.
    '''

    # Initially set to the base logger
    logger = EspaLogging.get_logger('base')

    processing_location = socket.gethostname()

    # Process each line from stdin
    for line in sys.stdin:
        if not line or len(line) < 1 or not line.strip().startswith('{'):
            continue

        # Reset these for each line
        (server, order_id, product_id) = (None, None, None)

        # Default to the command line value
        mapper_keep_log = args.keep_log

        try:
            line = line.replace('#', '')
            parms = json.loads(line)

            if not parameters.test_for_parameter(parms, 'options'):
                raise ValueError("Error missing JSON 'options' record")

            # TODO scene will be replaced with product_id someday
            (order_id, product_id, product_type, options) = \
                (parms['orderid'], parms['scene'], parms['product_type'],
                 parms['options'])

            # If it is missing due to above TODO, then add it
            if not parameters.test_for_parameter(parms, 'product_id'):
                parms['product_id'] = product_id

            # Figure out if debug level logging was requested
            debug = False
            if parameters.test_for_parameter(options, 'debug'):
                debug = options['debug']

            # Configure and get the logger for this order request
            EspaLogging.configure(settings.PROCESSING_LOGGER, order=order_id,
                                  product=product_id, debug=debug)
            logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)

            # If the command line option is True don't use the scene option
            if not mapper_keep_log:
                if not parameters.test_for_parameter(options, 'keep_log'):
                    options['keep_log'] = False

                mapper_keep_log = options['keep_log']

            logger.info("Processing %s:%s" % (order_id, product_id))

            # Update the status in the database
            if parameters.test_for_parameter(parms, 'xmlrpcurl'):
                if parms['xmlrpcurl'] != 'skip_xmlrpc':
                    server = xmlrpclib.ServerProxy(parms['xmlrpcurl'])
                    if server is not None:
                        status = server.update_status(product_id, order_id,
                                                      processing_location,
                                                      'processing')
                        if not status:
                            logger.warning("Failed processing xmlrpc call"
                                           " to update_status to processing")

            if product_id != 'plot':
                # Make sure we can process the sensor
                sensor_name = sensor.instance(product_id).sensor_name
                if sensor_name not in parameters.valid_sensors:
                    raise ValueError("Invalid Sensor %s" % sensor_name)

                # Make sure we have a valid output format
                if not parameters.test_for_parameter(options, 'output_format'):
                    logger.warning("'output_format' parameter missing"
                                   " defaulting to envi")
                    options['output_format'] = 'envi'

                if (options['output_format']
                        not in parameters.valid_output_formats):

                    raise ValueError("Invalid Output format %s"
                                     % options['output_format'])

            # ----------------------------------------------------------------
            # NOTE: The first thing the product processor does during
            #       initialization is validate the input parameters.
            # ----------------------------------------------------------------

            destination_product_file = 'ERROR'
            destination_cksum_file = 'ERROR'
            pp = None
            try:
                # All processors are implemented in the processor module
                pp = processor.get_instance(parms)
                (destination_product_file, destination_cksum_file) = \
                    pp.process()

            finally:
                # Free disk space to be nice to the whole system.
                if not mapper_keep_log and pp is not None:
                    pp.remove_product_directory()

            # Everything was successfull so mark the scene complete
            if server is not None:
                status = server.mark_scene_complete(product_id, order_id,
                                                    processing_location,
                                                    destination_product_file,
                                                    destination_cksum_file, "")
                if not status:
                    logger.warning("Failed processing xmlrpc call to"
                                   " mark_scene_complete")

            # Cleanup the log file
            if not mapper_keep_log:
                EspaLogging.delete_logger_file(settings.PROCESSING_LOGGER)

            # Reset back to the base logger
            logger = EspaLogging.get_logger('base')

        except ee.ESPAException, e:

            # First log the exception
            if hasattr(e, 'output'):
                logger.error("Code [%s]" % str(e.error_code))
            if hasattr(e, 'output'):
                logger.error("Output [%s]" % e.output)
            logger.exception("Exception encountered and follows")

            # Log the error information to the server
            # Depending on the error_code do something different
            # TODO - Today we are failing everything, but some things could be
            #        made recovereable in the future.
            #        So this code seems a bit ridiculous.
            status = False
            if server is not None:
                try:
                    if (e.error_code == ee.ErrorCodes.creating_stage_dir
                            or (e.error_code ==
                                ee.ErrorCodes.creating_work_dir)
                            or (e.error_code ==
                                ee.ErrorCodes.creating_output_dir)):

                        status = set_product_error(server,
                                                   order_id,
                                                   product_id,
                                                   processing_location)

                    elif (e.error_code == ee.ErrorCodes.staging_data
                          or e.error_code == ee.ErrorCodes.unpacking):

                        status = set_product_error(server,
                                                   order_id,
                                                   product_id,
                                                   processing_location)

                    elif (e.error_code == ee.ErrorCodes.metadata
                          or e.error_code == ee.ErrorCodes.surface_reflectance
                          or e.error_code == ee.ErrorCodes.browse
                          or e.error_code == ee.ErrorCodes.spectral_indices
                          or e.error_code == ee.ErrorCodes.create_dem
                          or e.error_code == ee.ErrorCodes.solr
                          or e.error_code == ee.ErrorCodes.cfmask
                          or e.error_code == ee.ErrorCodes.dswe
                          or e.error_code == ee.ErrorCodes.cleanup_work_dir
                          or e.error_code == ee.ErrorCodes.remove_products):

                        status = set_product_error(server,
                                                   order_id,
                                                   product_id,
                                                   processing_location)

                    elif e.error_code == ee.ErrorCodes.warping:

                        status = set_product_error(server,
                                                   order_id,
                                                   product_id,
                                                   processing_location)

                    elif e.error_code == ee.ErrorCodes.reformat:

                        status = set_product_error(server,
                                                   order_id,
                                                   product_id,
                                                   processing_location)

                    elif e.error_code == ee.ErrorCodes.statistics:

                        status = set_product_error(server,
                                                   order_id,
                                                   product_id,
                                                   processing_location)

                    elif (e.error_code == ee.ErrorCodes.packaging_product
                          or (e.error_code ==
                              ee.ErrorCodes.distributing_product)
                          or (e.error_code ==
                              ee.ErrorCodes.verifying_checksum)):

                        status = set_product_error(server,
                                                   order_id,
                                                   product_id,
                                                   processing_location)

                    else:
                        # Catch all remaining errors
                        status = set_product_error(server,
                                                   order_id,
                                                   product_id,
                                                   processing_location)

                    if status and not mapper_keep_log:
                        try:
                            # Cleanup the log file
                            EspaLogging. \
                                delete_logger_file(settings.PROCESSING_LOGGER)
                        except Exception, e:
                            logger.exception("Exception encountered"
                                             " stacktrace follows")

                except Exception, e:
                    logger.exception("Exception encountered and follows")
            # END - if server is not None

        except Exception, e:

            # First log the exception
            if hasattr(e, 'output'):
                logger.error("Output [%s]" % e.output)
            logger.exception("Exception encountered stacktrace follows")

            if server is not None:

                try:
                    status = set_product_error(server,
                                               order_id,
                                               product_id,
                                               processing_location)
                    if status and not mapper_keep_log:
                        try:
                            # Cleanup the log file
                            EspaLogging. \
                                delete_logger_file(settings.PROCESSING_LOGGER)
                        except Exception, e:
                            logger.exception("Exception encountered"
                                             " stacktrace follows")
                except Exception, e:
                    logger.exception("Exception encountered stacktrace"
                                     " follows")
    # END - for line in STDIN


# ============================================================================
if __name__ == '__main__':
    '''
    Description:
        Some parameter and logging setup, then call the process routine.
    '''

    # Grab our only command line parameter
    parser = ArgumentParser(
        description="Processes a list of scenes from stdin")
    parser.add_argument('--keep-log', action='store_true', dest='keep_log',
                        default=False, help="keep the generated log file")
    args = parser.parse_args()

    EspaLogging.configure_base_logger(filename='/tmp/espa-ondemand-mapper.log')
    # Initially set to the base logger
    logger = EspaLogging.get_logger('base')

    try:
        process(args)
    except Exception, e:
        logger.exception("Processing failed stacktrace follows")

    sys.exit(0)
