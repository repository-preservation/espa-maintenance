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
from espa_constants import EXIT_SUCCESS

# imports from espa/espa_common
try:
    from espa_logging import EspaLogging
except:
    from espa_common.espa_logging import EspaLogging

try:
    import utilities
except:
    from espa_common import utilities

# local objects and methods
import parameters
import plotting as plotter


# ============================================================================
def set_scene_error(product_type, orderid, processing_location):

    logger = EspaLogging.get_logger('espa.processing')
    logged_contents = EspaLogging.read_logger_file('espa.processing')

    if server is not None:
        try:
            status = server.set_scene_error(product_type, orderid,
                                            processing_location,
                                            logged_contents)

            if not status:
                logger.critical("Failed processing xmlrpc call to"
                                " set_scene_error")
                return False

        except Exception, e:
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
        (server, orderid) = (None, None)

        # Default to the command line value
        mapper_keep_log = args.keep_log

        output = ''
        try:
            line = line.replace('#', '')
            parms = json.loads(line)

            if not parameters.test_for_parameter(parms, 'options'):
                raise ValueError("Error missing JSON 'options' record")

            (orderid, product_type, options) = (parms['orderid'],
                                                parms['product_type'],
                                                parms['options'])

            # Figure out if debug level logging was requested
            debug = False
            if parameters.test_for_parameter(options, 'debug'):
                debug = options['debug']

            # Configure and get the logger for this order request
            EspaLogging.configure('espa.processing', order=orderid,
                                  product=product_type, debug=debug)
            logger = EspaLogging.get_logger('espa.processing')

            # If the command line option is True don't use the scene option
            if not mapper_keep_log:
                if not parameters.test_for_parameter(options, 'keep_log'):
                    options['keep_log'] = False

                mapper_keep_log = options['keep_log']

            logger.info("Processing %s:%s" % (orderid, product_type))

            # Update the status in the database
            if parameters.test_for_parameter(parms, 'xmlrpcurl'):
                if parms['xmlrpcurl'] != 'skip_xmlrpc':
                    server = xmlrpclib.ServerProxy(parms['xmlrpcurl'])
                    if server is not None:
                        status = server.update_status(product_type, orderid,
                                                      processing_location,
                                                      'processing')
                        if not status:
                            logger.warning("Failed processing xmlrpc call"
                                           " to update_status to processing")

            # Use the DEV_CACHE_HOSTNAME if present
            dev_cache_hostname = 'DEV_CACHE_HOSTNAME'
            if (dev_cache_hostname not in os.environ
                    or os.environ.get(dev_cache_hostname) is None
                    or len(os.environ.get(dev_cache_hostname)) < 1):
                # MUST USE THIS TODAY
                cache_host = utilities.get_cache_hostname()
            else:
                cache_host = os.environ.get('DEV_CACHE_HOSTNAME')

            # Use the DEV_CACHE_DIRECTORY if present
            dev_cache_directory = 'DEV_CACHE_DIRECTORY'
            if (dev_cache_directory not in os.environ
                    or os.environ.get(dev_cache_directory) is None
                    or len(os.environ.get(dev_cache_directory)) < 1):
                cache_directory = settings.ESPA_CACHE_DIRECTORY
            else:
                cache_directory = os.environ.get('DEV_CACHE_DIRECTORY')

            options['source_host'] = cache_host
            options['source_directory'] = os.path.join(cache_directory,
                                                       orderid)

            options['destination_host'] = cache_host
            options['destination_directory'] = os.path.join(cache_directory,
                                                            orderid)

            # TODO TODO TODO
            # Generate the command line that can be used with the specified
            # application
            # TODO TODO TODO

            # Build the plot command line
            cmd = ['./plot.py']
            cmd_line_options = \
                parameters.convert_to_command_line_options(parms)
            cmd.extend(cmd_line_options)
            cmd = ' '.join(cmd)
            logger.info("Processing plot with [%s]" % cmd)

            # Call the plotter with the parameters
            plotter.process(parms)

        except Exception, e:
            if len(output) > 0:
                logger.info("Output [%s]" % output)

            # First log the exception
            if hasattr(e, 'output'):
                logger.error("Output [%s]" % e.output)
            logger.exception("Exception encountered stacktrace follows")

            if server is not None:

                try:
                    status = set_scene_error(product_type, orderid,
                                             processing_location)
                    if status and not mapper_keep_log:
                        try:
                            # Cleanup the log file
                            EspaLogging. \
                                delete_logger_file('espa.processing')
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
        description="Processes a list of lpcs plot requests from stdin")
    parser.add_argument('--keep-log', action='store_true', dest='keep_log',
                        default=False, help="keep the generated log file")
    args = parser.parse_args()

    EspaLogging.configure_base_logger(filename='/tmp/espa-lpcs_mapper.log')
    # Initially set to the base logger
    logger = EspaLogging.get_logger('base')

    try:
        process(args)
    except Exception, e:
        logger.exception("Processing failed stacktrace follows")

    sys.exit(EXIT_SUCCESS)
