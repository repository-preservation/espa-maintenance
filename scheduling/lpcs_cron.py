#! /usr/bin/env python

'''
    FILE: lpcs_cron.py

    PURPOSE: Queries the xmlrpc service to find orders that need to be
             processed and builds/executes a plot.py job to process them.

    PROJECT: Land Satellites Data Systems Science Research and Development
             (LSRD) at the USGS EROS

    LICENSE: NASA Open Source Agreement 1.3

    HISTORY:

    Date              Programmer               Reason
    ----------------  ------------------------ -------------------------------
    Feb/2014          Ron Dilley               Initial implementation
    Sept/2014         Ron Dilley               Updated to use espa_common and
                                               our python logging setup
'''

import os
import sys
import json
import xmlrpclib

# espa-common objects and methods
from espa_constants import EXIT_FAILURE
from espa_constants import EXIT_SUCCESS

# imports from espa/espa_common
from espa_common import settings, utilities
from espa_common.espa_logging import EspaLogging

LOGGER_NAME = 'espa.cron.lpcs'


# ============================================================================
def run_orders():
    '''
    Description:
      Queries the xmlrpc service to see if there are any scenes that need to
      be processed.  If there are, this method builds and executes a plot job
      and updates the status in the database through the xmlrpc service.
    '''

    # Get the logger for this task
    logger = EspaLogging.get_logger(LOGGER_NAME)

    rpcurl = os.environ.get('ESPA_XMLRPC')
    server = None

    # Create a server object if the rpcurl seems valid
    if (rpcurl is not None and rpcurl.startswith('http://')
            and len(rpcurl) > 7):

        server = xmlrpclib.ServerProxy(rpcurl)
    else:
        logger.warning("Missing or invalid environment variable ESPA_XMLRPC")

    # Use the DEV_CACHE_HOSTNAME if present
    dev_cache_hostname = 'DEV_CACHE_HOSTNAME'
    if (dev_cache_hostname not in os.environ
            or os.environ.get(dev_cache_hostname) is None
            or len(os.environ.get(dev_cache_hostname)) < 1):
        cache_host = utilities.get_cache_hostname()  # MUST USE THIS TODAY
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

    try:
        logger.info("Checking for orders to process...")
        orders = server.get_lpcs_orders_to_process()

        if orders:
            logger.info("Found orders to process:")

            # Process the orders
            for order in orders:
                logger.info("Processing order [%s]" % order)

                # Build the order directory
                order_directory = os.path.join(cache_directory, order)

                # Build the plot command line
                cmd = ' '.join(['./plot.py', '--source_host', cache_host,
                                '--order_directory', order_directory])
                output = ''
                try:
                    output = utilities.execute_cmd(cmd)
                except Exception, e:
                    # TODO TODO TODO - Needs web side implementation
                    server.update_order_status(order, 'LPCS cron driver',
                                               'FAIL')

                    msg = ' '.join(["Error during execution of plot.py:",
                                    str(e)])
                    raise Exception(msg)
                finally:
                    if len(output) > 0:
                        logger.info(output)

                # TODO TODO TODO - Needs web side implementation
                server.update_order_status(order, 'LPCS cron driver', 'SUCC')

    except xmlrpclib.ProtocolError, e:
        logger.exception("A protocol error occurred")

    except Exception, e:
        logger.exception("Error Processing Plots")

    finally:
        server = None

# END - run_orders


# ============================================================================
if __name__ == '__main__':
    '''
    Description:
      Execute the core processing routine.
    '''

    # Configure and get the logger for this task
    EspaLogging.configure(LOGGER_NAME)
    logger = EspaLogging.get_logger(LOGGER_NAME)

    # Check required variables that this script should fail on if they are not
    # defined
    required_vars = ('ESPA_XMLRPC', "ESPA_WORK_DIR", "ANC_PATH", "PATH",
                     "HOME")
    for env_var in required_vars:
        if (env_var not in os.environ or os.environ.get(env_var) is None
                or len(os.environ.get(env_var)) < 1):

            logger.critical("$%s is not defined... exiting" % env_var)
            sys.exit(EXIT_FAILURE)

    try:
        run_orders()
    except Exception, e:
        logger.exception("Processing failed")
        sys.exit(EXIT_FAILURE)

    sys.exit(EXIT_SUCCESS)
