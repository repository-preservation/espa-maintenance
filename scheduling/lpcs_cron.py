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
'''

import os
import sys
import time
import json
import xmlrpclib
import subprocess
from datetime import datetime

# espa-common objects and methods
from espa_constants import *

# imports from espa/espa_common
try:
    from espa_logging import EspaLogging
except:
    from espa_common.espa_logging import EspaLogging

try:
    import settings
except:
    from espa_common import settings

# local objects and methods
import util


# ============================================================================
def run_orders():
    '''
    Description:
      Queries the xmlrpc service to see if there are any scenes that need to
      be processed.  If there are, this method builds and executes a plot job
      and updates the status in the database through the xmlrpc service.
    '''

    # Configure and get the logger for this task
    logger_name = 'espa.cron.%s' % args.priority.lower()
    EspaLogging.configure(logger_name)
    logger = EspaLogging.get_logger(logger_name)

    rpcurl = os.environ.get('ESPA_XMLRPC')
    server = None

    # Create a server object if the rpcurl seems valid
    if (rpcurl is not None and rpcurl.startswith('http://')
            and len(rpcurl) > 7):

        server = xmlrpclib.ServerProxy(rpcurl)
    else:
        logger.info("Missing or invalid environment variable ESPA_XMLRPC")

    # Use the DEV_CACHE_HOSTNAME if present
    dev_cache_hostname = 'DEV_CACHE_HOSTNAME'
    if (dev_cache_hostname not in os.environ
            or os.environ.get(dev_cache_hostname) is None
            or len(os.environ.get(dev_cache_hostname)) < 1):
        cache_host = util.get_cache_hostname()  # MUST USE THIS TODAY
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
                    output = util.execute_cmd(cmd)
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

    # Check required variables that this script should fail on if they are not
    # defined
    required_vars = ('ESPA_XMLRPC', "ESPA_WORK_DIR", "ANC_PATH", "PATH",
                     "HOME")
    for env_var in required_vars:
        if (env_var not in os.environ or os.environ.get(env_var) is None
                or len(os.environ.get(env_var)) < 1):

            print("$%s is not defined... exiting" % env_var)
            sys.exit(EXIT_FAILURE)

    run_orders()

    sys.exit(EXIT_SUCCESS)
