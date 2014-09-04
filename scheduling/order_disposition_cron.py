#! /usr/bin/env python

'''
    FILE: order_disposition_cron.py

    PURPOSE: Processes order finalization and email generation for accepted
             orders.

    PROJECT: Land Satellites Data Systems Science Research and Development
             (LSRD) at the USGS EROS

    LICENSE: NASA Open Source Agreement 1.3

    HISTORY:

    Date              Programmer               Reason
    ----------------  ------------------------ --------------------------------
    ??/??/????        David V. Hill            Initial implementation.
    Aug/2014          Ron Dilley               Made operational for cron
'''

import os
import sys
import xmlrpclib
import traceback

# espa-common objects and methods
from espa_constants import EXIT_FAILURE
from espa_constants import EXIT_SUCCESS

# imports from espa/espa_common
try:
    from espa_logging import EspaLogging
except:
    from espa_common.espa_logging import EspaLogging


# ============================================================================
def determine_order_disposition():
    '''
    Description:
      Interact with the web service to accomplish order finalization tasks
      along with sending the initial emails out to the users after their
      order has been accepted.
    '''

    # Configure and get the logger for this task
    logger_name = 'espa.cron'
    EspaLogging.configure(logger_name)
    logger = EspaLogging.get_logger(logger_name)

    rpcurl = os.environ.get('ESPA_XMLRPC')
    server = None

    # Create a server object if the rpcurl seems valid
    if (rpcurl is not None and rpcurl.startswith('http://')
            and len(rpcurl) > 7):

        server = xmlrpclib.ServerProxy(rpcurl)
    else:
        raise Exception("Missing or invalid environment variable ESPA_XMLRPC")

    # Verify xmlrpc server
    if server is None:
        msg = "xmlrpc server was None... exiting"
        raise Exception(msg)

    try:
        if not server.handle_orders():
            msg = "server.handle_orders() was not successful"
            raise Exception(msg)

    except xmlrpclib.ProtocolError, e:
        logger.exception("A protocol error occurred")

    except Exception, e:
        logger.exception("An error occurred finalizing orders")

    finally:
        server = None
# END - determine_order_disposition


# ============================================================================
if __name__ == '__main__':
    '''
    Description:
      Execute the order disposition determination routine.
    '''

    # Check required variables that this script should fail on if they are not
    # defined
    required_vars = ['ESPA_XMLRPC']
    for env_var in required_vars:
        if (env_var not in os.environ or os.environ.get(env_var) is None
                or len(os.environ.get(env_var)) < 1):

            print("$%s is not defined... exiting" % env_var)
            sys.exit(EXIT_FAILURE)

    determine_order_disposition()

    sys.exit(EXIT_SUCCESS)
