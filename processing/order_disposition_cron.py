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

from espa_logging import log


# ============================================================================
def determine_order_disposition():
    '''
    Description:
      Interact with the web service to accomplish order finalization tasks
      along with sending the initial emails out to the users after their
      order has been accepted.
    '''

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
        log("A protocol error occurred: %s" % str(e))
        tb = traceback.format_exc()
        log(tb)

    except Exception, e:
        log("An error occurred finalizing orders: %s" % str(e))
        tb = traceback.format_exc()
        log(tb)

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

            log("$%s is not defined... exiting" % env_var)
            sys.exit(EXIT_FAILURE)

    determine_order_disposition()

    sys.exit(EXIT_SUCCESS)
