import os
import sys
import xmlrpclib
# espa-common objects and methods
from espa_constants import EXIT_FAILURE
from espa_constants import EXIT_SUCCESS
import traceback

from espa_logging import log

def finalize_orders():

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
        result = server.finalize_orders()
        if not result:
            msg = "server.finalize_orders() result was not successful"
            raise Exception(msg)
            
    except Exception, e:
        log("An error occurred finalizing orders: %s" % str(e))
        tb = traceback.format_exc()
        log(tb)
        sys.exit(EXIT_FAILURE)

    sys.exit(EXIT_SUCCESS)        
        
    