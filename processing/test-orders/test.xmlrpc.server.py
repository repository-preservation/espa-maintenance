#! /usr/bin/env python

import os
import sys
import signal
import SocketServer
from argparse import ArgumentParser
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler


class LPCS_XMLRPCServer(SimpleXMLRPCServer):
    '''
    Description:
      TODO TODO TODO
    '''

    done = False

    def register_signal(self, signum):
        signal.signal(signum, self.signal_handler)

    def signal_handler(self, signum, frame):
        print "Recieved signal", signum
        self.shutdown()

    def shutdown(self):
        self.done = True
        return 0

    def serve_forever(self):
        while not self.done:
            try:
                self.handle_request()
            except Exception, e:
                pass
# END - LPCS_XMLRPCServer


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RDD')


# ============================================================================
# Called from the crons
def get_configuration(config_item):
    if config_item == 'landsatds.username':
        return 'dev_username'
    elif config_item == 'landsatds.password':
        return 'dev_password'
    elif config_item == 'landsatds.host':
        return 'localhost'
    elif config_item == 'ondemand_enabled':
        return 'true'

    return ''


# ============================================================================
# Called from the crons
def get_scenes_to_process(limit, user, priority, product_types):
    return True


# ============================================================================
# Called from the crons
def queue_products(product_list, processing_location, job_name):
    return True


# ============================================================================
# Called from the mappers
def update_status(sceneid, orderid, module, status):
    with open("xmlrpc_server.log", "a") as fd:
        fd.write("Scene [%s] Order [%s] Received [%s] From [%s]"
                 % (sceneid, orderid, status, module))
    return True


# ============================================================================
# Called from the mappers
def set_scene_error(product_type,
                    orderid,
                    processing_location,
                    logged_contents):
    with open("xmlrpc_server.log", "a") as fd:
        fd.write("Product Type [%s] Order ID [%s]"
                 " Processing Location [%s] Logged Contents [%s]"
                 % (product_type, orderid, processing_location,
                    logged_contents))
    return True


# ============================================================================
def build_argument_parser():
    '''
    Description:
      Build the command line argument parser
    '''

    # Create a command line argument parser
    description = "A test XMLRPC server"
    parser = ArgumentParser(description=description)

    parser.add_argument('--hostname',
                        action='store', dest='hostname', default='localhost',
                        help="specify the hostname")

    parser.add_argument('--port',
                        action='store', dest='port', default=8100,
                        help="specify the port to listen on")

    return parser
# END - build_argument_parser


# ============================================================================
if __name__ == '__main__':

    # Build the command line argument parser
    parser = build_argument_parser()

    # Parse the command line arguments
    args = parser.parse_args()

    print "Starting LPCS_XMLRPCServer on PID [%d]" % os.getpid()

    server = LPCS_XMLRPCServer((args.hostname, int(args.port)),
                               requestHandler=RequestHandler)

    # Add this in to allow shutdown from the service
    # server.register_function(server.shutdown)

    server.register_function(get_lpcs_orders_to_process,
                             'get_lpcs_orders_to_process')
    server.register_function(update_status, 'update_status')
    server.register_function(set_scene_error, 'set_scene_error')

    server.register_introspection_functions()

    server.register_signal(signal.SIGHUP)
    server.register_signal(signal.SIGINT)

    server.serve_forever()

    print "Stopped LPCS_XMLRPCServer"
