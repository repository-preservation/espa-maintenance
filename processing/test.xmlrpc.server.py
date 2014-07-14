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
def get_lpcs_orders_to_process():

    results = []
    try:
        results.append('LPCS-UTM-2')
        # TODO TODO TODO - ????Do we need to return more than the order ID????
        return results
    finally:
        del results


# ============================================================================
def update_status(sceneid, orderid, module, status):
    with open("xmlrpc_server.log", "a") as fd:
        fd.write("Scene [%s] Order [%s] Received [%s] From [%s]"
                 % (sceneid, orderid, status, module))
    return True


# ============================================================================
def set_scene_error(sceneid, orderid, module, status):
    with open("xmlrpc_server.log", "a") as fd:
        fd.write("Scene [%s] Order [%s] Received [%s] From [%s]"
                 % (sceneid, orderid, status, module))
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
