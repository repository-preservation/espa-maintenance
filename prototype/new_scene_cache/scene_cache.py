#!/usr/local/python-2.7.5/bin/python

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import os
import util
import SocketServer


class ForkingXMLRPCServer(SocketServer.ForkingMixIn, SimpleXMLRPCServer):
    pass

class ThreadingXMLRPCServer(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):
    pass

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2')


def scenes_exist(scenelist):

    results = []
    try:
        for x in scenelist:
            source_directory = ("/data/standard_l1t/%s/%s/%s/%s") % (util.getSensor(x), 
                                                                 util.getPath(x), 
                                                                 util.getRow(x), 
                                                                 util.getYear(x))
            if not x.endswith('.tar.gz'):
                x = "%s.tar.gz" % x

            target = "%s/%s" % (source_directory, x)
            #print ("Looking for %s" % target)
            if os.path.exists(target):
                results.append(x.split('.tar.gz')[0])
        return results
    finally:
        del results
        del scenelist
        

print "Starting AsyncXMLRPCServer"

#server = SimpleXMLRPCServer(("edclxs140.cr.usgs.gov", 50000), requestHandler=RequestHandler)
server = ThreadingXMLRPCServer(("edclxs140.cr.usgs.gov", 50000), requestHandler=RequestHandler)
server.register_introspection_functions()
server.register_function(scenes_exist, 'scenes_exist')
server.serve_forever()

