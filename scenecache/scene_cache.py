#!/usr/bin/env python

#--!/usr/local/python-2.7.5/bin/python
#Had to point direct to this distro because there were no links set and 
#the default Python instance was 2.4

"""
License:
  "NASA Open Source Agreement 1.3"

Description:
  Provides a way to determine if scenes exist on the online cache disk in bulk.
"""

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import os
import SocketServer
import unittest
import shutil
import re
import daemon
import settings


class ForkingXMLRPCServer(SocketServer.ForkingMixIn, SimpleXMLRPCServer):
    """A Forking XMLRPC Server built using ForkingMixIn class"""
    pass


class ThreadingXMLRPCServer(SocketServer.ThreadingMixIn, SimpleXMLRPCServer):
    """A Threading XMLRPC Server built using ThreadingMixIn class"""
    pass



class RequestHandler(SimpleXMLRPCRequestHandler):
    """Class to handle all the XMLRPC requests"""
    rpc_paths = ('/RPC2')
    
    server_version = settings.server_name
    
    #def do_POST(self):
    def do_POST(self):
        #send unauthorized if not in list   
        if self.client_address[0] not in settings.authorized_clients:
            self.send_response(401)
        else:
            #super(RequestHandler, self).do_POST(self)     
            #cannot use super() to make calls in python 2.7.5 with the 
            #SimpleXMLRPCRequestHandler, as its ultimate base class
            #BaseRequestHandler in SocketServer.py is not a new-style
            #class (it does not inherit from object).  Resort to manual
            #call to do_POST() instead.
            SimpleXMLRPCRequestHandler.do_POST(self)
            

       


class Utils(object):
    """Utility class to hold methods that should never be exposed through XMLRPC.  
    There is a similar class in the espa proper codebase but cannot be used because this
    code is built to run standalone on the online cache servers, which will not get 
    a full up espa deployment"""
    
                
    def strip_zeros(self, value):
        """Removes all leading zeros from a string"""
        if value.startswith('0'):
            value = value[1:len(value)]
            return self.strip_zeros(value)
        return value

#        while value.startswith('0'):
#            value = value[1:len(value)]
#            return value


    def is_valid_scene(self, scene_name):
        """Determines whether or not the value is a properly formatted scene id"""
        scene_name = scene_name.upper()    
        regex = "L(E7|T4|T5)\d{3}\d{3}\d{4}\d{3}[a-zA-Z]{3}\d{2}"
        if re.match(regex, scene_name):
            return True
        else:
            return False
        
    
    def get_path(self, scene_name):
        """Returns the path of a given scene"""
        return self.strip_zeros(scene_name[3:6])


    def get_row(self, scene_name):
        """Returns the row of a given scene"""
        return self.strip_zeros(scene_name[6:9])


    def get_year(self, scene_name):
        """Returns the year of a given scene"""
        return scene_name[9:13]


    def get_sensor(self, scene_name):
        """Returns the sensor of a given scene"""
        if scene_name[0:3] =='LT5' or scene_name[0:3] == 'LT4':
            return 'tm'
        elif scene_name[0:3] == 'LE7':
            return 'etm'

    
    def get_path_on_disk(self, scene, basedir, nlaps=False):
        """returns a tuple containing the path on disk where the file should
        reside and the filename itself.  Does not indicate in any way if it is actually
        present at the location."""
        
        if nlaps:
            sensor = 'nlaps/tm'
        else:
            sensor = self.get_sensor(scene)    
                
        src_dir = ("%s/%s/%s/%s/%s") % (basedir,
                                        sensor, 
                                        self.get_path(scene), 
                                        self.get_row(scene), 
                                        self.get_year(scene))

        if not scene.endswith('.tar.gz'):
            x = "%s.tar.gz" % scene
            
        return (src_dir, x)
    

class SceneCache(object):
    """The actual logic of the SceneCache. For both is_nlaps and scenes_exist, this
    class (over either XMLRPC or directly) allows callers to supply a list of scenes
    and receive a list in response.  The returned list contains all the supplied scenes
    that were found on disk."""
    
    def __init__(self, basedir=settings.base_dir):
        self.basedir = basedir
     
    def __scenes_exist(self, scenelist, nlaps=False):
        """ internal utility method that should not be called directly via xmlrpc
        scenelist = python list of scene ids
        nlaps = whether the scene list is nlaps or not
        Method will return a list of any scenes found on cache.  If empty list then none
        were found
        """        

        results = list()

        try:
            scenelist = [s for s in scenelist if Utils().is_valid_scene(s)]
            results = []
            
            for s in scenelist:
                s = s.split('.tar.gz')[0]
                path_tuple = Utils().get_path_on_disk(s, 
                                                      basedir=self.basedir,
                                                      nlaps=nlaps)
                real_path = os.path.join(path_tuple[0], path_tuple[1])


                if os.path.exists(real_path):
                    results.append(s)
        finally:
            del scenelist    
        return results
    

    def is_nlaps(self, scenelist):
        return self.__scenes_exist(scenelist, True)
    
    def scenes_exist(self, scenelist):
        return self.__scenes_exist(scenelist)
       


class TestSceneCache(unittest.TestCase):
    
    #base diretory to use for testing
    #basedir='/tmp/test_scene_cache'
    basedir='/data/standard_l1t'

    #baloney scenenames that we'll create on disk
    nlaps_scenes = ['LE70100202003111EDC00', 'LT50100202003111EDC01']    
    good_scenes = ['LT50330221999111EDC00', 'LE70010022000234EDC01']    
    
    #the scene cache to test
    cache = None    
    
    #the utils object for get_path_on_disk
    utils = None
    
    def buildcachefile(self, file_with_path_tuple):

        if not os.path.exists(file_with_path_tuple[0]):        
            os.makedirs(file_with_path_tuple[0])
            
        full_path = os.path.join(file_with_path_tuple[0],file_with_path_tuple[1])        

        with open(full_path, 'wb+') as h:
            h.write('temp file')
                     
           
    def setUp(self):
        #clean out directory if it exists
        shutil.rmtree(self.basedir, ignore_errors=True)        
        
        #build temp files to look like scenes
        self.cache = SceneCache(basedir=self.basedir)
        self.utils = Utils()
        
        for s in self.nlaps_scenes:
            fp = self.utils.get_path_on_disk(s, self.basedir, True)
            self.buildcachefile(fp)
        
        for s in self.good_scenes:
            fp = self.utils.get_path_on_disk(s, self.basedir, False)
            self.buildcachefile(fp)
        
        self.cache = SceneCache(basedir=self.basedir)
        self.utils = Utils()
                                
    def tearDown(self):
        #remove temp files    
        shutil.rmtree(self.basedir, ignore_errors=True)
        self.cache = None
        self.good_scenes = None
        self.nlaps_scenes = None
        self.basedir = None
    
    def test_scenes_exist_malformed_list(self):
        """Expects an empty list to be returned in the event a malformed list
        is supplied"""
        results = self.cache.scenes_exist(['this_isnt_a_sceneid'])        
        self.assertEqual(results, [])
        
    def test_scenes_exist_good_list(self):
        """Expects that the returned scene list matches the supplied list
        since the supplied list is on disk"""
        results = self.cache.scenes_exist(self.good_scenes)
        self.assertEqual(len(results),len(self.good_scenes))
        self.assertEqual(set(results),set(self.good_scenes))
    
    def test_scenes_exist_empty_list(self):
        """Expects an empty list to be returned if an empty list is supplied"""
        results = self.cache.scenes_exist([])        
        self.assertEqual(results, [])
    
    def test_scenes_exist_expect_missing(self):
        """Expects that the scenes will not be found on cache"""
        results = self.cache.scenes_exist(self.nlaps_scenes)
        self.assertEqual(results, [])
        
    def test_is_nlaps_malformed_list(self):
        """Expects an empty list to be returned in the event a malformed list
        is supplied"""
        results = self.cache.is_nlaps(['this_isnt_a_sceneid'])        
        self.assertEqual(results, [])
        
    def test_is_nlaps_good_list(self):
        """Expects that the returned scene list matches the supplied list
        since the supplied list is on disk"""
        results = self.cache.is_nlaps(self.nlaps_scenes)
        self.assertEqual(len(results),len(self.nlaps_scenes))
        self.assertEqual(set(results),set(self.nlaps_scenes))
    
    def test_is_nlaps_empty_list(self):
        """Expects an empty list to be returned if an empty list is supplied"""
        results = self.cache.is_nlaps([])        
        self.assertEqual(results, [])
    
    def test_is_nlaps_expect_missing(self):
        """Expects that the scenes will not be found on cache"""
        results = self.cache.is_nlaps(self.good_scenes)
        self.assertEqual(results, [])
            

def run():
    #change this port to be whatever is necessary...  Treat this 
    #file itself as a config file unless you wind up with more values
    #that need to be configured.
    
    url = settings.ip_address  
    port = settings.port
    multi = settings.multiprocess_model

    if multi == 'thread':
        server = ThreadingXMLRPCServer((url, port), requestHandler = RequestHandler)
    elif multi == 'fork':
        server = ForkingXMLRPCServer((url, port), requestHandler = RequestHandler)        
    elif multi == 'single':
        server = SimpleXMLRPCServer((url, port), requestHandler = RequestHandler)
    else:
        server = ThreadingXMLRPCServer((url, port), requestHandler=RequestHandler)
        
    server.register_introspection_functions()
    server.register_instance(SceneCache())
    server.serve_forever()
    
if __name__ == '__main__':

    print settings.startup_message
    
    #run the server as a daemon    
    if settings.run_as_daemon:
        with daemon.DaemonContext():
            run()
    else:
        run()
    
    
    
    

