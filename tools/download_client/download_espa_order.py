#!/usr/bin/env python

"""
Author: David Hill
Date: 01/31/2014
Purpose: A simple python client that will download all available (completed) scenes for
         a user order(s).

Requires: Python feedparser and standard Python installation.     

Version: 1.0
"""

import feedparser
import urllib2
import argparse
import shutil
import os

class SceneFeed(object):
    """SceneFeed parses the ESPA RSS Feed for the named email address and generates
    the list of Scenes that are available"""
    
    def __init__(self, email, host="http://espa.cr.usgs.gov"):
        """Construct a SceneFeed.
        
        Keyword arguments:
        email -- Email address orders were placed with
        host  -- http url of the RSS feed host
        """
        
        self.email = email
        
        if not host.startswith('http://'):
            host = ''.join(["http://", host])
        self.host = host
        
        self.feed_url = "%s/ordering/status/%s/rss/" % (self.host, self.email)
        
        
    def get_items(self, orderid='ALL'):
        """get_items generates Scene objects for all scenes that are available to be
        downloaded.  Supply an orderid to look for a particular order, otherwise all
        orders for self.email will be returned"""
        
        #yield Scenes with download urls
        feed = feedparser.parse(self.feed_url)
                
        for entry in feed.entries:

            #description field looks like this
            #'scene_status:thestatus,orderid:theid,orderdate:thedate'
            scene_order = entry.description.split(',')[1].split(':')[1]

            #only return values if they are in the requested order            
            if orderid == "ALL" or scene_order == orderid:
                yield Scene(entry.link, scene_order)
            
                
class Scene(object):
    
    def __init__(self, srcurl, orderid):
    
        self.srcurl = srcurl
    
        self.orderid = orderid
        
        parts = self.srcurl.split("/")
     
        self.filename = parts[len(parts) - 1]
        
        self.name = self.filename.split('.tar.gz')[0]
        
                  
              
class LocalStorage(object):
    
    def __init__(self, basedir):
        self.basedir = basedir
                    
    def directory_path(self, scene):
        return ''.join([self.basedir, os.sep, scene.orderid, os.sep])
        
    def scene_path(self, scene):
        return ''.join([self.directory_path(scene), scene.filename])
    
    def tmp_scene_path(self, scene):
        return ''.join([self.directory_path(scene), scene.filename, '.part'])
    
    def is_stored(self, scene):        
        return os.path.exists(self.scene_path(scene))        
    
    def store(self, scene):
        
        if self.is_stored(scene): return
                    
        download_directory = self.directory_path(scene)
        
        #make sure we have a target to land the scenes
        if not os.path.exists(download_directory):
            os.makedirs(download_directory)
            print ("Created target_directory:%s" % download_directory)
        
        req = urllib2.urlopen(scene.srcurl)

        print ("Copying %s to %s" % (scene.name, download_directory))
        
        with open(self.tmp_scene_path(scene), 'wb') as target_handle:
            shutil.copyfileobj(req, target_handle)
        
        os.rename(self.tmp_scene_path(scene), self.scene_path(scene))



if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-e", "--email", 
                        required=True,
                        help="email address for the user that submitted the order)")
                        
    parser.add_argument("-o", "--order",
                        required=True,
                        help="which order to download (use ALL for every order)")
                        
    parser.add_argument("-d", "--target_directory",
                        required=True,
                        help="where to store the downloaded scenes")   
    
    args = parser.parse_args()
    
    storage = LocalStorage(args.target_directory)
    
    for scene in SceneFeed(args.email).get_items(args.order):
        storage.store(scene)
        
    
