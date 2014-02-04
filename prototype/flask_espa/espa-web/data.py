#!/usr/bin/env python

from pymongo import Connection

class MongoData(object):
    
    constants = {}
    constants['db.username'] = None
    constants['db.password'] = None
    constants['db.url'] = None
    constants['db.espa'] = 'espa'
    constants['collections.scenes'] = 'scenes'
    constants['collections.orders'] = 'orders'
    constants['collections.config'] = 'configurations'
    
    
    def __init__(self, username = None, password = None, url = None):
        if username is not None:
            self.constants['db.username'] = username
        if password is not None:
            self.constants['db.password'] = password
        if url is not None:
            self.constants['db.url'] = url
        
    
    def get_db(self):
        #get a connection to the espa database
        connection = Connection()
        db = connection[self.constants.get('db.espa')]
        return db
        
    def get_collection(self, name):
        #get the named database collection
        coll_name = self.constants.get(name)
        db = self.get_db()
        coll = db[str(coll_name)]
        return coll
    
    def create_scene_indices(self):
        #need to review this based on the queries that are being run
        coll = self.get_collection('collections.scenes')
        coll.create_index([('id', DESCENDING), ('created', DESCENDING), ('status', DESCENDING)])
        coll.create_index([('status', DESCENDING)])
        coll.create_index([('orderid', DESCENDING)])
            
