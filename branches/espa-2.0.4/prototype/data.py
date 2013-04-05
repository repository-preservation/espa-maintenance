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
        coll = db[coll_name]
        return coll
    
    def create_scene_indices(self):
        #need to review this based on the queries that are being run
        coll = self.get_collection('collections.scenes')
        coll.create_index([('id', DESCENDING), ('created', DESCENDING), ('status', DESCENDING)])
        coll.create_index([('status', DESCENDING)])
        coll.create_index([('orderid', DESCENDING)])
            
    def create_order_indicies(self):
        #need to review this based on the queries that are being run
        coll = self.get_collection('collections.orders')
        coll.create_index([('email', DESCENDING), ('status', DESCENDING)])
    
    def create_config_indicies(self):
        #need to review this based on the queries that are being run
        coll = self.get_collection('collections.config')
        coll.create_index([('key', DESCENDING)])
            
    ############################################################################
    #Orders
    ############################################################################
    def create_order_num(self, email):
        #utility to create a new order number        
        pass
    
    def order_exists(self, orderid):
        #see if a given order exists
        coll = None
        try:
            coll = self.get_collection('collections.orders')
        except:
            pass
        finally:
            coll = None
        
    def delete_order(self, orderid):
        #delete the order and all scenes tied to the order
        coll = None
        try:
            coll = self.get_collection('collections.orders')
        except:
            pass
        finally:
            coll = None
            
    def update_order(self, orderid, values):
        #update the order with the supplied values
        coll = None
        try:
            coll = self.get_collection('collections.orders')
        except:
            pass
        finally:
            coll = None
    
    def update_orders(self, values):
        #updates all orders with a set of values
        coll = None
        try:
            coll = self.get_collection('collections.orders')
        except:
            pass
        finally:
            coll = None
                    
    def new_order(self, email):
        #create a new order
        coll = None
        try:
            coll = self.get_collection('collections.orders')
        except:
            pass
        finally:
            coll = None
        
    def get_orders_for_email(self, email):
        #returns all orders for a given email
        coll = None
        try:
            coll = self.get_collection('collections.orders')
        except:
            pass
        finally:
            coll = None
    
    ############################################################################
    #Scenes
    ############################################################################
    def new_scene(self,email, sceneid):
        #create a new scene tied to an order
        order_collect = None
        scene_collect = None
        try:
            order_collect = self.get_collection('collections.orders')
            scene_collect = self.get_collection('collections.scenes')
        except:
            pass
        finally:
            order_collect = None
            scene_collect = None
        
    def scene_exists(self, orderid, sceneid):
        #see if a scene exists in a given order
        order_collect = None
        scene_collect = None
        try:
            order_collect = self.get_collection('collections.orders')
            scene_collect = self.get_collection('collections.scenes')
        except:
            pass
        finally:
            order_collect = None
            scene_collect = None
            
    def delete_scene(self, orderid, sceneid):
        #delete a scene for a given order
        order_collect = None
        scene_collect = None
        try:
            order_collect = self.get_collection('collections.orders')
            scene_collect = self.get_collection('collections.scenes')
        except:
            pass
        finally:
            order_collect = None
            scene_collect = None
            
    def update_scene(self,orderid, sceneid, values):
        #update a scene with the given values
        order_collect = None
        scene_collect = None
        try:
            order_collect = self.get_collection('collections.orders')
            scene_collect = self.get_collection('collections.scenes')
        except:
            pass
        finally:
            order_collect = None
            scene_collect = None
               
    def update_scenes(self, orderid, values):
        #updates all scenes for an order with a set of values
        order_collect = None
        scene_collect = None
        try:
            order_collect = self.get_collection('collections.orders')
            scene_collect = self.get_collection('collections.scenes')
        except:
            pass
        finally:
            order_collect = None
            scene_collect = None
        
    def get_scenes_for_order(self, orderid):
        #returns all scenes for a given order
        order_collect = None
        scene_collect = None
        try:
            order_collect = self.get_collection('collections.orders')
            scene_collect = self.get_collection('collections.scenes')
        except:
            pass
        finally:
            order_collect = None
            scene_collect = None
            
    def get_scenes_by_status(self, status):
        #returns all scenes for a given status
        order_collect = None
        scene_collect = None
        try:
            order_collect = self.get_collection('collections.orders')
            scene_collect = self.get_collection('collections.scenes')
        except:
            pass
        finally:
            order_collect = None
            scene_collect = None
            
    def update_scenes_to_status(self, fromstatus, tostatus):
        #updates all scenes with a given status to a new status
        scene_collect = None
        try:
            scene_collect = self.get_collection('collections.scenes')
        except:
            pass
        finally:
            order_collect = None
            scene_collect = None
            
    def update_scene_status_for_order(self, orderid, tostatus):
        #updates all the scenes in an order to a new status
        #returns all scenes for a given order
        order_collect = None
        scene_collect = None
        try:
            order_collect = self.get_collection('collections.orders')
            scene_collect = self.get_collection('collections.scenes')
        except:
            pass
        finally:
            order_collect = None
            scene_collect = None
            
        
            
    
    ############################################################################
    #Configuration
    ############################################################################
    def get_config_item(self, key):
        #retrieves a configuration item if it exists
        config = self.get_collection('collections.config')        
        
    def set_config_item(self, key, value, overwrite=False):
        #sets a new configuration item
        config = self.get_collection('collections.config')        
        
    def delete_config_item(self, key):
        #deletes a configuration item
        config = self.get_collection('collections.config')        
        
        
        