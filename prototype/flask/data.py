from pymongo import Connection

class MongoData(object):
    
    config = {}
    config['db.username'] = None
    config['db.password'] = None
    config['db.url'] = None
    config['db.espa'] = 'espa'
    config['collections.scenes'] = 'scenes'
    config['collections.orders'] = 'orders'
    config['collections.config'] = 'configurations'
    config['debug'] = False
    
    
    def __init__(self, username = None, password = None, url = None, database_name = None, debug=False):
        if username:
            self.config['db.username'] = username
        if password:
            self.config['db.password'] = password
        if url:
            self.config['db.url'] = url
        if database_name:
            self.config['db.espa'] = database_name
        
    
    def get_db(self):
        #get a connection to the espa database
        connection = Connection()
        db = connection[self.config.get('db.espa')]
        return db
        
    def get_collection(self, name):
        #get the named database collection
        coll_name = self.config.get(name)
        db = self.get_db()
        coll = db[str(coll_name)]
        return coll
    
    def create_scene_indices(self):
        #need to review this based on the queries that are being run
        coll = self.get_collection('collections.scenes')
        coll.create_index([('id', DESCENDING), ('created', DESCENDING), ('status', DESCENDING)])
        coll.create_index([('status', DESCENDING)])
        coll.create_index([('orderid', DESCENDING)])
            
