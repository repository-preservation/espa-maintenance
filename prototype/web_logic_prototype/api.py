'''
ESPA business logic api.

Do NOT import anything into this module from Django
Do NOT touch a database, email server, file on the filesystem, or any 
other implementation specific entity here.
Do NOT do anything that references a transport, such as HTTP.

This module is pure logic for what the system does, 
not how its implemented.  This will provide a consistent API that will 
allow different transports and persistence mechanisms to be used.  

'''

import logic
import logging

class Users(object):
    ''' Users class documentation '''
    
    @staticmethod    
    def details(user_name):
        ''' details documentation '''
        
        #raises UserDoesNotExist
        return logic.user.details(user_name)
    
    @staticmethod
    def authenticate(user_name, password):
        ''' authenticate documentation '''
        
        #raises UserDoesNotExist
        return logic.user.authenticate(user_name, password)

    @staticmethod
    def exists(user_name):
        ''' exists documentation '''
        return logic.user.exists(user_name)
    
    @staticmethod            
    def add_user(user_name, real_name=None, contact_id=None):
        ''' add_user documentation '''
        
        #raises UserExists
        return logic.user.add_user(user_name, real_name, contact_id)
    
    @staticmethod
    def delete_user(user_name):
        ''' delete_user documentation '''
        
        #raises UserDoesNotExist
        return logic.user.delete_user(user_name)


class Roles(object):
    ''' Roles documentation '''
    
    @staticmethod
    def add_user_to_role(user_name, role_name):
        ''' add_user_to_role documentation '''
        
        #raises UserDoesNotExist, RoleDoesNotExist
        return logic.role.add_user_to_role(user_name, role_name)
    
    @staticmethod
    def remove_user_from_role(user_name, role_name):
        ''' remove_user_from_role documentation '''
        
        #raises UserDoesNotExist, RoleDoesNotExist
        return logic.role.remove_user_from_role(user_name, role_name)

    @staticmethod    
    def list_all():
        ''' list_all documentation '''
        
        return logic.role.list_all()
    
    @staticmethod
    def user_has_role(user_name, role_name):
        ''' has_role documentation '''
        
        #raises UserDoesNotExist, RoleDoesNotExist
        return logic.role.user_has_role(user_name, role_name)    
    
    @staticmethod
    def users_in_role(role_name):
        ''' users_in_role documentation '''
        
        #raises RoleDoesNotExist
        return logic.role.users_in_role(role_name)

    @staticmethod    
    def add_role(role_name):
        ''' add_role documentation '''
        
        #raises RoleExists
        logic.role.add_role(role_name, description=None)
    
    @staticmethod
    def delete_role(role_name):
        ''' delete_role documentation '''
        #raises RoleDoesNotExist
        return logic.role.delete_role(role_name)
    
    
class Orders(object):
    ''' Orders documentation '''
    
    # raises LimitViolation, PolicyViolation, ValidationError, SubmissionError,
    #        DuplicateOrder
    @staticmethod
    def place(user_name, input_products, product_options, opts=[]):
        ''' place documentation '''
        
        if not 'skip_metrics' in opts:
            # captures and logs order stats
            logic.metric.collect(user_name, input_products, product_options)
        
        if not 'skip_validation' in opts:
            # raises ValidationError 
            logic.validation.validate(input_products, product_options)
        
        if not 'skip_policy_check' in opts:
            # raises PolicyViolation
            logic.policy.check_policies(user_name,
                                        input_products,
                                        product_options)
                                                   
        if not 'skip_order_submission' in opts:
            
            return logic.order.place_order(user_name,
                                           input_products,
                                           product_options)
    
    @staticmethod    
    def list_all(user_name, status=None, order_type=None):
        ''' list_all documentation '''
        
        #raises UserDoesNotExist
        return logic.order.list_orders(user_name, status, order_type)

    @staticmethod    
    def view(order_id):
        ''' view documentation '''
        
        #raises OrderDoesNotExist
        return logic.order.view_order(order_id)
    
    @staticmethod
    def options(order_id):
        ''' options documentation '''
        
        #raises OrderDoesNotExist
        return logic.order.order_options(order_id)
    

class Products(object):
    ''' Products documentation '''
    
    @staticmethod
    def list_all():
        ''' list_all documentation '''
        
        return logic.product.list_all()
    
    @staticmethod    
    def description(product_name):
        ''' description documentation '''
        #Raises ProductDoesNotExist
        return logic.product.description(product_name)
        
 
class Reports(object):
    ''' Reports documentation '''

    @staticmethod    
    def list_categories():
        ''' list_categories documentation '''
        return logic.report.list_categories()
    
    @staticmethod
    def list_reports(category='all'):
        ''' list_reports documentation '''
        return logic.report.list_reports(category)

    @staticmethod    
    def generate(report_name):
        ''' generate documentation '''
        return logic.report.generate(report_name)


class Documentation(object):
    ''' Documentation documentation '''
    
    @staticmethod
    def list_categories():
        ''' list_categories documentation '''
        return logic.documentation.list_categories()

    @staticmethod    
    def list_documentation(category='all'):
        ''' list_documentation documentation '''
        return logic.documentation.list_documentation(category)
    
    @staticmethod
    def get_document(document_name):
        ''' get_document documentation '''
        return logic.documentation.get_document(document_name)
    

class Metrics(object):
    ''' Metrics documentation '''
    
    @staticmethod
    def list_categories():
        ''' list_categories documentation '''
        return logic.metric.list_categories()
    
    @staticmethod
    def list_metrics(category='all'):
        ''' list_metrics documentation '''
        return logic.metric.list_metrics(category)

    @staticmethod    
    def get_metric(metric_name):
        ''' get_metric documentation '''
        return logic.metric.get(metric_name)
    
    @staticmethod
    def collect(metric_name, metric_value):
        ''' collect_metric documentation '''
        return logic.metric.collect_metric(metric_name, metric_value)
    

class Cache(object):
    ''' Cache documentation '''
    
    @staticmethod
    def list_keys():
        ''' list_keys documentation '''
        
        return logic.cache.list_keys()

    @staticmethod    
    def get(key):
        ''' get documentation '''
        
        #Raises KeyDoesNotExist
        return logic.cache.get(key)
    
    @staticmethod
    def put(key, value, ttl):
        ''' put documentation '''
        
        #Raises KeyDoesNotExist
        return logic.cache.put(key, value, ttl)
    
    @staticmethod
    def invalidate(key):
        ''' invalidate documentation '''
        #Raises KeyDoesNotExist
        return logic.cache.invalidate(key)
    
