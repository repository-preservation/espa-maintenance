'''
ESPA business logic.

Do NOT import anything into this module from Django
Do NOT touch a database, email server, file on the filesystem, or any 
other implementation specific entity here.
Do NOT do anything that refernces a transport, such as HTTP.

This module is pure logic for what the system does, 
not how its implemented.  This will provide a consistent API that will 
allow different transports and persistence mechanisms to be used.  

'''

import policies
import metrics
import validation
import logging

class User(object):

    def has_role(self, username):
        pass
    
    def view_details(self, username):
        pass
    
    def authenticate(self, username, password):
        pass
    
    def exists(self, username):
        pass


class Order(object):
    
    # raises LimitViolation, PolicyViolation, ValidationError, SubmissionError
    def place_order(self, username, input_products, product_options, opts=[]):
        
        if not 'skip_metrics' in opts:
            # captures and logs order stats
            metrics.collect(username, input_products, product_options)
        
        if not 'skip_validation' in opts:
            # raises ValidationError 
            validation.validate(input_products, product_options)
        
        if not 'skip_policy_check' in opts:
            # raises PolicyViolation
            policies.check_policies(username, input_products, product_options)
        
        if not 'skip_order_submission' in opts:
            pass
        
    def view_orders(self, username):
        pass
    
    def view_order(self, orderid):
        pass
    
    def order_options(self, orderid):
        pass
    
    
    
    
    