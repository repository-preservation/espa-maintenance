'''
Author: David V. Hill
Description: This module contains the implementation
for any functions that need to be performed by both
the user facing webpages (placing orders, getting
order status, etc) and also by the api.py
'''
import security as sec
#import validate as val
import lta as lta


def authenticate(username, password):
    return sec.authenticate(username, password)

def authorize(username, role):
    return sec.authorize(username, role)

def place_new_order():
    pass

def list_orders(email):
    '''return list of Order objects'''
    pass

def get_order_details(order_number):
    '''return an Order object'''
    pass
