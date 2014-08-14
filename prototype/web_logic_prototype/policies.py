'''
Constraint checks for orders.  

Simple way to determine if code belongs here is to see if you can fill
in the blank:

"We have a policy of ________"

Example: "We have a policy of not allowing duplicate orders"

'''

# Violations
class PolicyViolation(Exception):
    pass


class DuplicateOrder(PolicyViolation):
    pass


class LimitExceeded(PolicyViolation):
       pass


class Policy(object):
    
    def check(self, username, input_products, product_options):
        raise NotImplementedError()

# Policies
class DuplicateOrderPolicy(Policy):
    
    def check(self, username, input_products, product_options):
        pass


class DailyOrderLimitPolicy(Policy):
    
    def check(self, username, input_products, product_options):
        pass

    
class WeeklyOrderLimitPolicy(Policy):
    
    def check(self, username, input_products, product_options):
        pass
    
    
class MonthlyOrderLimitPolicy(Policy):
    
    def check(self, username, input_products, product_options):
        pass


def check_policies(username, input_products, product_options):
    DuplicateOrderPolicy().check(username, input_products, product_options)
    DailyOrderLimitPolicy().check(username, input_products, product_options)
    WeeklyOrderLimitPolicy().check(username, input_products, product_options)
    MonthlyOrderLimitPolicy().check(username, input_products, product_options)