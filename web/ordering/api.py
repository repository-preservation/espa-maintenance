class User(object):
    '''The User API'''

    def new_order(self, verification_token, product_list, options):
        pass

    def view_order(self, order_id):
        pass

    def list_orders(self):
        pass

    def verify_products(self, product_list, ignore_errors=True):
        pass

    def search(self,
               north,
               south,
               east,
               west,
               start_year,
               start_month,
               start_day,
               end_year,
               end_month,
               end_day,
               include_months=['all'],
               include_years=['all'],
               include_products=['all'],
               limit=500):
        pass


class Admin(object):
    '''The Administration API'''

    def list_policies(self):
        pass

    def list_limits(self):
        pass

    def new_order(self,
                  user_name,
                  verification_token,
                  product_list,
                  options,
                  skip_policies=[],
                  skip_limits=[]):
        '''Allows an administrator to enter orders on the behalf of users
        and also to bypass configured policies or limits'''
        pass

    def delete_order(self, order_id):
        pass

    def update_order(self):
        pass

    def set_product_status(self,
                           new_status,
                           start_date=None,
                           end_date=None,
                           from_status=[],
                           for_users=[],
                           for_orders=[]):
        pass

    def list_orders(self,
                    for_users=[],
                    in_status=[],
                    start_date=None,
                    end_date=None):
        pass


class System(object):
    '''The System API'''
    pass
