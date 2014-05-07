########################################################################################################################
# Views.py
# Purpose: Handles all the interaction with espa web pages
# Original Author: David V. Hill
########################################################################################################################

import core
import json

import ordering.view_validator as vv

from ordering.models import Scene
from ordering.models import Order 
from ordering.models import Configuration
from ordering.models import UserProfile

from django import forms

from django.contrib.auth import logout
from django.contrib.auth.models import User
from django.contrib.auth.decorators import login_required
from django.contrib.syndication.views import Feed

from django.http import HttpResponse
from django.http import HttpResponseRedirect

from django.shortcuts import get_list_or_404

from django.template import loader
from django.template import Context
from django.template import RequestContext
from django.utils.feedgenerator import Rss201rev2Feed
from django.views.generic import View

class AbstractView(View):

    def _get_option_style(self, request):
        '''Utility method to determine which options to display in the templates based on the
        user.
        '''
        if hasattr(request, 'user'):        
            if request.user.username not in ('espa_admin', 'espa_internal'):
                return "display:none"
            else:
                return ""

       
    def _display_system_message(self, context):
        '''Utility method to populate the context with systems messages if there are any
        configured for display
        '''
        msg = Configuration().getValue('display_system_message')

        if msg.lower() == 'true':

            context['display_system_message'] = True
        
            context['system_message_title'] = Configuration().getValue('system_message_title')

            context['system_message_1'] = Configuration().getValue('system_message_1')

            context['system_message_2'] = Configuration().getValue('system_message_2')

            context['system_message_3'] = Configuration().getValue('system_message_3')
        else:
            context['display_system_message'] = False


#@login_required(login_url='/login/')
class Index(AbstractView):
    template = 'index.html'
    
    def get(self, request):
        '''Request handler for / and /index'''

        t = loader.get_template(self.template)
    
        c = Context({
            'my_message': 'LSDS Science R&D Processing',
        })
        
        self._display_system_message(c)    
        
        return HttpResponse(t.render(c))
                       

#@login_required(login_url='/login/')
class NewOrder(AbstractView):
    template = 'new_order.html'

    def get(self, request):
        '''Request handler for /neworder'''
        c = RequestContext(request,{'user':request.user,
                                    'optionstyle':self._get_option_style(request)
                                   })
    
        t = loader.get_template(self.template)        
        
        self._display_system_message(c)    
       
        return HttpResponse(t.render(c))
        
    def post(self, request):
        #request must be a POST and must also be encoded as multipart/form-data 
        #in order for the files to be uploaded
        
        context, errors, scene_errors = vv.validate_input_params(request)
        
        prod_option_context, prod_option_errors = vv.validate_product_options(request)
        
        if len(prod_option_errors) > 0:
            errors['product_options'] = prod_option_errors

        if len(scene_errors) > 0:
            errors['scenes'] = scene_errors
        
        print prod_option_context

        print "ERRORS"
        print errors
        
        if len(errors) > 0:

            print "Errors Detected..."

            c = RequestContext(request, {'errors':errors,
                                         'user':request.user,
                                         'optionstyle':self._get_option_style(request)
                                         }
                               )    

            t = loader.get_template(self.template)

            self._display_system_message(c)    

            return HttpResponse(t.render(c))
        else:
            print "No errors detected"
            
            option_string = json.dumps(prod_option_context)
            
            print "Option String"
            print option_string

            print "Saving new order"
            order = core.enter_new_order(context['email'],
                                         'espa',
                                         context['scenelist'],
                                         option_string,
                                         note = context['order_description']
                                         )
            print "Sending email"
            core.sendInitialEmail(order)

            print "Redirecting to status page"            
            return HttpResponseRedirect('/status/%s' % request.POST['email'])


#@login_required(login_url='/login/')
class ListOrders(AbstractView):
    
    def get(self, request, email=None, output_format=None):
        '''Request handler for displaying all user orders'''
    
        #no email provided, ask user for an email address
        if email is None or not core.validate_email(email):

            form = ListOrdersForm()

            c = RequestContext(request, {'form': form})
        
            self._display_system_message(c)    

            t = loader.get_template('listorders.html')

            return HttpResponse(t.render(c))
        else:
            #if we got here display the orders
            orders = core.list_all_orders(email)
        
            t = loader.get_template('listorders_results.html')

            c = RequestContext(request)

            c['email'], c['orders'] = email, orders

            self._display_system_message(c)    
  
            return HttpResponse(t.render(c))


#@login_required(login_url='/login/')
class OrderDetails(AbstractView):
    template = 'orderdetails.html'
    
    def get(self, request, orderid, output_format=None):
        '''Request handler to get the full listing of all the scenes & statuses for an order'''           

        t = loader.get_template(self.template)
    
        c = RequestContext(request)

        self._display_system_message(c)    

        c['order'], c['scenes'] = core.get_order_details(orderid)

        return HttpResponse(t.render(c))
        
        
class ListOrdersForm(forms.Form):
    '''Form object for the ListOrders form'''
    email = forms.EmailField()
        

class StatusFeed(Feed):
    '''Feed subclass to publish user orders via RSS'''
        
    feed_type = Rss201rev2Feed

    title = "ESPA Status Feed"

    link = ""
    
    def get_object(self, request, email):
        return get_list_or_404(Order, email=email)

    def link(self, obj):
        return "/status/%s/rss/" % obj[0].email
    
    def description(self, obj):
        #return "status:" % obj.status
        return "ESPA scene status for:%s" % obj[0].email
    
    def item_title(self, item):
        return item.name
    
    def item_link(self, item):
        return item.product_dload_url
    
    def item_description(self, item):
        orderid = item.order.orderid
        orderdate = item.order.order_date
        return "scene_status:%s,orderid:%s,orderdate:%s" % (item.status, orderid, orderdate)
           
    def items(self, obj):
        email = obj[0].email
        
        return Scene.objects.filter(status='complete',order__email=email).order_by('-order__order_date')

        


