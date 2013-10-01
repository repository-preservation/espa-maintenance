########################################################################################################################
# Views.py
# Purpose: Handles all the interaction with espa web pages
# Original Author: David V. Hill
########################################################################################################################


from django.http import HttpResponse, HttpResponseRedirect
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
from django.contrib.syndication.views import Feed
from django.contrib.syndication.views import FeedDoesNotExist
from django.shortcuts import get_object_or_404,get_list_or_404
from django.utils.feedgenerator import Rss201rev2Feed
from django.template import Context, loader, RequestContext
from django import forms
from django.core.files.uploadedfile import SimpleUploadedFile
from django.contrib.auth.models import User
from django.contrib.auth import logout
from ordering.models import Scene,Order,Configuration
import core, lta, json
import view_validator as vv
from datetime import datetime


########################################################################################################################
# get_option_style()
# Utility method to determine what options to display based on the user thats logged in
########################################################################################################################
def get_option_style(request):
    if hasattr(request, 'user'):
        user = request.user
        return "display:none" if (user.username != 'espa_admin' and user.username != 'espa_internal') else ""

########################################################################################################################
#default landing page for the ordering application
#@login_required(login_url='/login/')
########################################################################################################################
def index(request):
      
    t = loader.get_template('index.html')
    c = Context({
        'my_message': 'LSDS Science R&D Processing',
    })
            
    return HttpResponse(t.render(c))
                       
########################################################################################################################
#Request handler for /neworder.  Handles getting new orders into the system
########################################################################################################################
@login_required(login_url='/login/')
def neworder(request):
    
    ####################################################################################################################
    #Includes the system message in the request context if one is defined
    ####################################################################################################################
    def include_system_message(request_context):
        msg = Configuration().getValue('system_message')
        if len(msg) > 0 and msg != '' and msg != 'nothing':
            c['system_message'] = msg
    
    
        
    ####################################################################################################################
    #request handling
    ####################################################################################################################
    if request.method == 'GET':
        c = RequestContext(request,{'user':request.user,
                                    'optionstyle':get_option_style(request)}
                           )
        #t = loader.get_template('neworder.html')
        t = loader.get_template('rework.html')
        include_system_message(c)
        return HttpResponse(t.render(c))
        
    elif request.method == 'POST':
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
                                         'optionstyle':get_option_style(request)}
                               )    
            t = loader.get_template('rework.html')
            include_system_message(c)
            return HttpResponse(t.render(c))
        else:
            print "No errors detected"
            
            option_string = json.dumps(prod_option_context)
            
            print "Option String"
            print option_string
            print "Saving new order"
            order = core.enter_new_order(context['email'], 'espa', context['scenelist'], option_string, note = context['order_description'])
            print "Sending email"
            core.sendInitialEmail(order)
            print "Redirecting to status page"
            
            return HttpResponseRedirect('/status/%s' % request.POST['email'])


########################################################################################################################
#handles displaying all orders for a given user
########################################################################################################################
#@login_required(login_url='/login/')
@csrf_exempt
def listorders(request, email=None, output_format=None):

    #no email provided, ask user for an email address
    if email is None or not core.validate_email(email):
        form = ListOrdersForm()
        c = RequestContext(request,{'form': form})
        t = loader.get_template('listorders.html')
        return HttpResponse(t.render(c))

    #if we got here it's all good, display the orders
    orders = core.list_all_orders(email)
    t = loader.get_template('listorders_results.html')
    mimetype = 'text/html'   
    c = RequestContext(request)
    c['email'] = email
    c['orders'] = orders
    return HttpResponse(t.render(c), mimetype=mimetype)


########################################################################################################################
# Request handler to get the full listing of all the scenes & statuses for an order
########################################################################################################################
@csrf_exempt
def orderdetails(request, orderid, output_format=None):
    '''displays scenes for an order'''           

    t = loader.get_template('orderdetails.html')
    mimetype = 'text/html'   
    c = RequestContext(request)

    order,scenes = core.get_order_details(orderid)
    c['order'] = order
    c['scenes'] = scenes

    return HttpResponse(t.render(c), mimetype=mimetype)
        
        

########################################################################################################################
# Form Objects
########################################################################################################################
class ListOrdersForm(forms.Form):
    email = forms.EmailField()

class OrderForm(forms.Form):
    email = forms.EmailField()
    #add fields here for scene uploads
    files = forms.FileField()
    #dataset = forms.ChoiceField(choices=Order.DATASETS,required=True)
    note = forms.CharField(widget=forms.Textarea(attrs={'cols':'80'}))
    
    include_sourcefile = forms.BooleanField(initial=False)
    include_source_metadata = forms.BooleanField(initial=True)
    include_sr_toa = forms.BooleanField(initial=True)
    include_sr_thermal = forms.BooleanField(initial=False)
    include_sr = forms.BooleanField(initial=True)
    include_sr_browse = forms.BooleanField(initial=False)
    include_sr_ndvi = forms.BooleanField(initial=False)
    include_sr_ndmi = forms.BooleanField(initial=False)
    include_sr_nbr = forms.BooleanField(initial=False)
    include_sr_nbr2 = forms.BooleanField(initial=False)
    include_sr_savi = forms.BooleanField(initial=False)
    include_sr_evi = forms.BooleanField(initial=False)
    include_solr_index = forms.BooleanField(initial=False)
    include_cfmask = forms.BooleanField(initial=False)
        
########################################################################################################################

########################################################################################################################
class StatusFeed(Feed):
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

        


