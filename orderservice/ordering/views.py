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
from datetime import datetime

__author__ = "David V. Hill"

def get_option_style(request):
    if hasattr(request, 'user'):
        user = request.user
        return "display:none" if (user.username != 'espa_admin' and user.username != 'espa_internal') else ""

#default landing page for the ordering application
@login_required(login_url='/login/')
def index(request):
      
    t = loader.get_template('index.html')
    c = Context({
        'my_message': 'LDCM R&D ESPA Processing',
    })
            
    return HttpResponse(t.render(c))
                       

#handles getting new orders into the system
@login_required(login_url='/login/')
def neworder(request):
    if request.method == 'GET':
        form = OrderForm()
        c = RequestContext(request,{'form': form,
                                    'user':request.user,
                                    'optionstyle':get_option_style(request)}
                           )
        t = loader.get_template('neworder.html')
        
        #check for system messages that need to be displayed    
        config = Configuration()
        msg = config.getValue('system_message')
                
        if len(msg) > 0 and msg != '' and msg != 'nothing':
            c['system_message'] = msg
        return HttpResponse(t.render(c))

    #request must be a POST and must also be encoded as multipart/form-data in order for the
    #files to be uploaded
    elif request.method == 'POST':
        errors = {}
        if not request.POST.has_key('email') or not core.validate_email(request.POST['email']):
            errors['email'] = "Please provide a valid email address"

        if not request.FILES.has_key("file"):
            errors['file'] = "Please provide a scene list and include at least one scene for processing."
        else:
            scenelist = set()
            orderfile = request.FILES['file']
            lines = orderfile.read().split('\n')

            if len(lines) <= 0:
                errors['file'] = "No scenes found in your scenelist.  Please include at least one scene for processing."
        
        if len(errors) > 0:
            c = RequestContext(request, {'form':form,
                                         'errors':errors,
                                         'user':request.user,
                                         'optionstyle':get_option_style(request)}
                               )
            t = loader.get_template('neworder.html')
            msg = Configuration().getValue('system_message')
            if len(msg) > 0 and msg != '' and msg != 'nothing':
                c['system_message'] = msg
            return HttpResponse(t.render(c))
        
        note = None
        if request.POST.has_key('note'):
            note = request.POST['note']

        #################################################    
        #Form passed' validation.... now check the scenes
        #################################################
        
        #Simple length and prefix checks for scenelist items   
        errors = {}
        errors['scenes'] = list()
        for line in lines:
            line = line.strip()
            if line.find('.tar.gz') != -1:
                line = line[0:line.index('.tar.gz')]
            if len(line) >= 15 and (line.startswith("LT") or line.startswith("LE")):
                scenelist.add(line)

        #Run the submitted list by LTA so they can make sure the items are in the inventory
        lta_service = lta.LtaServices()
        verified_scenes = lta_service.verify_scenes(list(scenelist))

        for sc,valid in verified_scenes.iteritems():
            if valid == 'false':
                errors['scenes'].append("%s not found in Landsat inventory" % sc)

        #See if LTA barked at anything, notify user if so
        if len(errors['scenes']) > 0:                                    
            c = RequestContext(request,{'form':OrderForm(),
                                        'errors':errors,
                                        'user':request.user,
                                        'optionstyle':get_option_style(request)}
                               )
            t = loader.get_template('neworder.html')
            return HttpResponse(t.render(c))
        else:
            #If we made it here we are all good with the scenelist.  
            options = core.get_default_options()
            
            #Collect requested products.
            for o in options.iterkeys():
                if request.POST.has_key(o):
                    options[o] = True
                
            option_string = json.dumps(options)
            order = core.enter_new_order(request.POST['email'], 'espa', scenelist, option_string, note = note)
            core.sendInitialEmail(order)
        
            return HttpResponseRedirect('/status/%s' % request.POST['email'])
        
#handles displaying all orders for a given user
#@login_required(login_url='/login/')
@csrf_exempt
def listorders(request, email=None, output_format=None):
    #print ("%s/%s") % (email,output_format)

    _email = None

    #create placeholder for any validation errors
    errors = {}

    #check to see if this was a get request with an email address in the url
    if email is not None:
            _email = email

    #no email provided, ask user for an email address
    else:
        form = ListOrdersForm()
        c = RequestContext(request,{'form': form})
        t = loader.get_template('listorders.html')
        return HttpResponse(t.render(c))

    #if we got here it's all good, display the orders
    #orders = Order.objects.filter(email=_email).order_by('-order_date')
    orders = core.list_all_orders(_email)
    t = loader.get_template('listorders_results.html')
    mimetype = 'text/html'   
    c = RequestContext(request)
    c['email'] = _email
    c['orders'] = orders
    return HttpResponse(t.render(c), mimetype=mimetype)



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
        
        

############################################
# Form Objects
############################################
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

        


