# Create your views here.
from django.http import HttpResponse, HttpResponseRedirect
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
from ordering.models import Scene,Order,TramOrder,Configuration#,SceneOrder
from django.template import Context, loader, RequestContext
from django import forms
from django.core.files.uploadedfile import SimpleUploadedFile
from django.contrib.auth.models import User
from django.contrib.auth import logout
import re
from ordering.helper import *
from espa.espa import *
from django.contrib.syndication.views import Feed
from django.contrib.syndication.views import FeedDoesNotExist
from django.shortcuts import get_object_or_404,get_list_or_404
from django.utils.feedgenerator import Rss201rev2Feed
import json




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
        user = None
        if hasattr(request, 'user'):
            user = request.user
            if user.username != 'espa_admin' and user.username != 'espa_internal':
                optionstyle = "display:none"
            else:
                optionstyle = ""
        
        c = RequestContext(request,{'form': form, 'user':user,'optionstyle':optionstyle})
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
        if not request.POST.has_key('email') or not validate_email(request.POST['email']):
            errors['email'] = "Please provide a valid email address"
        if not request.FILES.has_key("file"):
            errors['file'] = "Please provide a scene list"
        
        if len(errors) > 0:                    
            c = RequestContext(request, {'form':form, 'errors':errors})
            t = loader.get_template('neworder.html')
            msg = Configuration().getValue('system_message')
            if len(msg) > 0 and msg != '' and msg != 'nothing':
                c['system_message'] = msg
            return HttpResponse(t.render(c))
        
        note = None
        if request.POST.has_key('note'):
            note = request.POST['note']
            
        #Form passed' validation.... 
        scenelist = set()     
        orderfile = request.FILES['file']
        lines = orderfile.read().split('\n')
            
        errors = {}
        errors['scenes'] = list()
        for line in lines:
            line = line.strip()
            if line.startswith('LT5') or line.startswith('LE7'):            
                                
                if line.endswith('.tar.gz'):
                    line = line[0:line.index('.tar.gz')]
                
                if len(line) != 21:
                    errors['scenes'].append('%s is not a valid scene' % line)
                    continue
                      
                scenelist.add(line)
                                
        if len(scenelist) < 1:
            errors['scenes'].append("No valid scenes were found in the order.")
                
        if len(errors['scenes']) > 0:
            c = RequestContext(request,{'form':form,'errors':errors})
            t = loader.get_template('neworder.html')
            return HttpResponse(t.render(c))
        else:
            options = {
                'include_sourcefile':False,
                'include_source_metadata':False,
                'include_sr_toa':False,
                'include_sr_thermal':False,
                'include_sr':False,
                'include_sr_browse':False,
                'include_sr_ndvi':False,
                'include_solr_index':False,
                'include_cfmask':False
                }
            
           
            for o in options.iterkeys():
                if request.POST.has_key(o):
                    options[o] = True
                
            option_string = json.dumps(options)
           
            order = Order()
            order.orderid = generate_order_id(request.POST['email'])
            order.email = request.POST['email']
            order.note = note
            order.status = 'ordered'
            order.order_date = datetime.now()
            order.product_options = option_string
            order.save()
                
            for s in set(scenelist):
                scene = Scene()
                scene.name = s
                scene.order = order
                scene.order_date = datetime.now()
                scene.status = 'submitted'
                scene.save()
                
            sendInitialEmail(order)
    
            status_url = '/status/%s' % request.POST['email']
    
            return HttpResponseRedirect(status_url)
        
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


    mimetype = None
    orders = None
    #if we got here it's all good, display the orders
    if output_format is not None and output_format == 'csv':
        scenes = Scene.objects.filter(order__email=_email,status='Complete').order_by('-order__order_date')
        output = ''
        for scene in scenes:
            line = ("%s,%s,%s\n") % (scene.name,scene.download_url,scene.source_l1t_download_url)
            output = output + line
        return HttpResponse(output, mimetype='text/plain')
        
    else:
        orders = Order.objects.filter(email=_email).order_by('-order_date')
        t = loader.get_template('listorders_results.html')
        mimetype = 'text/html'   
        c = RequestContext(request)
        c['email'] = _email
        c['orders'] = orders
        return HttpResponse(t.render(c), mimetype=mimetype)

#handles displaying scenes for an order all orders for a given user

@csrf_exempt
def orderdetails(request, orderid, output_format=None):
   
    #create placeholder for any validation errors
    errors = {}

    mimetype = None
    scenes = None
    #if we got here it's all good, display the orders
    if output_format is not None and output_format == 'csv':
        scenes = Scene.objects.filter(order__orderid=orderid,status='Complete')
        output = ''
        for scene in scenes:
            line = ("%s,%s,%s\n") % (scene.name,scene.download_url,scene.source_l1t_download_url)
            output = output + line
        return HttpResponse(output, mimetype='text/plain')
        
    else:
        order = Order.objects.get(orderid=orderid)
        scenes = Scene.objects.filter(order__orderid=orderid)
        t = loader.get_template('orderdetails.html')
        mimetype = 'text/html'   
        c = RequestContext(request)
        c['order'] = order
        c['scenes'] = scenes
        return HttpResponse(t.render(c), mimetype=mimetype)
        
        
#email validate method
def validate_email(email):
    pattern = '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}$'
    return re.match(pattern, email)

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

        


