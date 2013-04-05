# Create your views here.
from django.http import HttpResponse, HttpResponseRedirect
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_exempt
from ordering.models import Scene,Order,TramOrder#,SceneOrder
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

    #request must be a POST and must also be encoded as multipart/form-data in order for the
    #files to be uploaded
    if request.method == 'POST':
        
        dataset = None
        
        note = None
        
        if request.POST.has_key('note'):
            note = request.POST['note']
        
        #create placeholder for any validation errors
        errors = {}

        #Validate request parameters or fail
        if not request.POST.has_key('email') or not validate_email(request.POST['email']):
            errors['email'] = "Please provide a valid email address"           

        if not request.FILES.has_key('file') or not request.FILES.getlist('file'):
            errors['file'] = "Please provide a scene list"
                   
                  
        if not request.POST.has_key('dataset') or len(request.POST['dataset']) < 1:
            dataset = 'sr_ondemand'

        #end validate parameters

   
    
        scenelist = list()     
        for f in request.FILES.getlist('file'):
            #FIRST, parse inputs and verify they are good
            
            contents = f.read()            
            lines = contents.split('\n')
            to_order = list()

            sceneCount = 0
            
            #nlapsds = NLAPSDataSource(logger, context, username, password, host, port)
            config = Configuration()
            username = config.getValue('nlapsds.username')
            password = config.getValue('nlapsds.password')
            host = config.getValue('nlapsds.host')
            port = config.getValue('nlapsds.port')
            nlapsds = NLAPSDataSource(None, {}, username, password, host, port)
                       
            
            for line in lines:
                
                if line.startswith('LT5') or line.startswith('LE7'):
                    
                    #clean it up
                    line = line.strip()
                    if line.endswith('.tar.gz'):
                        line = line[0:line.index('.tar.gz')]
                    
                    #start validating entries
                    if len(line) != 21:
                        iserror = True
                        if not errors.has_key('scenes'):
                            errors['scenes'] = list()
                        errors['scenes'].append('%s is not a valid scene' % line)
                        continue
                
                    if nlapsds.isAvailable(line):
                        iserror = True
                        if not errors.has_key('scenes'):
                            errors['scenes'] = list()
                        errors['scenes'].append('%s is an NLAPS only scene' % line)
                        continue    
              
                    scenelist.append(line)
                #else if LT4 bark!
                    
            if len(scenelist) < 1:
                if not errors.has_key('scenes'):
                            errors['scenes'] = list()
                errors['scenes'].append("No valid scenes were found in the order.")
                #if not InventoryService().isValid(line):
                #    error['scenes'].append('%s not found in inventory' % line)
            
            #check to see if we're good or the user goofed... if bad tell them.
            if errors.has_key('scenes') and len(errors['scenes']) > 0:
                #fail here back to form and display errors
                form = OrderForm(initial={'email':request.POST['email']})
                c = RequestContext(request,{'form':form,'errors':errors})
                t = loader.get_template('neworder.html')
                return HttpResponse(t.render(c))
            else:
                username = config.getValue('landsatds.username')
                password = config.getValue('landsatds.password')
                host = config.getValue('landsatds.host')
                port = config.getValue('landsatds.port')
                landsatds = LandsatDataSource(None, {}, username, password, host, port)
                
                #everything is ok, go ahead and order up the scenes
                order = Order()
                order.orderid = generate_order_id(request.POST['email'])
                order.email = request.POST['email']
                order.chain = dataset
                order.note = note
                order.status = 'ordered'
                order.order_date = datetime.now()
               
                order.save()
                
                for s in scenelist:
                    scene = Scene()
                    scene.name = s
                    scene.order = order
                    scene.order_date = datetime.now()
                    
                   
                   
                    if landsatds.isAvailable(scene.name):                                
                        scene.status = 'oncache'
                    else:
                        to_order.append(scene)
                        scene.status = 'onorder'
                    scene.save()
                
                if len(to_order) > 0:
                    tramorderid = sendTramOrder(to_order)
                    tramorder = TramOrder()
                    tramorder.order_id = tramorderid
                    tramorder.order_date = datetime.now()
                    tramorder.save()
                    
                    for to in to_order:
                        to.tram_order = tramorder
                        to.save()
                
                sendInitialEmail(order)
                
            
            
            #end for
            #if sceneCount == 0:
                #complain and inform user no scenes were submitted.  tell them to try again.
                #error['scenes'].append("No scenes submitted for processing...")
                #order.delete()
            #else:
                #order.save()
            
                #if len(to_order) > 0:
                #    tramorderid = sendTramOrder(to_order)
                #    for s in to_order:
                #       s.tram_order_id = tramorderid
                #       s.save()
              
                #sendInitialEmail(order)

                
        #reuse our existing order status view for the confirmation page
        #return listorders(request, request.POST['email'])
        status_url = '/status/%s' % request.POST['email']
        return HttpResponseRedirect(status_url)
                
    else:
        #came in on a GET, display blank form
        form = OrderForm()
        c = RequestContext(request,{'form': form})
        t = loader.get_template('neworder.html')
        return HttpResponse(t.render(c))



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
    
        
class StatusFeed(Feed):
    feed_type = Rss201rev2Feed
    title = "ESPA Status Feed"
    link = "forshizzle"
    
    def get_object(self, request, email):
        #return get_object_or_404(Order, email=email)
        return get_list_or_404(Order, email=email)

    def link(self, obj):
        return "/status/%s/rss/" % obj[0].email
        #return obj.product_dload_url
    

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

        


