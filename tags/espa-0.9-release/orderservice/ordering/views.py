# Create your views here.
from django.http import HttpResponse, HttpResponseRedirect
from django.contrib.auth.decorators import login_required
from ordering.models import Scene,Order,SceneOrder
from django.template import Context, loader, RequestContext
from django import forms
from django.core.files.uploadedfile import SimpleUploadedFile
from django.contrib.auth.models import User
from django.contrib.auth import logout
import re
from ordering.helper import *



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

        #create placeholder for any validation errors
        errors = {}

        #Validate request parameters or fail
        if not request.POST['email'] or not validate_email(request.POST['email']):
            errors['email'] = "Please provide a valid email address"           

        if not request.FILES.getlist('file'):
            errors['file'] = "Please provide a scene list"
                   
        if len(errors) > 0:
            #fail here back to form and display errors
            form = OrderForm(initial={'email':request.POST['email']})
            c = RequestContext(request,{'form':form,'errors':errors})
            t = loader.get_template('neworder.html')
            return HttpResponse(t.render(c))

        #end validation

        #made it here so the order was submitted correctly, store files and enter order
        #in db, then send processing message to backend.
        order = Order()
        order.orderid = generate_order_id(request.POST['email'])
        order.email = request.POST['email']
        #this is bad.  leaves the potential for and order to be created with no scenes attached to it.
        order.save()
        
        for f in request.FILES.getlist('file'):
            contents = f.read()            
            lines = contents.split('\n')
            to_order = list()

            sceneCount = 0
            for line in lines:
                if line.startswith('L'): # and len(line) == 21 and (line.startswith('LE') or line.startswith('LT'))
                    if line.endswith('.tar.gz'):
                        line = line[0:line.index('.tar.gz')]
                        
                    sceneCount = sceneCount + 1
                    
                    scene = None
                    #go look for an existing scene that is not purged first
                    existingScene = Scene.objects.filter(name = line.strip())

                    if len(existingScene) > 0:
                        scene = existingScene[0]
                        #need to update the date so this doesn't get blown away before 14 days
                        if scene.status == 'Complete':
                            scene.completion_date = datetime.now()
                    else:                      
                        scene = Scene()
                        scene.name = line.strip()
                        #scene.order = order
                        scene.order_date = datetime.now()
                        if scene.isOnCache():
                            scene.status = 'On Cache'
                        elif scene.isNlapScene():
                            scene.status = 'Unavailable'
                            scene.note = 'NLAPS only scene'
                        elif scene.name.startswith('LT4'):
                            scene.status = 'Unavailable'
                            scene.note = 'Landsat 4 Not Supported'
                        else:
                            to_order.append(scene)
                            scene.status = 'On Order'


                    #This stuff needs to be executed only if the scene does not already exist.  It is allowing duplicates
                    #into the system
                    #we should definitely have a status set on this scene now
                    scene.save()

                    #create relation between the order and the scene
                    so = SceneOrder()
                    so.scene = scene
                    so.order = order
                    so.save()
                else:
                    #line did not start with L
                    pass
            #end for
            if sceneCount == 0:
                #complain and inform user no scenes were submitted.  tell them to try again.
                pass
            else:
                order.save()
            
                if len(to_order) > 0:
                    tramorderid = sendTramOrder(to_order)
                    for s in to_order:
                       s.tram_order_id = tramorderid
                       s.save()
              
                sendInitialEmail(order)

                
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
@login_required(login_url='/login/')
def listorders(request, email=None, output_format=None):
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
        scenes = Scene.objects.filter(order__email=_email,status='Complete')
        output = ''
        for scene in scenes:
            line = ("%s,%s,%s\n") % (scene.name,scene.download_url,scene.source_l1t_download_url)
            output = output + line
        return HttpResponse(output, mimetype='text/plain')
        
    else:
        orders = Order.objects.filter(email=_email)
        t = loader.get_template('listorders_results.html')
        mimetype = 'text/html'   
        c = RequestContext(request)
        c['email'] = _email
        c['orders'] = orders
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
    
        
        
        

        


