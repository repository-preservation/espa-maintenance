from email.mime.text import MIMEText
from smtplib import *
from suds.client import Client
from suds import null
from models import Scene,Order,Configuration,TramOrder
from datetime import timedelta
from espa.espa import *
from espa.scene_cache import SceneCache
import time
import json
import datetime


#load configuration values
try:
    tram_service_url = Configuration().getValue('tram.service.url')
    smtp_url = Configuration().getValue('smtp.url')
    espa_email_address = Configuration().getValue('espa.email.address')
    order_status_base_url = Configuration().getValue('order.status.base.url')
except Exception, err:
    print ("Could not load configuration values:%s" % err)


def sendInitialEmail(order):
    status_base_url = Configuration().getValue('espa.status.url')    
    status_url = ('%s/%s') % (status_base_url, order.email)

    header = ("""Thank you for your request for Surface Reflectance processing order number %s.  Your order has been received and is currently being processed.

You will receive an email notification when all units on this order have been completed.

You can check the status of your order and download already completed scenes directly from %s

Requested scenes:\n""") % (order.orderid, status_url)
      
    scenes = Scene.objects.filter(order__id = order.id)
    ordered = header

    if scenes:
        for s in scenes:
            ordered = ordered + s.name + '\n'

    #configure all these values
    msg = MIMEText(ordered)
    msg['Subject'] = 'Processing order received.'
    msg['To'] = order.email
    msg['From'] = 'espa@usgs.gov'
    s = SMTP(host='gssdsflh01.cr.usgs.gov')
    s.sendmail('espa@usgs.gov', order.email, msg.as_string())
    s.quit()



def sendCompletionEmail(email,ordernum,readyscenes=[]):
    status_base_url = Configuration().getValue('espa.status.url')
    status_url = ('%s/%s') % (status_base_url, email)    
    msg = ("""Your order for atmospherically corrected scenes is now complete. All scenes will remain available for 14 days.\n
After 14 days you will need to re-order the requested scenes if you were not able to retrieve them within this timeframe.\n
Order number: %s
Order status url: %s\n
Your scenes
-------------------------------------------\n""") % (ordernum, status_url)
    
    for r in readyscenes:
        msg = msg + r + '\n'  

    #configure these values
    msg = MIMEText(msg)
    msg['Subject'] = 'Processing order complete.'
    msg['To'] = email
    msg['From'] = 'espa@usgs.gov'
    s = SMTP(host='gssdsflh01.cr.usgs.gov')
    s.sendmail('espa@usgs.gov', email, msg.as_string())
    s.quit()
    
    


def sendTramOrder(scenes):
    #configure this
    
    tram_service_url = 'http://edclxs152.cr.usgs.gov/MassLoader/MassLoader?wsdl'
    client = Client(tram_service_url)
    tramorder = client.factory.create('order')
    tramscenes = client.factory.create('scenes')
    tramorder.scenes = tramscenes
    for scene in scenes:
        tramscene = client.factory.create('scene')
        tramscene.sceneId = scene.name
        tramscene.productName = getTramProductName(scene.name)
        tramscene.recipeId = null()
        tramscene.unitComment = null()
        tramscene.parameters = null()
        tramorder.scenes.scene.append(tramscene)
        #print('sceneid:%s, prodname:%s' % (scene.name, getTramProductName(scene.name)))
    tramorder.externalRefNumber = '111111'
    tramorder.orderComment = null()
    tramorder.priority = 5
    #configure this
    tramorder.registrationId = '252380'
    #configure this
    tramorder.requestor = 'EE'
    tramorder.roleId = null()
    
    try:
        response = client.service.submitOrder(tramorder)
        return response
    except Exception, e:
        print ("An error occurred submitting the order to tram: %s" % (e))
        #log error
        return -1


    
#simple helper to get correct product name for tram
def getTramProductName(sceneid):
    if sceneid.startswith('LT5'):
        return 'T273'
    elif sceneid.startswith('LE7'):
        if int(sceneid[9:13]) >= 2003 and int(sceneid[13:16]) >= 151:
            return 'T271'
        else:
            return 'T272'


def generate_order_id(email):
    d = datetime.datetime.now()
    return '%s-%s%s%s-%s%s%s' % (email,d.month,d.day,d.year,d.hour,d.minute,d.second)


def getSceneInputPath(sceneid):
    scene = Scene.objects.get(name=sceneid)
    return scene.getOnlineCachePath()
    

def getScenesToProcess():
    #sanity checks
    
    #are there even any scenes to handle?
    if Scene.objects.filter(status__in=['submitted', 'onorder', 'oncache']).count() <= 0:
        return []
    
    #is cache online?
    cache = SceneCache()
    if cache == None:
        print("Could not create the scene cache...")
        raise Exception("Could not create the scene cache...")
        
    if cache.last_updated() == None:
        #log message about the cache not being loaded
        return []

    #the cache is online and there are scenes to process...
    
    #get all the scenes that are in submitted status
    submitted = Scene.objects.filter(status='submitted')
    
    if submitted:
        
        #check to see which ones are sitting on cache
        submitted_list = [s.name for s in submitted]
        
         #check to see if they are NLAPS scenes first!!!
        nlaps_scenes = cache.is_nlaps(submitted_list)
        
        for s in submitted:
            if s.name in nlaps_scenes:
                s.status = 'unavailable'
                s.note = 'TMA data cannot be processed'
                s.save()
        
        oncache = cache.has_scenes(submitted_list)

        for s in submitted:
            if s.name in oncache:
                s.status = 'oncache'
                s.save()

        #find the submitted scenes that need to be ordered
        need_to_order = []
        for s in submitted:
            if s.status == 'submitted':
                need_to_order.append(s)

        #order these scenes from Tram now
        if len(need_to_order) > 0:
                        
            tram_order_id = sendTramOrder(need_to_order)
            tramorder = TramOrder()
            tramorder.order_id = tram_order_id
            tramorder.order_date = datetime.datetime.now()
            tramorder.save()
                    
            for to in need_to_order:
                to.tram_order = tramorder
                to.status = 'onorder'
                to.save()

    
    #get all the scenes that are on order and check to see if they are on cache
    ordered = Scene.objects.filter(status='onorder')

    if ordered:
        ordered_list = [s.name for s in ordered]
        oncache2 = cache.has_scenes(ordered_list)

        #change status to oncache for the ones that were found
        for s in ordered:
            if s.name in oncache2:
                s.status = 'oncache'
                s.save()

        #don't do anything with the ones that weren't oncache.  They remain on order.

    #now the database should be fully updated with the current status.

    #Pull the current oncache set from the db and include it as the result
    results = []
    available_scenes = Scene.objects.filter(status='oncache')
    if available_scenes:
        for a in available_scenes:
            order = a.order
            options = order.product_options
            options = options.replace("\\", "")
            oid = order.orderid
            #WE need a string that looks like this
            #{'orderid':orderid, 'scene':a.name, 'options':product_options}
            orderline = json.dumps({'orderid':oid, 'scene':a.name, 'options':options})
            print "Orderline follows"
            print orderline
            results.append(orderline)

    return results





#This needs to be changed, so does the distribution datasource.  Distro datasource needs to just put all the orders in the
#/base/whatever/user@host.com/order_num hierarchy structure.  Then when we go to clean up we can just
#wipe out the order_num and be done with it.

def purgeExpiredOrders():
    config = None
    username  = None
    password = None
    host = None
    port = None
    ds = None
    orders = None
    
    try:
        cutoff = datetime.datetime.now() - timedelta(days=14)
        #get the orders where status == complete and that were completed more than 14 days ago
        orders = Order.objects.raw('select * from ordering_order oo where oo.id not in (select order_id from ordering_scene where status in ("queued","onorder","processing","distributing","oncache","purged"))')
        config = Configuration()
        username = config.getValue('distrods.user')
        password = config.getValue('distrods.password')
        host = config.getValue('distrods.host')
        port = config.getValue('distrods.port')
        ds = DistributionDataSource(None, None, username, password, host, port)
        for o in orders:
            diff = cutoff - o.completion_date 
            if diff.days >= 0:
                scenes = Scene.objects.filter(order__id = o.id)
                for s in scenes:
                    ds.delete(s.name, s.product_distro_location)
                o.delete()
    finally:
        config = None
        username  = None
        password = None
        host = None
        port = None
        ds = None
        orders = None

def helperlogger(msg):
    #print(msg)
    #h = open('/tmp/helper.log', 'a+')
    #h.write(msg)
    #h.flush()
    #h.close()
    pass

    
def updateStatus(name, orderid, processing_loc, status):
    helperlogger("Updating status for scene:%s in order:%s from location:%s to %s\n" % (name,orderid,processing_loc,status))
    try:
        s = Scene.objects.get(name=name, order__orderid = orderid)
        if s:                
            helperlogger("Running update query for %s.  Setting status to:%s" % (s.name,status))
            s.status = status
            s.processing_location = processing_loc
            s.save()
            s = None
            return True
        else:
            #something went wrong, don't clean up other disk.
            #print("UpdateStatus:No scene was found with the name:%s for order:%s") % (name, orderid)
            helperlogger("UpdateStatus:No scene was found with the name:%s for order:%s" % (name, orderid))
            return False
    except Exception,e:
        helperlogger("Exception in updateStatus:%s" % e)
    
def setSceneError(name, orderid, processing_loc, error):
    o = Order.objects.get(orderid = orderid)
    s = Scene.objects.get(name=name, order__id = o.id)
    if s:
        s.status = 'error'
        s.processing_location = processing_loc
        s.log_file_contents = error
        s.save()
        return True
    else:
        #something went wrong, don't clean up other disk.
        print("setSceneError:No scene was found with the name:%s for order:%s") % (name, orderid)
        return False

def markSceneComplete(name, orderid, processing_loc,completed_file_location, destination_cksum_file = None,log_file_contents=None):
    print ("Marking scene:%s complete for order:%s" % (name, orderid))
    o = Order.objects.get(orderid = orderid)
    s = Scene.objects.get(name=name, order__id = o.id)
    if s:
        s.status = 'complete'
        s.processing_location = processing_loc
        s.product_distro_location = completed_file_location
        s.completion_date = datetime.datetime.now()
        s.cksum_distro_location = destination_cksum_file
        
        #if source_l1t_location is not None:
            #s.source_distro_location = source_l1t_location

        s.log_file_contents = log_file_contents
                                
        #Need to modify this as soon as we're going to start
        #providing more than 1 product
        base_url = Configuration().getValue('distribution.cache.home.url')

        product_file_parts = completed_file_location.split('/')
        product_file = product_file_parts[len(product_file_parts) - 1]
        cksum_file_parts = destination_cksum_file.split('/')
        cksum_file = cksum_file_parts[len(cksum_file_parts) - 1]
        s.product_dload_url = ('%s/orders/%s/%s') % (base_url,orderid,product_file)  
        s.cksum_download_url = ('%s/orders/%s/%s') % (base_url,orderid,cksum_file)
        s.save()

        sendEmailIfComplete(o.orderid,s)
            
        return True
    else:
        print("MarkSceneComplete:No scene was found with the name:%s" % name)
        return False


def sendEmailIfComplete(orderid, scene):
    '''Method to send out the order completion email for orders if the completion of a scene completes the order'''    
    o = Order.objects.get(orderid = orderid)
    scenes = Scene.objects.filter(order__id = o.id)

    #we have to do this because we need to see if all scenes for the given order are complete.
    #Don't know how to run that query through the Django Model interface.
    isComplete = True
    for s in scenes:
        if s.status != 'complete' and s.status != 'unavailable':
            isComplete = False
            break
        
    if isComplete and scenes:
        scene_names = [s.name for s in scenes if s.status != 'unavailable']
        o.status = 'complete'
        o.completion_date = datetime.datetime.now()
        o.save()
        sendCompletionEmail(o.email,o.orderid,readyscenes=scene_names)
                       
        
        

    

    


