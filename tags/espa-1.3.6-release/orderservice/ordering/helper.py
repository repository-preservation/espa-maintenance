from email.mime.text import MIMEText
from smtplib import *
from datetime import datetime
from suds.client import Client
from suds import null
from models import Scene,Order,Configuration
from datetime import datetime,timedelta
from espa.espa import *



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

    header = ("""Thank you for your request for Surface Reflectance processing.  Your order has been received and is currently being processed.
Order number: %s
Order status url: %s
Requested scenes:\n""") % (order.orderid, status_url)
    
    onorder = """
On Order - Will be processed once available (maximum 1 to 3 days)
-------------------------------------------
"""

    ready = """
On Cache - Ready For Processing
-------------------------------------------
"""
    unavailable = """
Unavailable
-------------------------------------------
"""
    
    scenes = Scene.objects.filter(order__id = order.id)
    orderedscenes = [scene.name for scene in scenes if scene.status == 'onorder']
    #orderedscenes = [scene.name for scene in order.scenes.all() if scene.status == 'On Order']
    readyscenes = [scene.name for scene in scenes if scene.status != 'onorder' and scene.status != 'unavailable']
    #readyscenes = [scene.name for scene in order.scenes.all() if scene.status != 'On Order' and scene.status != 'Unavailable']
    unavailablescenes = [scene.name for scene in scenes if scene.status == 'unavailable']
    #unavailablescenes = [scene.name for scene in order.scenes.all() if scene.status == 'Unavailable']

    ordered = header

    if orderedscenes:
        ordered = ordered + onorder
        for o in orderedscenes:
            ordered = ordered + o + '\n'

    if readyscenes:
        ordered = ordered + ready
        for o in readyscenes:
            ordered = ordered + o + '\n'

    if unavailablescenes:
        ordered = ordered + unavailable
        for o in unavailablescenes:
            ordered = ordered + o + '\n'
  

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
    tramorder.priority = 9
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
    d = datetime.now()
    return '%s-%s%s%s-%s%s%s' % (email,d.month,d.day,d.year,d.hour,d.minute,d.second)


def getSceneInputPath(sceneid):
    scene = Scene.objects.get(name=sceneid)
    return scene.getOnlineCachePath()
    

def getScenesToProcess():
    #FUCK!  We need to tie in with the espa datasources here too.
    scenes = Scene.objects.filter(status = 'onorder')[:50]
    print("Get scenes To Process")
    results = []
    
    config = Configuration()
    username = config.getValue('landsatds.username')
    password = config.getValue('landsatds.password')
    host = config.getValue('landsatds.host')
    port = config.getValue('landsatds.port')
    
    ds = LandsatDataSource(None, {},username, password, host, port)
    
    if scenes:
        for s in scenes:
            if ds.isAvailable(s.name) and s.status == 'onorder':
                s.status = 'oncache'
                s.save()
                
            #hardwire this bitch for the landsatDS right now
            #if s.isOnCache() and s.status == 'On Order':
            #    s.status = 'On Cache'
            #    s.save()
    
    scenes2 = Scene.objects.filter(status = 'oncache')[:50]

    if scenes2:    
        for ss in scenes2:
            order = ss.order
            oid = order.orderid
            results.append((oid,ss.name))

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
        cutoff = datetime.now() - timedelta(days=14)
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
            
def updateStatus(name, orderid, processing_loc, status):
    print ("Updating status for scene:%s in order:%s from location:%s to %s") % (name,orderid,processing_loc,status)
    o = Order.objects.get(orderid = orderid)
    s = Scene.objects.get(name=name, order__id = o.id)
    if s:                
        s.status = status
        s.processing_location = processing_loc
        s.save()
        return True
    else:
        #something went wrong, don't clean up other disk.
        print("UpdateStatus:No scene was found with the name:%s for order:%s") % (name, orderid)
        return False
    
def setSceneError(name, orderid, processing_loc, error):
    o = Orders.objects.get(orderid = orderid)
    s = Scene.objects.get(name=name, order__id = o.id)
    if s:
        s.status = status
        s.processing_location = processing_loc
        s.log_file_contents = error
        s.save()
        return True
    else:
        #something went wrong, don't clean up other disk.
        print("setSceneError:No scene was found with the name:%s for order:%s") % (name, orderid)
        return False

def markSceneComplete(name, orderid, processing_loc,completed_file_location, source_l1t_location = None,log_file_contents=None):
    o = Order.objects.get(orderid = orderid)
    s = Scene.objects.get(name=name, order__id = o.id)
    if s:
        s.status = 'complete'
        s.processing_location = processing_loc
        s.product_distro_location = completed_file_location
        s.completion_date = datetime.now()
        if source_l1t_location is not None:
            s.source_distro_location = source_l1t_location
        s.log_file_contents = log_file_contents
        u = Utilities(None)
        path = u.getPath(s.name)
        #path = s.getPath()
        path = u.stripZeros(path)
        
        row = u.getRow(s.name)
        row = u.stripZeros(row)
        
        #strip leading 0 off of path and row if it exists
        #if str(path).startswith('0'):
        #    path = str(path)[1:len(str(path))]

        #row = s.getRow()
        #if str(row).startswith('0'):
        #    row = str(row)[1:len(str(row))]
        
        #Need to modify this as soon as we're going to start
        #providing more than 1 product
        base_url = Configuration().getValue('distribution.cache.home.url')
        

        #This will always place the source right next to the generated product.    
        #base = base_url + '/' + sr_path
        
        config = Configuration()
        username = config.getValue('distributionds.user')
        password = config.getValue('distributionds.password')
        host = config.getValue('distributionds.host')
        port = config.getValue('distributionds.port')
        ds = DistributionDataSource(None, {'chain.name':'sr','order.id':orderid}, username, password, host, port)
        filepath = ds.getDataSourcePath(s.name)
      
        
        #s.product_dload_url = ('%s/%s/%s') % (base, filepath, s.name + '-sr.tar.gz')
        #s.source_download_url = ('%s/%s/%s') % (base, filepath, s.name + '.tar.gz')
        #s.product_dload_url = ('%s/espa/orders/%s/%s/%s') % (base_url,orderid,'sr',s.name + '-sr.tar.gz' )
        #s.product_dload_url = ('%s/data2/LSRD/orders/%s/%s/%s') % (base_url,orderid,'sr',s.name + '-sr.tar.gz' )
        s.product_dload_url = ('%s/orders/%s/%s/%s') % (base_url,orderid,'sr',s.name + '-sr.tar.gz' )
        #s.source_download_url = ('%s/espa/orders/%s/%s/%s') % (base_url,orderid,'sr',s.name + '.tar.gz' )
        #s.source_download_url = ('%s/data2/LSRD/orders/%s/%s/%s') % (base_url,orderid,'sr',s.name + '.tar.gz' )
        s.source_download_url = ('%s/orders/%s/%s/%s') % (base_url,orderid,'sr',s.name + '.tar.gz' )
        
        #s.download_url = ('%s/%s/%s/%s/%s/%s') % (base, u.getSensor(s.name),path,row,u.getYear(s.name),s.name + '-sr.tar.gz')
        #s.source_l1t_download_url = ('%s/%s/%s/%s/%s/%s') % (base,s.getSensor(),path,row,s.getYear(),s.name + '.tar.gz')
        
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
    isComplete = True
    for s in scenes:
        if s.status != 'complete' and s.status != 'unavailable':
            isComplete = False
            break
        
    #iterate over all the orders tied to this scene
    #for sceneorders in scene.sceneorder_set.all():
    #    order = sceneorders.order

    #    isComplete = True
        #now get each scene for the listed orders.  If the order is now complete, send completion email

    #    for ots in order.sceneorder_set.all():
    #        scene = ots.scene
            
    #        if scene.status != 'Complete' and scene.status != 'Unavailable':
    #            isComplete = False
    #            break

        #send email for this order if it's ready to go
    if isComplete:
        scene_names = [s.name for s in scenes if s.status != 'unavailable']
        o.status = 'complete'
        o.completion_date = datetime.now()
        o.save()
        sendCompletionEmail(o.email,o.orderid,readyscenes=scene_names)
        #scene_names = []
        #for so in order.sceneorder_set.all():
        #    if so.scene.status != 'Unavailable':
        #        scene_names.append(so.scene.name)
                       
        
        

    

    


