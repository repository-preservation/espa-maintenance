from email.mime.text import MIMEText
from smtplib import *
from datetime import datetime
from suds.client import Client
from suds import null
from models import Scene,Order,Configuration
from datetime import datetime,timedelta


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
        
    orderedscenes = [scene.name for scene in order.scenes.all() if scene.status == 'On Order']
    readyscenes = [scene.name for scene in order.scenes.all() if scene.status != 'On Order' and scene.status != 'Unavailable']
    unavailablescenes = [scene.name for scene in order.scenes.all() if scene.status == 'Unavailable']

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
    '''
    if orderedscenes and readyscenes:	
	ordered = header + onorder	
	for o in orderedscenes:
            ordered = ordered + o + '\n'
	ordered = ordered + ready	
	for r in readyscenes:
            ordered = ordered + r + '\n'
	
    elif orderedscenes and not readyscenes:	
	ordered = header + onorder
	for o in orderedscenes:
            ordered = ordered + o + '\n'
    elif not orderedscenes and readyscenes:	
	ordered = header + ready
	for r in readyscenes:
            ordered = ordered + r + '\n'
    else:
	pass
    '''

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
    msg = ("""You're order for atmospherically corrected scenes is now complete. All scenes will remain available for 14 days.\n
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
    scenes = Scene.objects.filter(status = 'On Order')
    print("Get scenes To Process")
    results = []
    
    if scenes:
        for s in scenes:
            if s.isOnCache() and s.status == 'On Order':
                s.status = 'On Cache'
                s.save()
    
    scenes2 = Scene.objects.filter(status = 'On Cache')

    if scenes2:    
        for ss in scenes2:
            results.append(ss.name)

    return results


def getScenesToPurge():
        results = []
        scenes = Scene.objects.filter(status = 'Complete')
        cutoff = datetime.now() - timedelta(days=14)
        for s in scenes:
            if (s.completion_date):
                diff = cutoff - s.completion_date
                if diff.days >= 0:
                    results.append(s.distribution_location)        
        return results
    
def updateStatus(name, status):
    s = Scene.objects.get(name=name)
    if s:
        #if this is being marked as queued, wipe out the log file
        #field so we can add the new one
        if status == 'Queued':
            s.log_file_contents = ''
                
        s.status = status
        s.save()
        return True
    else:
        #something went wrong, don't clean up other disk.
        print("UpdateStatus:No scene was found with the name:%s" % name)
        return False

def markSceneComplete(name,completed_file_location, source_l1t_location,log_file_contents):
    s = Scene.objects.get(name=name)
    if s:
        s.status = 'Complete'
        s.distribution_location = completed_file_location
        s.completion_date = datetime.now()
        s.source_l1t_distro_location = source_l1t_location
        s.log_file_contents = log_file_contents
        path = s.getPath()

        #strip leading 0 off of path and row if it exists
        if str(path).startswith('0'):
            path = str(path)[1:len(str(path))]

        row = s.getRow()
        if str(row).startswith('0'):
            row = str(row)[1:len(str(row))]
        
        #Need to modify this as soon as we're going to start
        #providing more than 1 product
        base_url = Configuration().getValue('distribution.cache.home.url')
        sr_path = Configuration().getValue('distribution.cache.sr.path')

        #This will always place the source right next to the generated product.    
        base = base_url + '/' + sr_path
        
        s.download_url = ('%s/%s/%s/%s/%s/%s') % (base, s.getSensor(),path,row,s.getYear(),s.name + '-SR.tar.gz')
        s.source_l1t_download_url = ('%s/%s/%s/%s/%s/%s') % (base,s.getSensor(),path,row,s.getYear(),s.name + '.tar.gz')
        s.save()

        sendEmailIfComplete(s)
            
        return True
    else:
        print("MarkSceneComplete:No scene was found with the name:%s" % name)
        return False


def sendEmailIfComplete(scene):
    '''Method to send out the order completion email for orders if the completion of a scene completes the order'''    
    
    #iterate over all the orders tied to this scene
    for sceneorders in scene.sceneorder_set.all():
        order = sceneorders.order

        isComplete = True
        #now get each scene for the listed orders.  If the order is now complete, send completion email

        for ots in order.sceneorder_set.all():
            scene = ots.scene
            
            if scene.status != 'Complete' and scene.status != 'Unavailable':
                isComplete = False
                break

        #send email for this order if it's ready to go
        if isComplete:
            scene_names = []
            for so in order.sceneorder_set.all():
                if so.scene.status != 'Unavailable':
                    scene_names.append(so.scene.name)
                       
            sendCompletionEmail(order.email,order.orderid,readyscenes=scene_names)

    


                                         
