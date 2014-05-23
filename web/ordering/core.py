'''
core.py
Purpose: Holds common logic needed between views.py and api.py
Original Author: David V. Hill
'''

from email.mime.text import MIMEText
from smtplib import *
from models import Scene
from models import Order
from models import Configuration
from django.contrib.auth.models import User
from django.db.models import Q
import json
import datetime
import lta
import re
import xmlrpclib
import urllib2


#load configuration values at the module level...
try:

    smtp_url = Configuration().getValue('smtp.url')
    espa_email_address = Configuration().getValue('espa.email.address')
    order_status_base_url = Configuration().getValue('order.status.base.url')
except Exception, err:
    print ("Could not load configuration values:%s" % err)


def is_number(s):
    '''Determines if a string value is a float or int.

    Keyword args:
    s -- A string possibly containing a float or int

    Return:
    True if s is a float or int
    False if s is not a float or int
    '''
    try:
        float(s)
        return True
    except ValueError:
        return False


def validate_email(email):
    '''Compares incoming email address against regular expression to make sure
    its at least formatted like an email

    Keyword args:
    email -- String to validate as an email address

    Return:
    True if the string is a properly formatted email address
    False if not
    '''
    pattern = '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}$'
    return re.match(pattern, email.strip())


# Runs a query against the database to list all orders for a given email
def list_all_orders(email):
    '''lists out all orders for a given user'''
    #TODO: Modify this query to remove reference to Order.email once all
    # pre-espa-2.3.0 orders (EE Auth) are out of the system
    o = Order.objects.filter(
        Q(email=email) | Q(user__email=email)
        ).order_by('-order_date')
    #return Order.objects.filter(email=email).order_by('-order_date')
    return o


#  Runs a database query to return all the scenes + status for a given order
def get_order_details(orderid):
    '''Returns the full order and all attached scenes'''
    order = Order.objects.get(orderid=orderid)
    scenes = Scene.objects.filter(order__orderid=orderid)
    return order, scenes


# Captures a new order and gets it into the database
def enter_new_order(username,
                    order_source,
                    scene_list,
                    option_string,
                    note=''):
    '''Places a new espa order in the database

    Keyword args:
    username -- Username of user placing this order
    order_source -- Should always be 'espa'
    scene_list -- A list containing scene ids
    option_string -- Dictionary of options for the order
    note -- Optional user supplied note

    Return:
    The fully populated Order object
    '''

    # find the user
    user = User.objects.get(username=username)

    # create the order
    order = Order()
    order.orderid = Order.generate_order_id(user.email)
    order.user = user
    order.note = note
    order.status = 'ordered'
    order.order_date = datetime.datetime.now()
    order.product_options = option_string
    order.order_source = order_source
    order.order_type = 'level2_ondemand'
    order.save()

    # save the scenes for the order
    for s in set(scene_list):
        scene = Scene()
        scene.name = s
        scene.order = order
        scene.order_date = datetime.datetime.now()
        scene.status = 'submitted'
        scene.save()

    return order


def send_email(recipient, subject, body):

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['To'] = recipient
    msg['From'] = 'espa@usgs.gov'
    s = SMTP(host='gssdsflh01.cr.usgs.gov')
    s.sendmail('espa@usgs.gov', msg['To'], msg.as_string())
    s.quit()


# Sends the order submission confirmation email
def send_initial_email(order):

    status_base_url = Configuration().getValue('espa.status.url')

    status_url = ('%s/%s') % (status_base_url, order.email)

    header = ("""Thank you for your order ( %s ).  Your order has been received and is currently being processed.

You will receive an email notification when all units on this order have been completed.

You can check the status of your order and download already completed scenes directly from %s

Requested scenes:\n""") % (order.orderid, status_url)

    scenes = Scene.objects.filter(order__id=order.id)
    msg = header

    if scenes:
        for s in scenes:
            msg = msg + s.name + '\n'

    send_email(recipient=order.user.email,
               subject='Processing Order Received',
               body=msg)

    #configure all these values
    #msg = MIMEText(ordered)
    #msg['Subject'] = 'Processing order received.'
    #msg['To'] = order.email
    #msg['From'] = 'espa@usgs.gov'
    #s = SMTP(host='gssdsflh01.cr.usgs.gov')
    #s.sendmail('espa@usgs.gov', order.email, msg.as_string())
    #s.quit()


def send_completion_email(email, ordernum, readyscenes=[]):

    status_base_url = Configuration().getValue('espa.status.url')

    status_url = ('%s/%s') % (status_base_url, email)

    msg = ("""Your order is now complete and can be downloaded from %s

This order will remain available for 14 days.  Any data not downloaded will need to be reordered after this time.

Please contact Customer Services at 1-800-252-4547 or email custserv@usgs.gov with any questions.

Your scenes
-------------------------------------------\n""") % (status_url)

    for r in readyscenes:
        msg = msg + r + '\n'

    send_email(recipient=email,
               subject='Processing for %s Complete' % ordernum,
               body=msg)

    #configure these values
    #msg = MIMEText(msg)
    #msg['Subject'] = 'Processing for %s complete.' % (ordernum)
    #msg['To'] = email
    #msg['From'] = 'espa@usgs.gov'
    #s = SMTP(host='gssdsflh01.cr.usgs.gov')
    #s.sendmail('espa@usgs.gov', email, msg.as_string())
    #s.quit()


def getSceneInputPath(sceneid):
    '''Returns the location on the online cache where a scene
    does/should reside

    Keyword args:
    sceneid -- The scene name

    Return:
    Path on disk where the scene should be located if it exists
    '''
    scene = Scene.objects.get(name=sceneid)
    return scene.getOnlineCachePath()


def scenecache_is_alive(url='http://edclpdsftp.cr.usgs.gov:50000/RPC2'):
    """Determine if the specified url has an http server
    that accepts POST calls

    Keyword args:
    url -- The url of the server to check

    Return:
    True -- If the contacted server is alive and accepts POST calls
    False -- If the server does not accept POST calls or the
             server could not be contacted
    """

    try:
        return urllib2.urlopen(url, data="").getcode() == 200
    except Exception, e:
        print e
        return False


def get_xmlrpc_proxy():
    """Return an xmlrpc proxy to the caller for the scene cache

    Returns -- An xmlrpclib ServerProxy object
    """
    url = 'http://edclpdsftp.cr.usgs.gov:50000/RPC2'
    #url = os.environ['ESPA_SCENECACHE_URL']
    if scenecache_is_alive(url):
        return xmlrpclib.ServerProxy(url)
    else:
        msg = "Could not contact scene_cache at %s" % url
        raise RuntimeError(msg)


def scenes_on_cache(scenelist):
    """Proxy method call to determine if the scenes in question are on disk

    Keyword args:
    scenelist -- A Python list of scene identifiers

    Returns:
    A subset of scene identifiers
    """
    return get_xmlrpc_proxy().scenes_exist(scenelist)


def scenes_are_nlaps(scenelist):
    """Proxy method call to determine if the scenes are nlaps scenes

    Keyword args:
    scenelist -- A Python list of scene identifiers

    Return:
    A subset of scene identifiers
    """
    return get_xmlrpc_proxy().is_nlaps(scenelist)


def get_scenes_to_process():
    #sanity checks

    #load up any orders from ee that are waiting for us.
    load_ee_orders()

    #are there even any scenes to handle?
    statuses = ['submitted', 'onorder', 'oncache']
    if Scene.objects.filter(status__in=statuses).count() <= 0:
        return []

    #is cache online?
    if not scenecache_is_alive():
        print("Could not contact the scene cache...")
        raise Exception("Could not contact the scene cache...")

    #the cache is online and there are scenes to process...

    #get all the scenes that are in submitted status
    submitted = Scene.objects.filter(status='submitted')[:500]

    if submitted:

        #check to see which ones are sitting on cache
        submitted_list = [s.name for s in submitted]

         #check to see if they are NLAPS scenes first!!!
        nlaps_scenes = scenes_are_nlaps(submitted_list)

        for s in submitted:
            if s.name in nlaps_scenes:
                s.status = 'unavailable'
                s.note = 'TMA data cannot be processed'
                s.save()

        oncache = scenes_on_cache(submitted_list)

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
            #TODO -- Change this to use the OrderWrapperService
            tram_order_id = lta.LtaServices().order_scenes(need_to_order)
            #something went wrong
            if tram_order_id == -1:
                raise Exception("Could not order scenes from TRAM!")

            for to in need_to_order:
                to.tram_order_id = tram_order_id
                to.status = 'onorder'
                to.save()

    #get all the scenes that are on order and check to see if they are on cache
    ordered = Scene.objects.filter(status='onorder')

    if ordered:
        ordered_list = [s.name for s in ordered]
        #oncache2 = cache.has_scenes(ordered_list)
        oncache2 = scenes_on_cache(ordered_list)

        #change status to oncache for the ones that were found
        for s in ordered:
            if s.name in oncache2:
                s.status = 'oncache'
                s.save()

    #don't do anything with the ones that weren't oncache.
    #They remain on order.
    #
    #Now the database should be fully updated
    #with the current status.
    #
    #Pull the current oncache set from the db
    #and include it as the result
    results = []
    available_scenes = Scene.objects.filter(status='oncache')[:500]
    if available_scenes:
        for a in available_scenes:
            order = a.order
            options = order.product_options
            options = options.replace("\\", "")
            oid = order.orderid

            orderline = json.dumps(
                {'orderid': oid, 'scene': a.name, 'options': options})

            results.append(orderline)

    return results


'''
def purge_expired_orders():
    config = None
    username  = None
    password = None
    host = None
    port = None
    ds = None
    orders = None

    try:
        cutoff = datetime.datetime.now() - timedelta(days=14)
        #get orders where status == complete and
        #that were completed > than 14 days ago
        orders = Order.objects.raw('select * from ordering_order oo \
        where oo.id not in \
        (select order_id from ordering_scene where status in \
        ("queued","onorder","processing","distributing","oncache","purged"))')

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
'''


# Simple logger method for this module
def helper_logger(msg):
    print(msg)
    #h = open('/tmp/helper.log', 'a+')
    #h.write(msg)
    #h.flush()
    #h.close()
    #pass


def update_status(name, orderid, processing_loc, status):

    helperlogger("Updating scene:%s order:%s from location:%s to %s\n"
                 % (name, orderid, processing_loc, status))

    try:
        s = Scene.objects.get(name=name, order__orderid=orderid)
        if s:
            helperlogger("Running update query for %s.  Setting status to:%s"
                         % (s.name, status))

            s.status = status
            s.processing_location = processing_loc
            s.log_file_contents = ""
            s.save()
            s = None
            return True
        else:
            helperlogger("Scene[%s] not found in order[%s]"
                         % (name, orderid))

            return False
    except Exception, e:
        helperlogger("Exception in updateStatus:%s" % e)


#  Marks a scene in error and accepts the log file contents
def set_scene_error(name, orderid, processing_loc, error):
    o = Order.objects.get(orderid=orderid)
    s = Scene.objects.get(name=name, order__id=o.id)
    if s:
        s.status = 'error'
        s.processing_location = processing_loc
        s.log_file_contents = error
        s.save()
        return True
    else:
        #something went wrong, don't clean up other disk.
        print("Scene[%s] not found in Order[%s]"
              % (name, orderid))

        return False


#  Marks a scene unavailable and stores a reason
def set_scene_unavailable(name, orderid, processing_loc, error, note):
    o = Order.objects.get(orderid=orderid)
    s = Scene.objects.get(name=name, order__id=o.id)
    if s:
        s.status = 'unavailable'
        s.processing_location = processing_loc
        s.completion_date = datetime.datetime.now()
        s.log_file_contents = error
        s.note = note
        s.save()

        if o.order_source == 'ee':
            #update ee
            ltasvc = lta.LtaServices()
            ltasvc.update_order(o.ee_order_id, s.ee_unit_id, 'R')

        #if there are no more inprocess scenes,
        #mark the order complete and send email
        update_order_if_complete(o.orderid, s.name)

        return True
    else:
        #something went wrong, don't clean up other disk.
        msg = "Scene[%s] not found in Order[%s]" \
              % (name, orderid)

        print(msg)

        return False


#  Marks a scene complete in the database for a given order
def mark_scene_complete(name,
                        orderid,
                        processing_loc,
                        completed_file_location,
                        destination_cksum_file=None,
                        log_file_contents=""):

    print ("Marking scene:%s complete for order:%s" % (name, orderid))
    o = Order.objects.get(orderid=orderid)
    s = Scene.objects.get(name=name, order__id=o.id)
    if s:
        s.status = 'complete'
        s.processing_location = processing_loc
        s.product_distro_location = completed_file_location
        s.completion_date = datetime.datetime.now()
        s.cksum_distro_location = destination_cksum_file

        s.log_file_contents = log_file_contents

        base_url = Configuration().getValue('distribution.cache.home.url')

        product_file_parts = completed_file_location.split('/')
        product_file = product_file_parts[len(product_file_parts) - 1]
        cksum_file_parts = destination_cksum_file.split('/')
        cksum_file = cksum_file_parts[len(cksum_file_parts) - 1]

        s.product_dload_url = ('%s/orders/%s/%s') % \
                              (base_url, orderid, product_file)

        s.cksum_download_url = ('%s/orders/%s/%s') % \
                               (base_url, orderid, cksum_file)

        s.save()

        if o.order_source == 'ee':
            #update ee
            ltasvc = lta.LtaServices()
            ltasvc.update_order(o.ee_order_id, s.ee_unit_id, 'C')

        update_order_if_complete(o.orderid, s)

        return True
    else:
        print("MarkSceneComplete:No scene was found with the name:%s" % name)
        return False


def update_order_if_complete(orderid, scene):
    '''Method to send out the order completion email
    for orders if the completion of a scene
    completes the order

    Keyword args:
    orderid -- id of the order
    scene -- scene name

    '''
    o = Order.objects.get(orderid=orderid)
    scenes = Scene.objects.filter(order__id=o.id)

    #we have to do this because we need to see if all scenes
    #for the given order are complete. Don't know how to
    #run that query through the Django Model interface.
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

        #only send the email if this was an espa order.
        if o.order_source == 'espa':
            sendCompletionEmail(o.email, o.orderid, readyscenes=scene_names)


def load_ee_orders():
    ''' Loads all the available orders from lta into
    our database and updates their status
    '''
    ltasvc = lta.LtaServices()

    #This returns a dict that contains a list of dicts{}
    #key:(order_num,email) = list({sceneid:, unit_num:})
    orders = ltasvc.get_available_orders()

    #use this to cache calls to EE Registration Service username lookups
    local_cache = {}

    #Capture in our db
    for eeorder, email, contactid in orders:

        #create the orderid based on the info from the eeorder
        order_id = Order.generate_ee_order_id(email, eeorder)

        # paranoia... initialize this to None since its used in the loop.
        order = None

        #go look to see if it already exists in the db
        try:
            order = Order.objects.get(orderid=order_id)
        except Order.DoesNotExist:

            reg = lta.RegistrationServiceClient()

            # retrieve the username from the EE registration service
            # cache this call
            if contactid in local_cache:
                username = local_cache[contactid]
            else:
                username = reg.get_username(contactid)
                local_cache[contactid] = username

            #now look the user up in our db.  Create if it doesn't exist
            # we'll want to put some caching in place here too
            try:
                user = User.objects.get(username=username)

                # make sure the email we have on file is current
                if not user.email or user.email is not email:
                    user.email = email
                    user.save()
            except User.DoesNotExist:
                # Create a new user. Note that we can set password
                # to anything, because it won't be checked; the password
                # from RegistrationServiceClient will.
                user = User(username=username, password='this isnt used')
                user.is_staff = False
                user.is_superuser = False
                user.email = email
                user.save()

                UserProfile(contactid=contactid, user=user).save()

            # We have a user now.  Now build the new Order since it
            # wasn't found.
            #Didn't find it in the db... make the order now
            order = Order()
            order.orderid = order_id
            order.user = user
            order.order_type = 'level2_ondemand'
            order.status = 'ordered'
            order.note = 'EarthExplorer order id: %s' % eeorder
            order.product_options = json.dumps(Order.get_default_ee_options)
            order.ee_order_id = eeorder
            order.order_source = 'ee'
            order.order_date = datetime.datetime.now()
            order.save()

        for s in orders[eeorder, email]:
            #go look for the scene by ee_unit_id.  This will stop
            #duplicate key update collisions

            scene = None
            try:
                scene = Scene.objects.get(order=order,
                                          ee_unit_id=s['unit_num'])

                if scene.status == 'complete':

                    success, msg, status = ltasvc.update_order(eeorder,
                                                               s['unit_num'],
                                                               "C")
                    if not success:
                        log_msg = "Error updating lta for \
                        [eeorder:%s ee_unit_num:%s \
                        scene name:%s order:%s" \
                        % (eeorder, s['unit_num'], scene.name, order.orderid)

                        helperlogger(log_msg)

                        log_msg = "Error detail: \
                        lta return message:%s  lta return \
                        status code:%s" % (msg, status)

                        helperlogger(log_msg)
                elif scene.status == 'unavailable':
                    success, msg, status = ltasvc.update_order(eeorder,
                                                               s['unit_num'],
                                                               "R")

                    if not success:
                        log_msg = "Error updating lta for \
                        [eeorder:%s ee_unit_num:%s \
                        scene name:%s order:%s" \
                        % (eeorder, s['unit_num'], scene.name, order.orderid)

                        helperlogger(log_msg)

                        log_msg = "Error detail: \
                        lta return message:%s  lta return \
                        status code:%s" % (msg, status)

                        helperlogger(log_msg)
            except Scene.DoesNotExist:
                scene = Scene()
                scene.name = s['sceneid']
                scene.ee_unit_id = s['unit_num']
                scene.order = order
                scene.order_date = datetime.datetime.now()
                scene.status = 'submitted'
                scene.save()

            #Update LTA
            success, msg, status = ltasvc.update_order(eeorder,
                                                       s['unit_num'],
                                                       "I")
            if not success:
                log_msg = "Error updating lta for \
                [eeorder:%s ee_unit_num:%s scene \
                name:%s order:%s" % (eeorder, s['unit_num'],
                                     scene.name,
                                     order.orderid)

                helperlogger(log_msg)

                log_msg = "Error detail: lta return message:%s  \
                lta return status code:%s" % (msg, status)

                helperlogger(log_msg)
