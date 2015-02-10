'''
core.py
Purpose: Holds common logic needed between views.py and api.py
Original Author: David V. Hill
'''

import models
from models import Scene
from models import Order
from models import Configuration
from models import UserProfile
from django.contrib.auth.models import User
from django.conf import settings
from django.db import transaction
import json
import datetime
import urllib
import lta
import lpdaac
import errors
import espa_common



class Emails(object):

    def __init__(self):
        self.status_base_url = Configuration().getValue('espa.status.url')

    def __send(self, recipient, subject, body):
        return espa_common.utilities.send_email(recipient=recipient,
                                                subject=subject,
                                                body=body)

    def __order_status_url(self, email):
        return ''.join([self.status_base_url, '/', email])

    @transaction.atomic
    def send_all_initial(self):
        '''Finds all the orders that have not had their initial emails sent and
        sends them'''

        orders = Order.objects.filter(status='ordered')
        for o in orders:
            if not o.initial_email_sent:
                self.send_initial(o)
                o.initial_email_sent = datetime.datetime.now()
                o.save()

    def send_initial(self, order):

        if isinstance(order, str):
            order = Order.objects.get(orderid=order)
        elif isinstance(order, int):
            order = Order.objects.get(id=order)

        if not isinstance(order, models.Order):
            msg = 'order must be str, int or instance of models.Order'
            raise TypeError(msg)

        email = order.user.email
        url = self.__order_status_url(email)

        m = list()
        m.append("Thank you for your order.\n\n")
        m.append("%s has been received and is currently " % order.orderid)
        m.append("being processed.  ")
        m.append("Another email will be sent when this order is complete.\n\n")
        m.append("You may view the status of your order and download ")
        m.append("completed products directly from %s\n\n" % url)
        m.append("Requested products\n")
        m.append("-------------------------------------------\n")

        #scenes = Scene.objects.filter(order__id=order.id)

        products = order.scene_set.all()

        for product in products:
            name = product.name

            if name == 'plot':
                name = "Plotting & Statistics"
            m.append("%s\n" % name)

        email_msg = ''.join(m)
        subject = 'Processing order %s received' % order.orderid

        return self.__send(recipient=email, subject=subject, body=email_msg)

    def send_completion(self, order):

        if isinstance(order, str):
            order = Order.objects.get(orderid=order)
        elif isinstance(order, int):
            order = Order.objects.get(id=order)

        if not isinstance(order, models.Order):
            msg = 'order must be str, int or instance of models.Order'
            raise TypeError(msg)

        email = order.user.email
        url = self.__order_status_url(email)

        m = list()
        m.append("%s is now complete and can be downloaded " % order.orderid)
        m.append("from %s.\n\n" % url)
        m.append("This order will remain available for 14 days.  ")
        m.append("Any data not downloaded will need to be reordered ")
        m.append("after this time.\n\n")
        m.append("Please contact Customer Services at 1-800-252-4547 or ")
        m.append("email custserv@usgs.gov with any questions.\n\n")
        m.append("Requested products\n")
        m.append("-------------------------------------------\n")

        products = order.scene_set.filter(status='complete')

        for product in products:
            line = product.name
            if line == 'plot':
                line = "Plotting & Statistics"

            m.append("%s\n" % line)

        body = ''.join(m)
        subject = 'Processing for %s complete.' % order.orderid

        return self.__send(recipient=email, subject=subject, body=body)

'''
TODO -- Create new method handle_submitted_scenes() or something to that effect
_process down to this comment should be included in it.

The rest of this method down should actually be 'get_scenes_to_process()'

TODO -- renamed this module 'actions.py'
TODO -- OO'ize the order handling into OrderHandler()
TODO -- Encapsulate all models.py classes here... don't let them flow
TODO --     up into the callers of this module.
TODO -- OrderHandler().get_scenes_to_process()
TODO -- OrderHandler().determine_disposition()
TODO -- OrderHandler().cancel(Order())
TODO -- OrderHandler().cancel(Order(), ProductSensor())
TODO -- OrderHandler().cleanup(Order())
TODO -- OrderHandler().status(Order())
TODO -- OrderHandler().status(Order(), ProductSensor())

TODO -- Build HadoopHandler() as well.
TODO -- HadoopHandler().cluster_status()
TODO -- HadoopHandler().cancel_job(jobid)
'''


class ProductHandler(object):

    def check_ordered(self):
        pass

    def accept_submitted(self):
        pass

    def move_to_queued(self):
        pass

    def move_to_complete(self):
        pass

    def move_to_unavailable(self):
        pass

    def move_to_error(self):
        pass

    def move_to_retry(self):
        pass

    def move_to_submitted(self):
        pass

    def are_oncache(self):
        pass


class LandsatProductHandler(object):

    def __init__(self, *args, **kwargs):
        super(LandsatProductHandler, self).__init__(*args, **kwargs)

    def check_ordered(self):
        pass

    def accept_submitted(self):
        pass


class ModisProductHandler(object):

    def __init__(self, *args, **kwargs):
        super(ModisProductHandler, self).__init__(*args, **kwargs)


class OrderHandler(object):

    def __init__(self):
        pass

    def all_products_complete(self, order):
        pass

    def cancel(self, order):
        pass

    def status(self, order):
        pass

    def details(self, order):
        pass

    def cleanup(self, order):
        pass

    def finalize_all(self):
        pass

    def load_ee(self):
        pass


@transaction.atomic
def set_product_retry(name,
                      orderid,
                      processing_loc,
                      error,
                      note,
                      retry_after,
                      retry_limit=None):
    '''Sets a product to retry status'''

    product = Scene.objects.get(name=name, order__orderid=orderid)

    #if a new retry limit has been provided, update the db and use it
    if retry_limit is not None:
        product.retry_limit = retry_limit

    if product.retry_count + 1 <= product.retry_limit:
        product.status = 'retry'
        product.retry_count = product.retry_count + 1
        product.retry_after = retry_after
        product.log_file_contents = error
        product.processing_loc = processing_loc
        product.note = note
        product.save()
    else:
        raise Exception("Retry limit exceeded")


@transaction.atomic
#  Marks a scene unavailable and stores a reason
def set_product_unavailable(name, orderid, processing_loc, error, note):

    product = Scene.objects.get(name=name, order__orderid=orderid)

    product.status = 'unavailable'
    product.processing_location = processing_loc
    product.completion_date = datetime.datetime.now()
    product.log_file_contents = error
    product.note = note
    product.save()

    if product.order.order_source == 'ee':
        #update ee
        lta.update_order_status(product.order.ee_order_id,
                                product.ee_unit_id, 'R')

    return True


def set_products_unavailable(products, reason):
    '''Bulk updates products to unavailable status and updates EE if
    necessary.

    Keyword args:
    products - A list of models.Scene objects
    reason - The user facing reason the product was rejected
    '''
    for p in products:
        if not isinstance(p, Scene):
            raise TypeError()

    for p in products:
        p.status = 'unavailable'
        p.completion_date = datetime.datetime.now()
        p.note = reason
        p.save()

        if p.order.order_source == 'ee':
            lta.update_order_status(p.order.ee_order_id, p.ee_unit_id, 'R')


@transaction.atomic
def handle_retry_products():
    now = datetime.datetime.now()

    filter_args = {'status': 'retry',
                   'retry_after__lt': now}

    update_args = {'status': 'submitted',
                   'note': ''}

    Scene.objects.filter(**filter_args).update(**update_args)


@transaction.atomic
def handle_onorder_landsat_products():

    filters = {
        'tram_order_id__isnull': False,
        'status': 'onorder'
    }

    select_related = 'order'

    products = Scene.objects.filter(**filters).select_related(select_related)
    product_tram_ids = products.values_list('tram_order_id')
    tram_ids = list(set([p[0] for p in product_tram_ids]))

    rejected = []
    available = []

    for tid in tram_ids:
        order_status = lta.get_order_status(tid)

        # There are a variety of product statuses that come back from tram
        # on this call.  I is inprocess, Q is queued for the backend system,
        # D is duplicate, C is complete and R is rejected.  We are ignoring
        # all the statuses except for R and C because we don't care.
        # In the case of D (duplicates), when the first product completes, all
        # duplicates will also be marked C
        for unit in order_status['units']:
            if unit['unit_status'] == 'R':
                rejected.append(unit['sceneid'])
            elif unit['unit_status'] == 'C':
                available.append(unit['sceneid'])

    #Go find all the tram units that were rejected and mark them
    #unavailable in our database.  Note that we are not looking for
    #specific tram_order_id/sceneids as duplicate tram orders may have been
    #submitted and we want to bulk update all scenes that are onorder but
    #have been rejected
    if len(rejected) > 0:
        rejected_products = [p for p in products if p.name in rejected]
        set_products_unavailable(rejected_products,
                                 'Level 1 product could not be produced')

    #Now update everything that is now on cache
    filters = {
        'status': 'onorder',
        'name__in': available
    }
    updates = {
        'status': 'oncache',
    }

    if len(available) > 0:
        Scene.objects.filter(**filters).update(**updates)


def handle_submitted_landsat_products():

    @transaction.atomic
    def mark_nlaps_unavailable():

        print("In mark_nlaps_unavailable")

        #First things first... filter out all the nlaps scenes
        filters = {
            'status': 'submitted',
            'sensor_type': 'landsat'
        }

        print("looking for submitted landsat products")
        landsat_products = Scene.objects.filter(**filters)

        #build list input for calls to the scene cache
        landsat_submitted = [l.name for l in landsat_products]

        print("Checking for nlaps products now")

        # find all the submitted products that are nlaps and reject them
        landsat_nlaps = espa_common.nlaps.products_are_nlaps(landsat_submitted)

        landsat_submitted = None

        print("Found %i landsat nlaps products" % len(landsat_nlaps))

        # bulk update the nlaps scenes
        if len(landsat_nlaps) > 0:

            nlaps = [p for p in landsat_products if p.name in landsat_nlaps]

            landsat_nlaps = None

            set_products_unavailable(nlaps, 'TMA data cannot be processed')

    def get_contactids_for_submitted_landsat_products():

        print("In get_contactids_for_submitted_landsat_products")

        filters = {
            'order__scene__status': 'submitted',
            'order__scene__sensor_type': 'landsat'
        }
        u = User.objects.filter(**filters)
        u = u.select_related('userprofile__contactid')
        contact_ids = u.values_list('userprofile__contactid').distinct()

        print("contact ids:")
        print contact_ids

        return [c[0] for c in contact_ids]

    @transaction.atomic
    def update_landsat_product_status(contact_id):
      
        print("update_landsat_product_status")

        filters = {
            'status': 'submitted',
            'sensor_type': 'landsat',
            'order__user__userprofile__contactid': contact_id
        }

        #limit this to 500, 9000+ scenes are stressing EE
        products = Scene.objects.filter(**filters)[:500]
        product_list = [p.name for p in products]

        print("Ordering %s scenes for contact:%s" % (len(product_list), contact_id))

        results = lta.order_scenes(product_list, contact_id)

        print("Checking ordering results for contact:%s" % contact_id)

        if 'available' in results and len(results['available']) > 0:
            #update db
            filter_args = {
                'status': 'submitted',
                'name__in': results['available'],
                'sensor_type': 'landsat',
                'order__user__userprofile__contactid': contact_id
            }

            update_args = {'status': 'oncache'}

            Scene.objects.filter(**filter_args).update(**update_args)

        if 'ordered' in results and len(results['ordered']) > 0:
            #response = lta.order_scenes(orderable, contact_id)

            filter_args = {'status': 'submitted',
                           'name__in': results['ordered'],
                           'order__user__userprofile__contactid': contact_id}

            update_args = {'status': 'onorder',
                           'tram_order_id': results['lta_order_id']}

            Scene.objects.filter(**filter_args).update(**update_args)

        if 'invalid' in results and len(results['invalid']) > 0:
            #look to see if they are ee orders.  If true then update the
            #unit status

            invalid = [p for p in products if p.name in results['invalid']]

            set_products_unavailable(invalid, 'Not found in landsat archive')

    #Here's the real logic for this handling submitted landsat products
    mark_nlaps_unavailable()

    for contact_id in get_contactids_for_submitted_landsat_products():
        try:
            print("Updating landsat_product_status for %s" % contact_id)
            update_landsat_product_status(contact_id)
        except Exception, e:
            msg = '''Could not update_landsat_product_status for %s\n
                     Exception:%s''' % (contact_id, e)
            print(msg)



@transaction.atomic
def handle_submitted_modis_products():
    ''' Moves all submitted modis products to oncache if true '''

    print("handle_submitted_modis")

    filter_args = {'status': 'submitted', 'sensor_type': 'modis'}
    modis_products = Scene.objects.filter(**filter_args)

    if len(modis_products) > 0:

        oncache_list = list()

        for product in modis_products:
            if lpdaac.input_exists(product.name):
                oncache_list.append(product.name)

        filter_args = {'status': 'submitted',
                       'name__in': oncache_list,
                       'sensor_type': 'modis'}

        update_args = {'status': 'oncache'}

        Scene.objects.filter(**filter_args).update(**update_args)


@transaction.atomic
def handle_submitted_plot_products():
    ''' Moves plot products from submitted to oncache status once all
        their underlying rasters are complete or unavailable '''

    filter_args = {'status': 'ordered', 'order_type': 'lpcs'}
    plot_orders = Order.objects.filter(**filter_args)


    for order in plot_orders:
        product_count = order.scene_set.count()

        filter_args = {'status': 'complete'}
        complete_products = order.scene_set.filter(**filter_args).count()

        filter_args = {'status': 'unavailable'}
        unavailable_products = order.scene_set.filter(**filter_args).count()


        #if this is an lpcs order and there is only 1 product left that
        #is not done, it must be the plot product.  Will verify this
        #in next step.  Plotting cannot run unless everything else
        #is done.

        if product_count - (unavailable_products + complete_products) == 1:
            filter_args = {'status': 'submitted', 'sensor_type': 'plot'}
            plot = order.scene_set.filter(**filter_args)
            if len(plot) >= 1:
                for p in plot:
                    if complete_products == 0:
                        p.status = 'unavailable'
                        p.note = ('No input products were available for '
                                  'plotting and statistics')
                    else:
                        p.status = 'oncache'
                        p.note = ''
                    p.save()


@transaction.atomic
def handle_submitted_products():
    handle_submitted_landsat_products()
    handle_submitted_modis_products()
    handle_submitted_plot_products()


@transaction.atomic
def get_products_to_process(record_limit=500,
                            for_user=None,
                            priority=None,
                            product_types=['landsat', 'modis'],
                            encode_urls=False):
    '''Find scenes that are oncache and return them as properly formatted
    json per the interface description between the web and processing tier'''

    # cast the record_limit to int since that's how its being used:
    if record_limit is not None:
        record_limit = int(record_limit)

    # use kwargs so we can dynamically build the filter criteria
    filters = {
        'order__scene__status': 'oncache'
    }

    #optimize the query so it creates a join call rather than executing
    #multiple database calls for the related fields
    select_related = ('order__orderid,'
                      'order__priority,'
                      'order__product_options,'
                      'userprofile__contactid')

    # use orderby for the orderby clause
    orderby = 'order__order_date'

    if for_user is not None:
        # Find orders submitted by a specific user
        filters['username'] = for_user

    if priority is not None:
        # retrieve by specified priority
        filters['order__priority'] = priority

    if product_types is not None:
        #filter based on what user asked for... modis, landsat or plot
        filters['order__scene__sensor_type__in'] = product_types

    u = User.objects.filter(**filters)
    u = u.select_related(select_related).order_by(orderby)

    #pull all the cids but cast to set() to eliminate dups then back to list
    #to support index based iteration
    cids = list(set([c[0] for c in u.values_list('userprofile__contactid')]))

    results = []

    for cid in cids:

        if record_limit is not None and len(results) + 1 > record_limit:
            break

        filters = {
            'order__user__userprofile__contactid': cid,
            'status': 'oncache'
        }

        if priority is not None:
            filters['order__priority'] = priority

        select_related = 'order'

        orderby = 'order__order_date'

        scenes = Scene.objects.filter(**filters)
        scenes = scenes.select_related(select_related)

        if record_limit is not None:
            scenes = scenes.order_by(orderby)[:record_limit]
        else:
            scenes = scenes.order_by(orderby)

        #landsat = [s.name for s in scenes where s.sensor_type = 'landsat']
        landsat = [s.name for s in scenes if s.sensor_type == 'landsat']
        landsat_urls = lta.get_download_urls(landsat, cid)

        modis = [s.name for s in scenes if s.sensor_type == 'modis']
        modis_urls = lpdaac.get_download_urls(modis)

        for scene in scenes:

            if record_limit is not None and len(results) + 1 > record_limit:
                break

            dload_url = None

            if scene.sensor_type == 'landsat':

                if ('status' in landsat_urls[scene.name] and
                    landsat_urls[scene.name]['status'] != 'available'):
                        try:
                            lookup = espa_common.settings.RETRY
                            limit = lookup['retry_missing_l1']['retry_limit']
                            timeout = lookup['retry_missing_l1']['timeout']
                            ts = datetime.datetime.now()
                            after = ts + datetime.timedelta(seconds=timeout)

                            set_product_retry(scene.name,
                                              scene.order.orderid,
                                              'get_products_to_process',
                                              'product was not available',
                                              'reordering missing level 1 product',
                                              after, limit)
                        except:
                            set_product_error(scene.name, scene.order.order,
                                              'get_products_to_process',
                                              ('level1 product data '
                                              'not available after EE call '
                                              'marked product as available'))
                        continue


                if 'download_url' in landsat_urls[scene.name]:
                    dload_url = landsat_urls[scene.name]['download_url']
                    if encode_urls:
                        dload_url = urllib.quote(dload_url, '')

            elif scene.sensor_type == 'modis':
                if 'download_url' in modis_urls[scene.name]:
                    dload_url = modis_urls[scene.name]['download_url']

                    if encode_urls:
                        dload_url = urllib.quote(dload_url, '')

            result = {
                'orderid': scene.order.orderid,
                'product_type': scene.sensor_type,
                'scene': scene.name,
                'priority': scene.order.priority,
                'options': json.loads(scene.order.product_options)
            }

            if scene.sensor_type == 'plot':
                results.append(result)
            elif dload_url is not None:
                result['download_url'] = dload_url
                results.append(result)
            else:
                print("dload_url for %s:%s was None, skipping..."
                    % (scene.order.id, scene.name))

    return results


# Simple logger method for this module
def helper_logger(msg):
    print(msg)


@transaction.atomic
def update_status(name, orderid, processing_loc, status):

    product = Scene.objects.get(name=name, order__orderid=orderid)

    product.status = status
    product.processing_location = processing_loc
    product.log_file_contents = ""
    product.save()

    return True


@transaction.atomic
#  Marks a scene in error and accepts the log file contents
def set_product_error(name, orderid, processing_loc, error):

    product = Scene.objects.get(name=name, order__orderid=orderid)

    #attempt to determine the disposition of this error
    resolution = errors.resolve(error)

    if resolution is not None:

        if resolution.status == 'submitted':
            product.status = 'submitted'
            product.note = ''
            product.save()
        elif resolution.status == 'unavailable':
            set_product_unavailable(product.name,
                                    product.order.orderid,
                                    processing_loc,
                                    error,
                                    resolution.reason)
        elif resolution.status == 'retry':
            try:
                set_product_retry(product.name,
                                  product.order.orderid,
                                  processing_loc,
                                  error,
                                  resolution.reason,
                                  resolution.extra['retry_after'],
                                  resolution.extra['retry_limit'])
            except Exception, e:
                print("Exception setting %s to retry:%s" % (name, e))
                product.status = 'error'
                product.processing_location = processing_loc
                product.log_file_contents = error
                product.save()

                if settings.DEBUG:
                    print("Exception setting %s to retry:%s" % (name, e))
    else:
        product.status = 'error'
        product.processing_location = processing_loc
        product.log_file_contents = error
        product.save()

    return True


@transaction.atomic
def queue_products(order_name_tuple_list, processing_location, job_name):
    ''' Allows the caller to place products into queued status in bulk '''

    if not isinstance(order_name_tuple_list, list):
        msg = list()
        msg.append("queue_products expects a list of ")
        msg.append("tuples(order_id, product_id) for the first argument")
        raise TypeError(''.join(msg))

    # this should be a dictionary of lists, with order as the key and
    # the scenes added to the list
    orders = {}

    for order, product_name in order_name_tuple_list:
        if not order in orders:
            orders[order] = list()
        orders[order].append(product_name)

    # now use the orders dict we built to update the db
    for order in orders:
        products = orders[order]

        filter_args = {'name__in': products, 'order__orderid': order}

        update_args = {'status': 'queued',
                       'processing_location': processing_location,
                       'log_file_contents': '',
                       'job_name': job_name}

        #helper_logger("Queuing %s:%s from %s for job %s"
        #              % (order, products, processing_location, job_name))

        Scene.objects.filter(**filter_args).update(**update_args)

    return True


@transaction.atomic
#  Marks a scene complete in the database for a given order
def mark_product_complete(name,
                          orderid,
                          processing_loc,
                          completed_file_location,
                          destination_cksum_file=None,
                          log_file_contents=""):

    print ("Marking scene:%s complete for order:%s" % (name, orderid))
    product = Scene.objects.get(name=name, order__orderid=orderid)

    product.status = 'complete'
    product.processing_location = processing_loc
    product.product_distro_location = completed_file_location
    product.completion_date = datetime.datetime.now()
    product.cksum_distro_location = destination_cksum_file

    product.log_file_contents = log_file_contents

    base_url = Configuration().getValue('distribution.cache.home.url')

    product_file_parts = completed_file_location.split('/')
    product_file = product_file_parts[len(product_file_parts) - 1]
    cksum_file_parts = destination_cksum_file.split('/')
    cksum_file = cksum_file_parts[len(cksum_file_parts) - 1]

    product.product_dload_url = ('%s/orders/%s/%s') % \
                                (base_url, orderid, product_file)

    product.cksum_download_url = ('%s/orders/%s/%s') % \
                                 (base_url, orderid, cksum_file)

    product.save()

    if product.order.order_source == 'ee':
        #update ee
        lta.update_order_status(product.order.ee_order_id,
                                product.ee_unit_id, 'C')
        return True


@transaction.atomic
def update_order_if_complete(order):
    '''Method to send out the order completion email
    for orders if the completion of a scene
    completes the order

    Keyword args:
    orderid -- id of the order

    '''
    complete_scene_status = ['complete', 'unavailable']

    if type(order) == str:
        #will raise Order.DoesNotExist
        order = Order.objects.get(orderid=order)
    elif type(order) == int:
        order = Order.objects.get(id=order)

    if not type(order) == Order:
        msg = "%s must be of type models.ordering.Order, int or str" % order
        raise TypeError(msg)

    # find all scenes that are not complete
    scenes = order.scene_set.exclude(status__in=complete_scene_status)

    if len(scenes) == 0:

        print("Trying to complete order: %s" % order.orderid)
        order.status = 'complete'
        order.completion_date = datetime.datetime.now()
        order.save()

        #only send the email if this was an espa order.
        if order.order_source == 'espa' and not order.completion_email_sent:
            try:
                sent = None
                sent = send_completion_email(order)
                if sent is None:
                    raise Exception("Completion email not sent")
                else:
                    order.completion_email_sent = datetime.datetime.now()
                    order.save()
            except Exception, e:
                msg = "Error calling send_completion_email:%s" % e
                print(msg)
                raise Exception(msg)


@transaction.atomic
def load_ee_orders():
    ''' Loads all the available orders from lta into
    our database and updates their status
    '''

    #check to make sure this operation is enabled.  Bail if not
    enabled = Configuration().getValue("load_ee_orders_enabled")
    if enabled.lower() != 'true':
        helper_logger(("enable_load_ee_orders is disabled,"
                       "skipping load_ee_orders()"))
        return

    # This returns a dict that contains a list of dicts{}
    # key:(order_num, email, contactid) = list({sceneid:, unit_num:})
    orders = lta.get_available_orders()

    # use this to cache calls to EE Registration Service username lookups
    local_cache = {}

    # Capture in our db
    for eeorder, email, contactid in orders:

        # create the orderid based on the info from the eeorder
        order_id = Order.generate_ee_order_id(email, eeorder)

        # paranoia... initialize this to None since it's used in the loop.
        order = None

        # go look to see if it already exists in the db
        try:
            order = Order.objects.get(orderid=order_id)
        except Order.DoesNotExist:

            # retrieve the username from the EE registration service
            # cache this call
            if contactid in local_cache:
                username = local_cache[contactid]
            else:
                username = lta.get_user_name(contactid)
                local_cache[contactid] = username

            # now look the user up in our db.  Create if it doesn't exist
            # we'll want to put some caching in place here too
            try:
                user = User.objects.get(username=username)

                # make sure the email we have on file is current
                if not user.email or user.email is not email:
                    user.email = email
                    user.save()

                #try to retrieve the userprofile.  if it doesn't exist create
                try:
                    user.userprofile
                except UserProfile.DoesNotExist:
                    UserProfile(contactid=contactid, user=user).save()

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
            # TODO: This code should be housed in the models module.
            # TODO: This logic should not be visible at this level.
            order = Order()
            order.orderid = order_id
            order.user = user
            order.order_type = 'level2_ondemand'
            order.status = 'ordered'
            order.note = 'EarthExplorer order id: %s' % eeorder
            order.product_options = json.dumps(Order.get_default_ee_options(),
                                               sort_keys=True,
                                               indent=4)
            order.ee_order_id = eeorder
            order.order_source = 'ee'
            order.order_date = datetime.datetime.now()
            order.priority = 'normal'
            order.save()

        for s in orders[eeorder, email, contactid]:
            #go look for the scene by ee_unit_id.  This will stop
            #duplicate key update collisions

            scene = None
            try:
                scene = Scene.objects.get(order=order,
                                          ee_unit_id=s['unit_num'])

                if scene.status == 'complete':

                    success, msg, status =\
                        lta.update_order_status(eeorder, s['unit_num'], "C")

                    if not success:
                        log_msg = ("Error updating lta for "
                                   "[eeorder:%s ee_unit_num:%s "
                                   "scene name:%s order:%s")
                        log_msg = log_msg % (eeorder, s['unit_num'],
                                             scene.name, order.orderid)

                        helper_logger(log_msg)

                        log_msg = ("Error detail: lta return message:%s "
                                   "lta return status code:%s")
                        log_msg = log_msg % (msg, status)

                        helper_logger(log_msg)

                elif scene.status == 'unavailable':
                    success, msg, status =\
                        lta.update_order_status(eeorder, s['unit_num'], "R")

                    if not success:
                        log_msg = ("Error updating lta for "
                                   "[eeorder:%s ee_unit_num:%s "
                                   "scene name:%s order:%s")
                        log_msg = log_msg % (eeorder, s['unit_num'],
                                             scene.name, order.orderid)

                        helper_logger(log_msg)

                        log_msg = ("Error detail: "
                                   "lta return message:%s  lta return "
                                   "status code:%s") % (msg, status)

                        helper_logger(log_msg)
            except Scene.DoesNotExist:
                # TODO: This code should be housed in the models module.
                # TODO: This logic should not be visible at this level.
                scene = Scene()

                product = None
                try:
                    product = espa_common.sensor.instance(s['sceneid'])
                except:
                    log_msg = ("Received product via EE that "
                               "is not implemented: %s" % s['sceneid'])
                    helper_logger(log_msg)
                    continue

                sensor_type = None

                if isinstance(product, espa_common.sensor.Landsat):
                    sensor_type = 'landsat'
                elif isinstance(product, espa_common.sensor.Modis):
                    sensor_type = 'modis'

                scene.sensor_type = sensor_type
                scene.name = product.product_id
                scene.ee_unit_id = s['unit_num']
                scene.order = order
                scene.order_date = datetime.datetime.now()
                scene.status = 'submitted'
                scene.save()

            # Update LTA
            success, msg, status =\
                lta.update_order_status(eeorder, s['unit_num'], "I")

            if not success:
                log_msg = ("Error updating lta for "
                           "[eeorder:%s ee_unit_num:%s scene "
                           "name:%s order:%s") % (eeorder,
                                                  s['unit_num'],
                                                  scene.name,
                                                  order.orderid)

                helper_logger(log_msg)

                log_msg = ("Error detail: lta return message:%s  "
                           "lta return status code:%s") % (msg, status)

                helper_logger(log_msg)


    # Sends the order submission confirmation email
def send_initial_email(order):
    return Emails().send_initial(order)


def send_completion_email(order):
    return Emails().send_completion(order)


def send_initial_emails():
    return Emails().send_all_initial()


@transaction.atomic
def finalize_orders():
    '''Checks all open orders in the system and marks them complete if all
    required scene processing is done'''

    orders = Order.objects.filter(status='ordered')
    for o in orders:
        update_order_if_complete(o)

    return True


def handle_orders():
    '''Logic handler for how we accept orders + products into the system'''
    send_initial_emails()
    handle_onorder_landsat_products()
    handle_retry_products()
    load_ee_orders()
    handle_submitted_products()
    finalize_orders()
    return True
