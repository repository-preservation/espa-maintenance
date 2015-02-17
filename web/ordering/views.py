import json
import collections
from espa_common import sensor

import django.contrib.auth

from espa_common import utilities

from ordering import validators
from ordering.models import Scene
from ordering.models import Order
from ordering.models import Configuration as Config
from ordering.models import Download
from ordering.models import DownloadSection

from django import forms
from django.conf import settings
from django.contrib.syndication.views import Feed
from django.core.urlresolvers import reverse
from django.core.cache import cache
from django.db.models import Q
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.http import Http404
from django.template import loader
from django.template import RequestContext
from django.utils.feedgenerator import Rss201rev2Feed
from django.views.generic import View

from django.contrib.auth.models import User


class AbstractView(View):

    
    #def _get_option_style(self, request):
    #    '''Utility method to determine which options to display in the
    #    templates based on the user.
    #
    #    Keyword args:
    #    request -- An HTTP request object
    #
    #    Return:
    #    str('display:none') if the user is not admin or internal
    #    str('') otherwise
    #    '''
    #    if hasattr(request, 'user'):
    #        
    #        if request.user.username not in ('espa_admin', 'espa_internal'):
    #            return "display:none"
    #        else:
    #            return ""

    def _display_system_message(self, ctx):
        '''Utility method to populate the context with systems messages if
        there are any configured for display

        Keyword args:
        ctx -- A RequestContext object (dictionary)

        Return:
        No return.  Dictionary is passed by reference.
        '''

        # calls to the Django cache return none if the key is not present
        msg = cache.get('display_system_message')

        # look for the trigger flag 'display_system_message' in the cache.
        # if its not there then look in the Config() model for it then update
        # the cache
        if not msg:
            msg = Config().getValue('display_system_message')
            cache.set('display_system_message',
                      msg,
                      timeout=settings.SYSTEM_MESSAGE_CACHE_TIMEOUT)

        # system message is only going to be displayed if the msg.lower() is
        # equal to 'true' (string value)
        if msg.lower() == 'true':

            ctx['display_system_message'] = True

            cache_keys = ['system_message_title', 'system_message_body']

            cache_vals = cache.get_many(cache_keys)

            # flag to determine if any cached values were expired/missing and
            # need to be updated
            update_cache = False

            c = Config()

            #look through the cache_vals and see if any of them are none
            for key in cache_keys:
                if not key in cache_vals:
                    update_cache = True
                    cache_vals[key] = c.getValue(key)

            if update_cache:
                cache.set_many(cache_vals,
                               timeout=settings.SYSTEM_MESSAGE_CACHE_TIMEOUT)

            ctx['system_message_title'] = cache_vals['system_message_title']
            ctx['system_message_body'] = cache_vals['system_message_body']
            c = None
        else:
            ctx['display_system_message'] = False

    def _get_request_context(self,
                             request,
                             params=dict(),
                             include_system_message=True):

        context = RequestContext(request, params)

        if include_system_message:
            self._display_system_message(context)

        return context


class AjaxForm(AbstractView):
    def get(self, request):
        template = 'ordering/test.html'
        c = self._get_request_context(request)
        t = loader.get_template(template)
        return HttpResponse(t.render(c))


class TestAjax(AbstractView):

    def render_to_json_response(self, context, **response_kwargs):
        data = json.dumps(context)
        response_kwargs['content_type'] = 'application/json'
        return HttpResponse(data, **response_kwargs)

    def get(self, request):

        name = request.GET.get('name', '')

        data = {'user': request.user.get_username(),
                'name': name,
                'status': 'GET request ok'}

        return self.render_to_json_response(data)

    def post(self, request):

        name = "No name provided"
        if 'name' in request.POST:
            name = request.POST['name']

        age = "No age provided"
        if 'age' in request.POST:
            age = request.POST['age']

        data = {'user': request.user.get_username(),
                'name': name,
                'age': age,
                'status': 'POST request ok'}

        return self.render_to_json_response(data)


class Index(AbstractView):
    template = 'ordering/index.html'

    def get(self, request):
        '''Request handler for / and /index

        Keyword args:
        request -- HTTP request object

        Return:
        HttpResponse
        '''
        c = self._get_request_context(request)
        t = loader.get_template(self.template)
        return HttpResponse(t.render(c))


class NewOrder(AbstractView):
    template = 'ordering/new_order.html'
    input_product_list = None

    def _get_order_description(self, parameters):
        description = None
        if 'order_description' in parameters:
            description = parameters['order_description']
        return description

    def _get_order_options(self, request):

        defaults = Order.get_default_options()

        # This will make sure no additional options past the ones we are
        # expecting will make it into the database
        #for key in request.POST.iterkeys():
        for key in defaults:
            if key in request.POST.iterkeys():
                val = request.POST[key]
                if val is True or str(val).lower() == 'on':
                    defaults[key] = True
                elif utilities.is_number(val):
                    if str(val).find('.') != -1:
                        defaults[key] = float(val)
                    else:
                        defaults[key] = int(val)
                else:
                    defaults[key] = val

        return defaults

    def _get_input_product_list(self, request):

        if not self.input_product_list:
            if 'input_product_list' in request.FILES:
                _ipl = request.FILES['input_product_list'].read().split('\n')
                self.input_product_list = _ipl

        retval = collections.namedtuple("InputProductListResult",
                                        ['input_products', 'not_implemented'])
        retval.input_products = list()
        retval.not_implemented = list()

        if self.input_product_list:
            for line in self.input_product_list:

                line = line.strip()

                try:
                    s = sensor.instance(line)
                    retval.input_products.append(s)
                except sensor.ProductNotImplemented, ni:
                    retval.not_implemented.append(ni.product_id)

        return retval

    def _get_verified_input_product_list(self, request):

        ipl = self._get_input_product_list(request)

        if ipl:
            payload = {'input_products': ipl.input_products}

            lplv = validators.LandsatProductListValidator(payload)

            mplv = validators.ModisProductListValidator(payload)

            landsat = lplv.get_verified_input_product_set(ipl.input_products)

            modis = mplv.get_verified_input_product_set(ipl.input_products)

            return list(landsat.union(modis))
        else:
            return None

    def get(self, request):
        '''Request handler for new order initial form

        Keyword args:
        request -- HTTP request object

        Return:
        HttpResponse
        '''

        c = self._get_request_context(request)
        c['user'] = request.user
        #c['optionstyle'] = self._get_option_style(request)

        t = loader.get_template(self.template)

        return HttpResponse(t.render(c))

    def post(self, request):
        '''Request handler for new order submission

        Keyword args:
        request -- HTTP request object

        Return:
        HttpResponseRedirect upon successful submission
        HttpResponse if there are errors in the submission
        '''
        #request must be a POST and must also be encoded as multipart/form-data
        #in order for the files to be uploaded

        validator_parameters = {}

        # coerce the request.POST to be a normal Python dictionary
        validator_parameters = dict(request.POST)

        # retrieve the namedtuple for the input product list
        ipl = self._get_input_product_list(request)

        # send the validator only the items in the list that could actually
        # be instantiated as a sensor.  The other tuple item not_implemented
        # is being ignored unless we want to tell the users about all the
        # junk they included in their input file
        validator_parameters['input_products'] = ipl.input_products

        validator = validators.NewOrderValidator(validator_parameters)

        if validator.errors():

            c = self._get_request_context(request)

            #unwind the validator errors.  It comes out as a dict with a key
            #for the input field name and a value of a list of error messages.
            # At this point we are only displaying the error messages in one
            # block but going forward will be able to put the error message
            # right next to the field where the error occurred once the
            # template is properly modified.
            errors = validator.errors().values()

            error_list = list()

            for e in errors:
                for m in e:
                    m = m.replace("\n", "<br/>")
                    m = m.replace("\t", "    &#149; ")
                    m = m.replace(" ", "&nbsp;")
                    error_list.append(m)

            c['errors'] = sorted(error_list)
            c['user'] = request.user
            c['optionstyle'] = self._get_option_style(request)

            t = loader.get_template(self.template)

            return HttpResponse(t.render(c))

        else:

            vipl = self._get_verified_input_product_list(request)

            order_options = self._get_order_options(request)

            order_type = "level2_ondemand"

            if order_options['include_statistics'] is True:
                vipl.append("plot")
                order_type = "lpcs"

            option_string = json.dumps(order_options,
                                       sort_keys=True,
                                       indent=4)

            desc = self._get_order_description(request.POST)

            order = Order.enter_new_order(request.user.username,
                                          'espa',
                                          vipl,
                                          option_string,
                                          order_type,
                                          note=desc
                                          )

            email = order.user.email

            url = reverse('list_orders', kwargs={'email': email})

            return HttpResponseRedirect(url)


class ListOrders(AbstractView):
    template = "ordering/listorders.html"
    
    def get(self, request, email=None, output_format=None):
        '''Request handler for displaying all user orders

        Keyword args:
        request -- HTTP request object
        email -- the user's email
        output_format -- deprecated

        Return:
        HttpResponse
        '''
       
        if email is None or not utilities.validate_email(email):
            user = User.objects.get(username=request.user.username)
            email = user.email

        orders = Order.list_all_orders(email)

        form = ListOrdersForm(initial={'email': email})
        
        c = self._get_request_context(request, {'form': form, 
                                                'email': email,
                                                'orders': orders
                                                })
                                                
        t = loader.get_template(self.template)

        return HttpResponse(t.render(c))
        


class Downloads(AbstractView):
    template = 'ordering/downloads.html'

    def get(self, request):
        '''Request handler to display the downloads template

        Keyword args:
        request -- HTTP request object

        Return:
        HttpResponse
        '''
        ob = 'display_order', 'title'

        d = DownloadSection.objects.filter(visible=True).order_by(ob)

        t = loader.get_template(self.template)

        c = self._get_request_context(request, {'sections': d})

        return HttpResponse(t.render(c))


class OrderDetails(AbstractView):
    template = 'ordering/orderdetails.html'

    def get(self, request, orderid, output_format=None):
        '''Request handler to get the full listing of all the scenes
        & statuses for an order

        Keyword args:
        request -- HTTP request object
        orderid -- the order id for the order
        output_format -- deprecated

        Return:
        HttpResponse
        '''

        t = loader.get_template(self.template)

        c = self._get_request_context(request)
        try:
            c['order'], c['scenes'] = Order.get_order_details(orderid)
            return HttpResponse(t.render(c))
        except Order.DoesNotExist:
            raise Http404


class LogOut(AbstractView):
    template = "ordering/loggedout.html"

    def get(self, request):
        '''Simple view to log a user out and land them on an exit page'''

        django.contrib.auth.logout(request)

        t = loader.get_template(self.template)

        c = self._get_request_context(request, include_system_message=False)

        return HttpResponse(t.render(c))


class ListOrdersForm(forms.Form):
    '''Form object for the ListOrders form'''
    email = forms.EmailField()


class StatusFeed(Feed):
    '''Feed subclass to publish user orders via RSS'''

    feed_type = Rss201rev2Feed

    title = "ESPA Status Feed"

    link = ""

    def _get_email(self, obj):
        if obj.email:
            return obj.email
        else:
            return obj.user.email

    def get_object(self, request, email):
        orders = Order.objects.filter(Q(email=email) | Q(user__email=email))
        if not orders:
            raise Http404
        else:
            return orders

    def link(self, obj):
        return reverse('status_feed',
                       kwargs={'email': self._get_email(obj[0])})

    def description(self, obj):
        return "ESPA scene status for:%s" % self._get_email(obj[0])

    def item_title(self, item):
        return item.name

    def item_link(self, item):
        return item.product_dload_url

    def item_description(self, item):
        orderid = item.order.orderid
        orderdate = item.order.order_date

        return "scene_status:%s,orderid:%s,orderdate:%s" \
               % (item.status, orderid, orderdate)

    def items(self, obj):

        #email = obj[0].email
        email = self._get_email(obj[0])

        SO = Scene.objects

        r = SO.filter(Q(order__email=email) | Q(order__user__email=email))\
              .filter(status='complete')\
              .order_by('-order__order_date')
        return r
