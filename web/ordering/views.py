import core
import json

import django.contrib.auth

from ordering import validators
from ordering.models import Scene
from ordering.models import Order
from ordering.models import Configuration as Config

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

    def _get_option_style(self, request):
        '''Utility method to determine which options to display in the
        templates based on the user.

        Keyword args:
        request -- An HTTP request object

        Return:
        str('display:none') if the user is not admin or internal
        str('') otherwise
        '''
        if hasattr(request, 'user'):
            if request.user.username not in ('espa_admin', 'espa_internal'):
                return "display:none"
            else:
                return ""

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

            cache_vals = cache.get_many(['system_message_title',
                                         'system_message_1',
                                         'system_message_2',
                                         'system_message_3'])

            # flag to determine if any cached values were expired/missing and
            # need to be updated
            update_cache = False

            c = Config()

            #look through the cache_vals and see if any of them are none
            for key in cache_vals:
                if not cache_vals[key]:
                    update_cache = True
                    cache_vals[key] = c.get_value(key)

            if update_cache:
                cache.set_many(cache_vals,
                               timeout=settings.SYSTEM_MESSAGE_CACHE_TIMEOUT)


            ctx['system_message_title'] = cache_vals['system_message_title']
            ctx['system_message_1'] = cache_vals['system_message_1']
            ctx['system_message_2'] = cache_vals['system_message_2']
            ctx['system_message_3'] = cache_vals['system_message_3']
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


class Index(AbstractView):
    template = 'index.html'

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
    template = 'new_order.html'


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
                elif core.is_number(val):
                    if str(val).find('.') != -1:
                        defaults[key] = float(val)
                    else:
                        defaults[key] = int(val)
                else:
                    defaults[key] = val

        return defaults

    def _get_scenelist(self, request):
        data = list()

        if 'scenelist' in request.FILES:
            data = request.FILES['scenelist'].read().split('\n')

        return [d.strip() for d in data]

    def _get_verified_scenelist(self, request):
        sl = self._get_scenelist(request)
        payload = {'scenelist': self._get_scenelist(request)}
        slv = validators.SceneListValidator(payload)
        return list(slv.get_verified_scene_set(sl))

    def get(self, request):
        '''Request handler for new order initial form

        Keyword args:
        request -- HTTP request object

        Return:
        HttpResponse
        '''

        c = self._get_request_context(request)
        c['user'] = request.user
        c['optionstyle'] = self._get_option_style(request)

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
        validator_parameters = dict(request.POST)
        validator_parameters['scenelist'] = self._get_scenelist(request)
        validator = validators.NewOrderValidator(validator_parameters)
                
        if validator.errors():
            
            print("VALIDATOR ERRORS")
            print(type(validator.errors()))

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
                    error_list.append(m)

            c['errors'] = error_list
            c['user'] = request.user
            c['optionstyle'] = self._get_option_style(request)

            t = loader.get_template(self.template)

            return HttpResponse(t.render(c))

        else:
            #option_string = json.dumps(selected_options)
            #option_string = json.dumps(self._get_order_options(request))
            option_string = json.dumps(self._get_order_options(request),
                                       sort_keys=True,
                                       indent=4)

            order = Order.enter_new_order(request.user.username,
                                          'espa',
                                          self._get_verified_scenelist(request),
                                          option_string,
                                          note=self._get_order_description(request.POST)
                                          )
            core.send_initial_email(order)

            #TODO -- Remove this logic once all pre ESPA-2.3.0 orders are out
            # of the system
            if order.email:
                email = order.email
            else:
                email = order.user.email

            url = reverse('list_orders', kwargs={'email': email})
            return HttpResponseRedirect(url)


class ListOrders(AbstractView):
    initial_template = "listorders.html"
    results_template = "listorders_results.html"

    def get(self, request, email=None, output_format=None):
        '''Request handler for displaying all user orders

        Keyword args:
        request -- HTTP request object
        email -- the user's email
        output_format -- deprecated

        Return:
        HttpResponse
        '''

        #no email provided, ask user for an email address
        if email is None or not core.validate_email(email):
            user = User.objects.get(username=request.user.username)

            #default the email field to the current user email
            form = ListOrdersForm(initial={'email': user.email})

            c = self._get_request_context(request, {'form': form})

            t = loader.get_template(self.initial_template)

            return HttpResponse(t.render(c))
        else:
            #if we got here display the orders
            orders = Order.list_all_orders(email)

            t = loader.get_template(self.results_template)

            c = self._get_request_context(request, {'email': email,
                                                    'orders': orders
                                                    })
            return HttpResponse(t.render(c))


class OrderDetails(AbstractView):
    template = 'orderdetails.html'

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
    template = "loggedout.html"

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
