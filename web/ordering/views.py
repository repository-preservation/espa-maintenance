import core
import json
import django.contrib.auth
import ordering.view_validator as vv

from ordering.models import Scene
from ordering.models import Order
from ordering.models import Configuration as Config

from django import forms

from django.contrib.syndication.views import Feed
from django.core.urlresolvers import reverse
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
        msg = Config().getValue('display_system_message')

        if msg.lower() == 'true':
            c = Config()
            ctx['display_system_message'] = True
            ctx['system_message_title'] = c.getValue('system_message_title')
            ctx['system_message_1'] = c.getValue('system_message_1')
            ctx['system_message_2'] = c.getValue('system_message_2')
            ctx['system_message_3'] = c.getValue('system_message_3')
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

    def get(self, request):
        '''Request handler for new order initial form

        Keyword args:
        request -- HTTP request object

        Return:
        HttpResponse
        '''

        print("foshizzle")

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

        context, errors, scene_errors = vv.validate_input_params(request)

        option_ctx, option_errs = vv.validate_product_options(request)

        if len(option_errs) > 0:
            errors['product_options'] = option_errs

        if len(scene_errors) > 0:
            errors['scenes'] = scene_errors

        if len(errors) > 0:

            c = self._get_request_context(request)

            c['errors'] = errors
            c['user'] = request.user
            c['optionstyle'] = self._get_option_style(request)

            t = loader.get_template(self.template)

            return HttpResponse(t.render(c))

        else:

            option_string = json.dumps(option_ctx)

            order = Order.enter_new_order(request.user.username,
                                         'espa',
                                         context['scenelist'],
                                         option_string,
                                         note=context['order_description']
                                         )
            core.send_initial_email(order)

            #TODO -- Remove this logic once all pre ESPA-2.3.0 orders are out
            # of the system
            if order.email:
                email = order.email
            else:
                email = order.user.email

            #TODO -- Fix this
            url = reverse('list_orders', kwargs={'email': email})
            #return HttpResponseRedirect('/status/%s' % request.POST['email'])
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
