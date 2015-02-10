from django.conf.urls import patterns
from django.conf.urls import url
from django.contrib import admin
from ordering.views import StatusFeed
from ordering import rpc
from ordering.views import NewOrder
from ordering.views import ListOrders
from ordering.views import OrderDetails
from django.contrib.auth.decorators import login_required

from ordering.views import TestAjax
from ordering.views import AjaxForm

'''
author David V. Hill

Url module for the ordering/ application within the espa_web project
'''

admin.autodiscover()

urlpatterns = patterns('',
    url(r'^rpc$',
        rpc.rpc_handler),

    url(r'^new',
        login_required(NewOrder.as_view()),
        name='new_order'),

    url(r'^status/$',
        login_required(ListOrders.as_view()),
        name='listorders_form'),

    url(r'^status/(?P<email>[A-Za-z0-9._%+-\\\']+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4})/$',
        login_required(ListOrders.as_view()),
        name='list_orders'),

    url(r'^status/(?P<email>[A-Za-z0-9._%+-\\\']+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4})/rss/$',
        StatusFeed(),
        name='status_feed'),

    url(r'^status/(?P<orderid>[A-Za-z0-9._%+-\\\']+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}-[0-9]{6,8}-[0-9]{3,6})/$',
        login_required(OrderDetails.as_view()),
        name='espa_order_detail'),

    url(r'^status/(?P<orderid>[A-Za-z0-9._%+-\\\']+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}-[0-9]{13})/$',
        login_required(OrderDetails.as_view()),
        name='ee_order_detail'),

    url(r'^status/(?P<orderid>[A-Za-z0-9._%+-\\\']+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}.*)/$',
        login_required(OrderDetails.as_view()),
        name='generic_order_status_detail'),

    url(r'^ajax/$',
        TestAjax.as_view(),
        name='ajax'),

    url(r'^test/$',
        AjaxForm.as_view(),
        name='ajax_form'),
)
