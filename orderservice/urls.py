from django.conf.urls.defaults import *
from django.contrib import admin
from ordering.views import StatusFeed


admin.autodiscover()

urlpatterns = patterns('',
    (r'^rpc$', 'ordering.rpc.rpc_handler'),
    (r'^new','ordering.views.neworder'),
    (r'^status/$', 'ordering.views.listorders'),
    (r'^status/([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4})/$', 'ordering.views.listorders'),
    #(r'^status/(?P<email>[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4})/(?P<output_format>[A-Za-z]{3})', 'ordering.views.listorders'),
    (r'^status/(?P<email>[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4})/rss/$', StatusFeed()),
       
    (r'^status/(?P<orderid>[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}-[0-9]{6,8}-[0-9]{3,6})/$', 'ordering.views.orderdetails'),
    (r'^admin/', include(admin.site.urls)),
    (r'^login/$', 'django.contrib.auth.views.login', {'template_name': 'login.html'}),
    (r'^$', 'ordering.views.neworder'),
                         

)
