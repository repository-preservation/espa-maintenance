from django.conf.urls import patterns, include, url
from django.contrib import admin
from ordering.views import Index
from django.contrib.auth import views as django_views
from django.contrib.auth.decorators import login_required

'''
author David V Hill

URL module for the main espa_web project
'''

admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'espa_web.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),
    url(r'^ordering/', include('ordering.urls')),
    url(r'^admin/', include(admin.site.urls)),
    url(r'^login/$', django_views.login, {'template_name': 'login.html'}, name='login'),
    url(r'^$', login_required(Index.as_view()), name='root'),
    url(r'^index/$', login_required(Index.as_view()), name='index'),
)
