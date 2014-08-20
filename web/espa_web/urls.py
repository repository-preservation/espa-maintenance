from django.conf.urls import patterns, include, url
from django.contrib import admin
from ordering.views import Index
from ordering.views import LogOut
from ordering.views import Downloads
from console import views as console_views
from django.contrib.auth import views as django_views
from django.contrib.auth.decorators import login_required

'''
author David V Hill

URL module for the main espa_web project
'''

admin.autodiscover()

urlpatterns = patterns('',
                       url(r'^ordering/',
                           include('ordering.urls')),

                       url(r'^console/',
                           include('console.urls')),

                       url(r'^admin/',
                           include(admin.site.urls)),

                       url(r'^login/$',
                           django_views.login,
                           {'template_name': 'ordering/login.html'},
                           name='login'),

                       url(r'^logout/$',
                           LogOut.as_view(),
                           name='logout'),

                       url(r'^$',
                           login_required(Index.as_view()),
                           name='root'),

                       url(r'^index/$',
                           login_required(Index.as_view()),
                           name='index'),

                       url(r'^downloads/$',
                           login_required(Downloads.as_view()),
                           name='downloads'),

                       )
