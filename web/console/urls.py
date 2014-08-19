from django.conf.urls import patterns, url
from django.contrib.auth.decorators import login_required

from views import Index, StatusMessage

urlpatterns = patterns('', 
    url(r'^statusmsg',
        login_required(StatusMessage.as_view()), name='statusmsg'),
    url(r'^$',
        login_required(Index.as_view()), name='consoleindex')
    )
