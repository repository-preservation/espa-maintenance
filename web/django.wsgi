import os
import sys

__author__ = "David V. Hill"


os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'
homevar = os.environ['HOME']
if not homevar.startswith('/home/espa'):
    homevar = '/home/espa'
    
#print ("DJANGO_SETTINGS HOMEVAR IS:%s" % homevar)
import django.core.handlers.wsgi
application = django.core.handlers.wsgi.WSGIHandler()

path = '%s/espa-site/orderservice' % homevar

sys.path.append('%s/espa-site' % homevar)
sys.path.append('%s/espa-site/espa' % homevar)
sys.path.append('%s/espa-site/espa/conversion_tools' % homevar)
sys.path.append('%s/espa-site/orderservice' % homevar)
sys.path.append('%s/espa-site/orderservice/ordering' % homevar)
