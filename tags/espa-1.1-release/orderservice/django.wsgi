import os
import sys

os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'

import django.core.handlers.wsgi
application = django.core.handlers.wsgi.WSGIHandler()

path = '/home/espa/espa-site/orderservice'

sys.path.append('/home/espa/espa-site')
sys.path.append('/home/espa/espa-site/orderservice')
sys.path.append('/home/espa/espa-site/orderservice/ordering')
