import os
import sys

__author__ = "David V. Hill"


os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'

import settings
import django.core.handlers.wsgi

application = django.core.handlers.wsgi.WSGIHandler()

sys.path.append(os.path.join(settings.APPLICATION_ROOT, 'espa-site'))
sys.path.append(os.path.join(settings.APPLICATION_ROOT, 'espa-site', 'espa'))
sys.path.append(os.path.join(settings.APPLICATION_ROOT, 'espa-site', 'web'))
sys.path.append(os.path.join(settings.APPLICATION_ROOT, 'espa-site', 'web', 'ordering'))

