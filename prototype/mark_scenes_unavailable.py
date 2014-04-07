
import os, sys
os.chdir('/home/espa/espa-site/orderservice')
os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'

from ordering import core as c
from ordering.models import *

data = scenelist.readlines()
scenelist = open('/tmp/bad_scenes.txt', 'r')
data = scenelist.readlines()
scenelist.flush()
scenelist.close()
clean = [d.strip() for d in data]
for x in clean:
    s = Scene.objects.filter(status = 'onorder', name=x)
    for ss in s:
        c.set_scene_unavailable(ss.name, ss.order.orderid, 'local', '', 'not found in landsat inventory')
        c.update_order_if_complete(ss.order.orderid, ss.name)

