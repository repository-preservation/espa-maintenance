import requests
import shutil
import os
from PIL import Image

#url = 'http://earthexplorer.usgs.gov/browse/landsat_8/2015/157/071/LC81570712015056LGN00.jpg'
url = 'http://earthexplorer.usgs.gov/browse/etm/60/14/2003/LE7060014100315151.jpg'

target_file = '/tmp/pic.jpg'
final_image = '/tmp/a.jpg'

r = requests.get(url, stream=True)
if r.status_code == 200:
    with open(target_file, 'wb') as f:
        r.raw.decode_content = True
        shutil.copyfileobj(r.raw, f)


#now clip the image to what we need
target_width = 700 
target_height = 650

try:
    im = Image.open(target_file)
    h, w = im.size
    left = w/2 - (target_width/2)
    right = w - left
    top = h/2 - (target_height/2)
    bottom = h - top

    box = (left, top, right, bottom)
    pi = im.crop(box)
    pi.save(final_image)
finally:
    os.remove(target_file)
