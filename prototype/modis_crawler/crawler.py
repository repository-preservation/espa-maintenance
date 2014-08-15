#!/usr/bin/env python

import httplib
from bs4 import BeautifulSoup
import re
from multiprocessing import Pool

host = 'e4ftl01.cr.usgs.gov'


def get_modis_tile_names(path):
    tiles = list()

    #path = '/MOLA/MYD09A1.005/2002.09.22/'

    conn = httplib.HTTPConnection(host)
    conn.request("GET", path)

    resp = conn.getresponse()
    data = resp.read()
    resp.close()

    soup = BeautifulSoup(data)
    links = soup.find_all('a')
    for l in links:
        link = l.get('href')
        if link.startswith("MYD") and link.endswith("hdf"):
            tiles.append(link)

    return tiles

def get_dates(path):
    expr = r'\d{4}\.\d{2}\.\d{2}\/'
    conn = httplib.HTTPConnection(host)
    conn.request("GET", path)

    resp = conn.getresponse()
    data = resp.read()
    resp.close()

    soup = BeautifulSoup(data)
    links = soup.find_all('a')

    dates = list()    

    for link in links:
        l = link.get('href')
        if re.match(expr, l):
            dates.append(l[:-1])

    return dates


def get_index_urls():
    parts= {
        'products': ['09A1.005', '09GA.005', '09GQ.005', '09Q1.005', '13Q1.005', '13A1.005', '13A2.005', '13A3.005'],
        'sensors' : {'MOLA': 'MYD', 'MOLT': 'MOD'}
    }

    index_paths = list()

    for sensor in parts['sensors']:
        path_prefix = sensor

        file_prefix = parts['sensors'][sensor]

        for product in parts['products']:
            product_path = "/%s/%s%s/" % (path_prefix, file_prefix, product)
            dates = get_dates(product_path)
            for d in dates:
                index_path = "%s%s/" % (product_path, d)
                index_paths.append(index_path)

    return index_paths


        
if __name__ == '__main__':
  
    tiles = list()
    
    urls = get_index_urls()

    print("Found %s urls to be indexed" % (len(urls)))
  
    pool = Pool(3)
    #pool.map(get_modis_tile_names, urls)
    tile_list  = pool.map(get_modis_tile_names, urls)

    tally = 0
    for tile in tile_list:
        tally = tally + len(tile)

    print("Found %s modis tiles" % tally)
    pool.close()

 
    #tiles.extend(get_modis_tile_names(path))
    #print("Found %i tiles" % len(tiles))


