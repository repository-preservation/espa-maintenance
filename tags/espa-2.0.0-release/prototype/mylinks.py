#!/usr/bin/env python
from BeautifulSoup import BeautifulSoup,SoupStrainer
import urllib,sys





def getLinks(url):
    
    data = urllib.urlopen(url)
    for link in BeautifulSoup(data, parseOnlyThese=SoupStrainer('a')):
        if link.has_key('href'):
            if( (link['href'].startswith('http') or link['href'].startswith('/')) and len(link['href']) > 1):
                foundlink = link['href']
                if foundlink.startswith("/"):
                    foundlink = url.strip() + foundlink
                print ("%s -> %s" % (url.strip(),foundlink))





if __name__ == '__main__':
#getLinks("http://davidvhill.com")
    for line in sys.stdin:
        #print("Getting %s" % line)
        
        getLinks(line)
    
        
