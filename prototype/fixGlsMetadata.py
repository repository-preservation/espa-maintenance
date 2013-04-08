#!/usr/bin/env python
from cStringIO import StringIO

if __name__ == '__main__':
    f = open('/home/dhill/gls2005tst/LE71980192005259ASN00/result.txt')
    data = f.readlines()
    f.close()
    length = len(data)
    buffer = StringIO()
    
    #print length
    count = 1
    for d in data:
        if count < length:
            buffer.write(d)
            count = count + 1
    
    print buffer.getvalue()
    buffer.close()
    