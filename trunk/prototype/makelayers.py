#!/usr/bin/env python

import os
import commands

def makeLayers(datadir):
    files = os.listdir(datadir)
    for f in files:
        if f.endswith('tif'):
            layername = f.split('.tif')[0]
            #print layername
            template = '''
LAYER
   NAME 'espa:single:%s'
   TYPE RASTER
   STATUS OFF
   DATA ../data/%s
   CLASS
     STYLE
       OUTLINECOLOR 255 255 0
       WIDTH 3.0
     END
   END
END''' % (layername, f)
            #print template
            layer_file = 'layers/%s.layer' % layername
            layer_file_h = open(layer_file, 'wb+')
            layer_file_h.write(template)
            layer_file_h.close()
            yield layer_file

#=========================================================
            
if __name__ == '__main__':
    mapfile = open('gls2005.map', 'wb+')

    header_template = '''
MAP
  DEBUG ON
  NAME "gls"
  STATUS ON
  SIZE 1280 640
  #SYMBOLSET "../etc/symbols.txt"
  EXTENT -180 -90 180 90
  UNITS DD
  SHAPEPATH "shapes"
  IMAGECOLOR 0 0 0
  #FONTSET "../etc/fonts.txt"

  #
  # Start of web interface definition
  #
  WEB
    IMAGEPATH "/home/dhill/gls-2005/"
    IMAGEURL "/ms_tmp/"
  END
'''
    tileindexfile = 'gls_2005_index.shp'
    shape_template = '''

   LAYER
      NAME 'gls2005'
      STATUS OFF
      TILEINDEX "%s"
      TILEITEM "Location"
      TYPE RASTER
   END
''' % tileindexfile
    
    cmd = "gdaltindex %s /home/dhill/gls-2005/data/*.tif" % tileindexfile
    print cmd
    retcode,gdalcmd = commands.getstatusoutput(cmd)
    if retcode != 0:
        print ("Error creating tile index... exiting")
        exit(1)

    commands.getstatusoutput('mv *index* shapes/')
    

    country_template = '''
    LAYER
       NAME 'country_boundaries'
       STATUS DEFAULT
       TYPE POLYGON
       DATA countries
       CLASS
          OUTLINECOLOR 30 30 30
       END
    END

    LAYER
        NAME 'states_boundaries'
        STATUS DEFAULT
        TYPE POLYGON
        DATA statesp020
        CLASS
          OUTLINECOLOR 30 30 30
        END
    END
'''

    
    
    bluemarble_template = '''

   LAYER
      NAME 'bm'
      STATUS DEFAULT
      TILEINDEX "bluemarble.shp"
      TILEITEM "Location"
      TYPE RASTER
    END

'''
    mapfile.write(header_template)    
    
    for layer in makeLayers('data'):
        #pass
        include = '    INCLUDE "%s"\n' % layer
        mapfile.write(include)

    mapfile.write(shape_template)
    mapfile.write(bluemarble_template)
    mapfile.write(country_template)
    mapfile.write("END")
    mapfile.close()
    


    
