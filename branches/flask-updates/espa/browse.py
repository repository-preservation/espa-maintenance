#!/usr/bin/env python

class Browse(object):
    def __init__(self, work_dir, metadata, scene_name, resolution, debug=False):
        self.work_dir = work_dir
        self.metadata = metadata
        self.scene_name = scene_name
        self.resolution = resolution
        self.debug = debug
  

    #==============================================================
    #create a browse image for the product
    #==============================================================
    def make_browse(self, output_filename, bands=(5,4,3), resolution=100, projection_string=None, tiff_compression_type='DEFLATE'):
        print("Executing make_browse()")
                    
        try:
            #extrasdir = work_dir #os.path.join(work_dir, 'extras')
            #output_file = "%s-sr-browse.tif" % scene_name
            output_file = os.path.join(self.work_dir, output_filename)
        
            if not os.path.exists(self.work_dir):
                os.makedirs(self.work_dir)

            convertHDFToGTiff("%s/lndsr*hdf" % self.work_dir, "%s/out.tiff" % self.work_dir)
        
            cmds = []
            cmds.append(('gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF %s/out.tiff5 %s/browse.tiff5') % (self.work_dir, self.work_dir))
            cmds.append(('gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF %s/out.tiff4 %s/browse.tiff4') % (self.work_dir, self.work_dir))
            cmds.append(('gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF %s/out.tiff3 %s/browse.tiff3') % (self.work_dir, self.work_dir))
            cmds.append(('gdal_merge_simple -in %s/browse.tiff5 -in %s/browse.tiff4 -in %s/browse.tiff3 -out %s/final.tif') % (self.work_dir, self.work_dir,self.work_dir, self.work_dir))

            #deproject into geographic
            cmds.append(('gdalwarp -dstalpha -srcnodata 0 -t_srs EPSG:4326 %s/final.tif %s/warped.tif') % (self.work_dir, self.work_dir))

            #resize and rename
            cmds.append(('gdal_translate -co COMPRESS=DEFLATE -co PREDICTOR=2 -outsize %s%% %s%% -a_nodata -9999 -of GTIFF %s/warped.tif %s') %     (resolution,resolution,self.work_dir, output_file))

            #cleanup        
            cmds.append(('rm -rf %s/warped.tif') % (extrasdir))
            cmds.append(('rm -rf %s/*tiff*') % (extrasdir))
            cmds.append(('rm -rf %s/*out*') % (extrasdir))
            cmds.append(('rm -rf %s/final.tif') % (extrasdir))
                
            for cmd in cmds:
                if debug:
                    print "Running:%s" % cmd
                status,output = commands.getstatusoutput(cmd)
                if status != 0:
                    print ("Error occurred running:%s" % cmd)
                
                    print output
                    return status
            
            #add the browse cornerpoints to the context here
            #need to pull these from the level 1 metadata (IF it's already in longlat that is) instead so we have actual data cornerpoints instead of
            #scene cornerpoints
            #coords = parseGdalInfo(output_file)
            #metadata['BROWSE_UL_CORNER_LAT'] = coords['browse.ul'][0]
            #metadata['BROWSE_UL_CORNER_LON'] = coords['browse.ul'][1]
            #metadata['BROWSE_UR_CORNER_LAT'] = coords['browse.ur'][0]
            #metadata['BROWSE_UR_CORNER_LON'] = coords['browse.ur'][1]
            #metadata['BROWSE_LL_CORNER_LAT'] = coords['browse.ll'][0]
            #metadata['BROWSE_LL_CORNER_LON'] = coords['browse.ll'][1]
            #metadata['BROWSE_LR_CORNER_LAT'] = coords['browse.lr'][0]
            #metadata['BROWSE_LR_CORNER_LON'] = coords['browse.lr'][1]          
                                    
            print("MakeBrowse() complete...")
        except Exception,e:
            print e
            return -1
        finally:
            pass
        return 0
