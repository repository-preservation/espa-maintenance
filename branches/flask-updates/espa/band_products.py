#!/usr/bin/env python

class BandProduct(object):

    def __init__(self, work_directory, scene_name, debug=False):
        self.work_directory = work_directory
        self.scene_name = scene_name
        self.debug = debug

    #==============================================================
    #create NDVI product for current scene
    #==============================================================
    def make_ndvi(self):
        print("Executing make_ndvi")
                       
        try:
            ndviDir = "%s" % work_directory
            ndvi_output_file = "%s-sr-ndvi.tif" % scene_name
            ndvi_output_file = os.path.join(ndviDir, ndvi_output_file)
        
            #start with a clean slate
            if not os.path.exists(ndviDir):
                os.makedirs(ndviDir)

            status = convertHDFToGTiff("%s/lndsr*hdf" % work_directory, "%s/out.tiff" % ndviDir)
            if status != 0:
                print ("Status %s:Error converting lndsr to Geotiff" % str(status))
                return status

            gc.collect()
            
            # load the proper geotiff bands into GDAL 
            red_file = ("%s/out.tiff3") % (ndviDir)
            in_ds = gdal.Open(red_file) 
            red = in_ds.ReadAsArray()
            geo = in_ds.GetGeoTransform()  
            proj = in_ds.GetProjection()   
            shape = red.shape          
            in_ds = None

            nir_file = ("%s/out.tiff4") % (ndviDir)
            in_ds = gdal.Open(nir_file)
            nir = in_ds.ReadAsArray()
            in_ds = None


            # NDVI = (nearInfrared - red) / (nearInfrared + red)
            nir = np.array(nir, dtype = float)  # change the array data type from integer to float to allow decimals
            red = np.array(red, dtype = float)

            np.seterr(divide='ignore')
                
            numerator = np.subtract(nir, red) 
            denominator = np.add(nir, red)
            nir = None
            red = None
            gc.collect()

            ndvi = np.divide(numerator,denominator)
            numerator = None
            denominator = None
            gc.collect()

            #put this into 10000 range
            ndvi = np.multiply(ndvi, 10000)
            gc.collect()
                
            #set all negative values to 0
            np.putmask(ndvi, ndvi < 0, 0)
                
            #set all values greater than 10000 to 10000
            np.putmask(ndvi, ndvi > 10000, 10000)
                
            driver = gdal.GetDriverByName('GTiff')

      
            ndvifile = ('%s/ndvi.tif') % (ndviDir)
            dst_ds = driver.Create( ndvifile, shape[1], shape[0], 1, gdal.GDT_Float32)

            # here we set the variable dst_ds with 
            # destination filename, number of columns and rows
            # 1 is the number of bands we will write out
            # gdal.GDT_Float32 is the data type - decimals
            dst_ds.SetGeoTransform(geo)
            dst_ds.SetProjection(proj) 
            dst_ds.GetRasterBand(1).WriteArray(ndvi)  
            stat = dst_ds.GetRasterBand(1).GetStatistics(1,1)
            dst_ds.GetRasterBand(1).SetStatistics(stat[0], stat[1], stat[2], stat[3])
            dst_ds = None

            gc.collect()

            in_ds = None
            dst_ds = None

            cmd = ('gdal_translate -ot UInt16 -scale 0 10000 0 10000 -of GTiff %s %s') % (ndvifile, ndvi_output_file)
            status,output = commands.getstatusoutput(cmd)
            if status != 0:
                print ("Error converting ndvi.tif to %s" % ndvi_output_file)
                print output
                return status
                
            cmd = ('rm -rf %s/out.tiff* %s/ndvi.tif') % (ndviDir, ndviDir)
            status,output = commands.getstatusoutput(cmd)
        except Exception, e:
            print e
            return -1
        finally:
            gc.collect()

        print ("NDVI() complete...")
        return 0
