#!/usr/bin/env python
'''
Example code of how to create a 3 band composite using the Python 
GDAL bindings
'''
from osgeo import gdal
import osr

img1 = gdal.Open("/home/dhill/Desktop/prototype/resize/L5029030_03020101008_B60.TIF")
img2 = gdal.Open("/home/dhill/Desktop/prototype/resize/L5029030_03020101008_B40.TIF")
img3 = gdal.Open("/home/dhill/Desktop/prototype/resize/L5029030_03020101008_B50.TIF")

proj = img1.GetProjectionRef()

x = img1.RasterXSize
y = img1.RasterYSize

band1 = img1.GetRasterBand(1).ReadAsArray()
band2 = img2.GetRasterBand(1).ReadAsArray()
band3 = img3.GetRasterBand(1).ReadAsArray()

form = "GTiff"
driver = gdal.GetDriverByName(form)

dst_ds = driver.Create("/home/dhill/Desktop/prototype/resize/work/645.tif", x,y,3,gdal.GDT_Byte)

dst_ds.GetRasterBand(1).WriteArray(band1)
dst_ds.GetRasterBand(2).WriteArray(band2)
dst_ds.GetRasterBand(3).WriteArray(band3)

dst_ds.SetProjection(proj)

img1 = None
img2 = None
img3 = None
dst_ds = None
