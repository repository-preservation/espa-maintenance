#/usr/bin/env bash

mkdir browse;
mkdir browse/tiles;

gdal_translate -of GTIFF -sds lndsr*hdf browse/out.tiff;

gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF browse/out.tiff5 browse/browse.tiff5
gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF browse/out.tiff4 browse/browse.tiff4
gdal_translate -ot Byte -scale 0 10000 0 255 -of GTIFF browse/out.tiff3 browse/browse.tiff3

gdal_merge_simple -in browse/browse.tiff5 -in browse/browse.tiff4 -in browse/browse.tiff3 -out browse/final.tif;

gdalwarp -dstalpha -srcnodata 0 browse/final.tif browse/warped.tif

gdal_translate -a_srs EPSG:4326 -co COMPRESS=DEFLATE -co PREDICTOR=2 -outsize 50% 50% -a_nodata -9999 -of GTIFF browse/warped.tif browse/browse.tif;

#gdal_retile.py -v -r bilinear -pyramidOnly -co TILED=YES -co COMPRESS=DEFLATE -co PREDICTOR=2 -ot Byte -targetDir browse/tiles browse/browse.tif

#gdal_retile.py -v -r bilinear -pyramidOnly -levels 5 -co TILED=YES -co COMPRESS=DEFLATE -co PREDICTOR=2 -ot Byte -targetDir browse/tiles browse/browse.tif

rm -rf browse/warped.tif;
rm -rf browse/*tiff*;
rm -rf browse/*out*;
rm -rf browse/final.tif;
