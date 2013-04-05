#!/usr/bin/env python
import argparse
import sys
from converters import LL2PR_Converter

#############################################################################
#
# Module: latlong_to_tile.py
#
# Description: This script will take an input latitude/longitude in decimal
#     degrees and return the respective 10 degree MODIS tile(s) which to cover
#     that lat/long.
#
# Usage: latlong2tile.py <lat> <lon>
#     <lat> is the latitude in decimal degrees
#     <lon> is the longitude in decimal degrees
#
# Developer History:
#     Gail Schmidt    Original Development          May 2012
#
# Notes:
# 1. When just using the MODIS bounding coordinates and comparing the specified
#    lat/long to those coordinates, the algorithem usually ends up with more
#    than one tile in which the point resides.  That's really not possible, but
#    is part of the fact that the bounding coordinates are being used.  So the
#    algorithm will list the tiles in order of which tile center is closest to
#    the point.
#
#############################################################################

# Get the input arguments
parser = argparse.ArgumentParser(description='Determine the MODIS tile(s) \
for the specified latitude and longitude.')
parser.add_argument('lat', action="store", type=float, nargs=1,
    help='latitude in decimal degrees (-90.0 to 90.0)')
parser.add_argument('lon', action="store", type=float, nargs=1,
    help='longitude in decimal degrees (-180.0 to 180.0)')
args = parser.parse_args()
lat = args.lat[0]
lon = args.lon[0]
#print 'DEBUG latitude = ', lat
#print 'DEBUG longitude = ', lon

# Determine which path/rows are required for this point
conv = LL2PR_Converter()
results = conv.latlong_to_tile (lat, lon)
nmodis_tiles = results[0]
if nmodis_tiles == 0:
    sys.exit ('Exiting. Invalid latitude/longitude point provided for input.')

# Return the list of MODIS tiles which cover the specified lat/long
print 'For latitude {0}, longitude {1} the following {2} MODIS 10 degree ' \
      'tile(s) were located, in order of their distance from the point to ' \
      'the tile center:'.format(lat, lon, nmodis_tiles)
for i in range(nmodis_tiles):
    print '    h{:02d}v{:02d}'.format(results[1+i*2], results[2+i*2])
