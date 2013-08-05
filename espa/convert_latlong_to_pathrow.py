#!/usr/bin/env python
import argparse
import sys
from convert import LL2PR_Converter

#############################################################################
#
# Module: latlong_to_pathrow.py
#
# Description: This script will take an input latitude/longitude in decimal
#     degrees and return the respective descending WRS-2 path/row(s) which to
#     cover that lat/long.
#
# Usage: latlong2pathrow.py <lat> <lon>
#     <lat> is the latitude in decimal degrees
#     <lon> is the longitude in decimal degrees
#
# Developer History:
#     Gail Schmidt    Original Development          June 2012
#
# Notes:
# 1. When just using the WRS-2 bounding coordinates and comparing the specified
#    lat/long to those coordinates, the point can reside within multiple
#    path/rows.  The algorithm will list the path/rows in order of which scene
#    center is closest to the point.
# 2. This application currently skips over ascending rows (123 - 245).  Rows
#    1 through 121 descend to the southmost row, which is row 122.  Rows
#    123 to 245 comprise the ascending portion of the orbit.  Row 246 is the
#    northernmost row.  And rows 247 and 248 begin the descending portion of
#    the next orbit (path) leading to row 1.  If the ascending rows are
#    processed, then the corner points will need to be flipped.  UL switched
#    with LR and UR switched with LL.  UL -> LR, UR -> LL, LL -> UR, LR -> UL
#
#############################################################################

# Get the input arguments
parser = argparse.ArgumentParser(description='Determine the decending WRS-2 \
path/row(s) for the specified latitude and longitude.')
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
results = conv.latlong_to_pathrow (lat, lon)
npathrow = results[0]
if npathrow == 0:
    sys.exit ('Exiting. Invalid latitude/longitude point provided for input.')

# Return the list of WRS path/rows which cover the specified lat/long
#print 'For latitude {0}, longitude {1} the following {2} WRS-2 path/row(s) ' \
#      'were located, in order of their distance from the point to the scene ' \
#      'center:'.format(lat, lon, npathrow)
for i in range(npathrow):
    print '    p{:d}r{:d}'.format(results[1+i*2], results[2+i*2])
