#!/usr/bin/env python
import argparse
import sys
from converters import LL2PR_Converter

#############################################################################
#
# Module: tile_to_pathrow.py
#
# Description: This script will take an input 10 degree MODIS tile and
#     return the respective descending WRS-2 path/rows required to cover that
#     tile.
#
# Usage: tile_to_pathrow.py <htile> <vtile>
#     <htile> is the MODIS horizontal tile number
#     <vtile> is the MODIS vertical tile number
#
# Developer History:
#     Gail Schmidt    Original Development          June 2012
#
# Notes:
# 1. There will be a large number of path/rows which cover the bounding
#    coordinates of the MODIS tile.  The algorithm will list the path/rows in
#    order of which scene center is closest to the center of the MODIS tile.
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
parser = argparse.ArgumentParser(description='Determine the descending \
path/rows needed to cover the specified MODIS 10 degree tile.')
parser.add_argument('htile', action="store", type=int, nargs=1,
    help='MODIS horizontal tile number (0-35)')
parser.add_argument('vtile', action="store", type=int, nargs=1,
    help='MODIS vertical tile number (0-17)')
args = parser.parse_args()
htile = args.htile[0]
vtile = args.vtile[0]
#print 'DEBUG htile = ', htile
#print 'DEBUG vtile = ', vtile

# Determine which path/rows are required for this tile
conv = LL2PR_Converter()
results = conv.tile_to_pathrow (htile, vtile)
npathrow = results[0]
if npathrow == 0:
    sys.exit ('Exiting. Invalid tile argument or tile is fill.')

# Return the list of path/rows which cover the specified MODIS tile
print 'For htile {0}, vtile {1} the following {2} path/row(s) were ' \
      'located, in order of their distance from the center of the scene to ' \
      'the center of the tile:'.format(htile, vtile, npathrow)
for i in range(npathrow):
    print '    p{:d}r{:d}'.format(results[1+i*2], results[2+i*2])
