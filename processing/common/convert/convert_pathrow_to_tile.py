#!/usr/bin/env python
import argparse
import sys
from convert import LL2PR_Converter

#############################################################################
#
# Module: pathrow_to_tile.py
#
# Description: This script will take an input WRS-2 path/row and return
#     the respective 10 degree MODIS tile(s) required to cover that
#     path/row.
#
# Usage: pathrow_to_tile.py <path> <row>
#     <path> is the WRS-2 path number
#     <row> is the WRS-2 row number
#
# Developer History:
#     Gail Schmidt    Original Development          May 2012
#
# Notes:
#
#############################################################################

# Get the input arguments
parser = argparse.ArgumentParser(description='Determine the MODIS tile(s) \
for the specified path and row.')
parser.add_argument('path', action="store", type=int, nargs=1,
    help='WRS path (1-233)')
parser.add_argument('row', action="store", type=int, nargs=1,
    help='WRS row (1-248)')
args = parser.parse_args()
wrs_path = args.path[0]
wrs_row = args.row[0]
#print 'DEBUG path = ', wrs_path
#print 'DEBUG row = ', wrs_row

# Determine which tiles are required for this path/row
conv = LL2PR_Converter()
results = conv.pathrow_to_tile (wrs_path, wrs_row)
nmodis_tiles = results[0]
if nmodis_tiles == 0:
    sys.exit ('Exiting. Invalid path/row argument.')

# Return the list of MODIS tiles which cover the specified path/row
#print 'For path {0}, row {1} the following {2} MODIS 10 degree tile(s) ' \
#      'are needed:'.format(wrs_path, wrs_row, nmodis_tiles)
for i in range(nmodis_tiles):
    print '    h{:02d}v{:02d}'.format(results[1+i*2], results[2+i*2])
