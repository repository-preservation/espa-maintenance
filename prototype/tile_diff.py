#! /usr/bin/env python

'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  This loads tiles into memory and compares them against each other for every
  combination.  Only two tiles are in memory at a time.

  Provides tile_diff method for usage as a module.

History:
  Created Nov/2013 by Ron Dilley, USGS/EROS
'''

import os
import sys
import numpy as np
from argparse import ArgumentParser

from espa_constants import *
from espa_logging import log

def tile_diff(tile_data_1, tile_data_2):
    return (not np.array_equal(tile_data_1, tile_data_2))
# END - tile_difference


'''
TODO TODO TODO - Need to add more of these data types in to the processing
TODO TODO TODO - Need to add more of these data types in to the processing
TODO TODO TODO - Need to add more of these data types in to the processing
TODO TODO TODO - Need to add more of these data types in to the processing

np.bool         Boolean (True or False) stored as a byte
np.int          Platform integer (normally either int32 or int64)
np.int8         Byte (-128 to 127)
np.int16        Integer (-32768 to 32767)
np.int32        Integer (-2147483648 to 2147483647)
np.int64        Integer (9223372036854775808 to 9223372036854775807)
np.uint8        Unsigned integer (0 to 255)
np.uint16       Unsigned integer (0 to 65535)
np.uint32       Unsigned integer (0 to 4294967295)
np.uint64       Unsigned integer (0 to 18446744073709551615)
np.float        Shorthand for float64.
np.float16      Half precision float: sign bit, 5 bits exponent, 10 bits
                mantissa
np.float32      Single precision float: sign bit, 8 bits exponent, 23 bits
                mantissa
np.float64      Double precision float: sign bit, 11 bits exponent, 52 bits
                mantissa
np.complex      Shorthand for complex128.
np.complex64    Complex number, represented by two 32-bit floats
                (real and imaginary components)
np.complex128   Complex number, represented by two 64-bit floats
                (real and imaginary components)
'''
#=============================================================================
if __name__ == '__main__':
    '''
    Description:
      If running this script manually provides arguments on the command line
      for diffing tiles.
    '''

    # Create a command line argument parser
    parser = ArgumentParser(usage="%(prog)s [options]")

    parser.add_argument('--tile', '--tile_filename',
        action='append', dest='tiles',
        help="specify tiles to be processed; can specify multiple on the" \
            " command line")

    # Parse the command line
    args = parser.parse_args()

    # Verify that some tiles are present
    if args.tiles == None:
        log ("Missing '--tile' parameters for processing")
        sys.exit(EXIT_FAILURE)

    # Get the count of tiles to help with later processing
    tile_count = len(args.tiles)

    # Generate difference information for each combination of tiles
    if tile_count < 2:
        log ("Need at least two tiles for performing a diff")
        sys.exit(EXIT_FAILURE)

    # Kinda ridiculous to diff more than five files maybe even three
    if tile_count > 5:
        log ("Diff is limited to no more than five tiles")
        sys.exit(EXIT_FAILURE)

    # Actually do diffs
    for tile_index_1 in range(tile_count):
        fd = open(args.tiles[tile_index_1], 'rb')
        tile_data_1 = np.fromfile(file=fd, dtype=np.int16)
        fd.close()

        for tile_index_2 in range(tile_index_1, tile_count):
            fd = open(args.tiles[tile_index_2], 'rb')
            tile_data_2 = np.fromfile(file=fd, dtype=np.int16)
            fd.close()

            if tile_index_1 != tile_index_2:
                diff_result = tile_diff(tile_data_1, tile_data_2)
                if diff_result == True:
                    print "%s and %s differ" % \
                        (args.tiles[tile_index_1], args.tiles[tile_index_2])
                else:
                    print "%s and %s are same" % \
                        (args.tiles[tile_index_1], args.tiles[tile_index_2])
        # END - for tile_index_2
    # END - for tile_index_1

    sys.exit(EXIT_SUCCESS)

