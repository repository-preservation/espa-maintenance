#! /usr/bin/env python

import sys
from cStringIO import StringIO
from argparse import ArgumentParser
from osgeo import gdal, osr
import numpy as np

ONE_METER_IN_DD = (0.0002695/30.0)


# ============================================================================
def projection_minbox(ul_lon, ul_lat, lr_lon, lr_lat,
                      target_proj4, pixel_size, pixel_size_units):
    '''
    Description:
      Determines the minimum box in map coordinates that contains the
      geographic coordinates.  Minimum and maximum extent values are returned
      in map coordinates.

    Parameters:
      ul_lon       = Upper Left longitude in decimal degrees
      ul_lat       = Upper Left latitude in decimal degrees
      lr_lon       = Lower Right longitude in decimal degrees
      lr_lat       = Lower Right latitude in decimal degrees
      target_proj4 = The user supplied target proj4 string
      pixel_size   = The target pixel size in meters used to step along the
                     projected area boundary
      pixel_size_units = The units the pixel size is in 'dd' or 'meters'

    Returns:
        (min_x, min_y, max_x, max_y) in meters
    '''

    # We are always going to be geographic
    source_proj4 = "+proj=longlat +datum=WGS84 +no_defs"

    print("Using source [%s]" % source_proj4)
    print("Using target [%s]" % target_proj4)

    # Create and initialize the source SRS
    source_srs = osr.SpatialReference()
    source_srs.ImportFromProj4(source_proj4)

    # Create and initialize the target SRS
    target_srs = osr.SpatialReference()
    target_srs.ImportFromProj4(target_proj4)

    # Create the transformation object
    transform = osr.CoordinateTransformation(source_srs, target_srs)

    # Determine the step in decimal degrees
    step = pixel_size
    if pixel_size_units == 'meters':
        # Convert it to decimal degrees
        step = ONE_METER_IN_DD * pixel_size

    # Determine the lat and lon values to iterate over
    longitudes = np.arange(ul_lon, lr_lon, step, np.float)
    latitudes = np.arange(lr_lat, ul_lat, step, np.float)

    # Initialization using the two corners
    (ul_x, ul_y, z) = transform.TransformPoint(ul_lon, ul_lat)
    (lr_x, lr_y, z) = transform.TransformPoint(lr_lon, lr_lat)

    min_x = min(ul_x, lr_x)
    max_x = max(ul_x, lr_x)
    min_y = min(ul_y, lr_y)
    max_y = max(ul_y, lr_y)

    print('Direct translation of the provided geographic coordinates')
    print('min_x', 'min_y', 'max_x', 'max_y')
    print("(%.4lf, %.4lf, %.4lf, %.4lf)" % (min_x, min_y, max_x, max_y))

    # Walk across the top and bottom of the geographic coordinates
    for lon in longitudes:
        # Upper side
        (ux, uy, z) = transform.TransformPoint(lon, ul_lat)

        # Lower side
        (lx, ly, z) = transform.TransformPoint(lon, lr_lat)

        min_x = min(ux, lx, min_x)
        max_x = max(ux, lx, max_x)
        min_y = min(uy, ly, min_y)
        max_y = max(uy, ly, max_y)

    # Walk along the left and right of the geographic coordinates
    for lat in latitudes:
        # Left side
        (lx, ly, z) = transform.TransformPoint(ul_lon, lat)

        # Right side
        (rx, ry, z) = transform.TransformPoint(lr_lon, lat)

        min_x = min(rx, lx, min_x)
        max_x = max(rx, lx, max_x)
        min_y = min(ry, ly, min_y)
        max_y = max(ry, ly, max_y)

    del(transform)
    del(source_srs)
    del(target_srs)

    print('Map coordinates after minbox determination')
    print('min_x', 'min_y', 'max_x', 'max_y')
    print("(%.4lf, %.4lf, %.4lf, %.4lf)" % (min_x, min_y, max_x, max_y))

    return (min_x, min_y, max_x, max_y)
# END - projection_minbox


# ============================================================================
if __name__ == '__main__':

    description = ("Convert geographic bounding box coordinates to map"
                   " projection minbox values.  You can specify a UTM"
                   " north/south zone using the command line or provide"
                   " your own proj4 string to use.  Stepping is based upon a"
                   " value of 0.0002695(decimal degrees) for 30.0(meters)")
    parser = ArgumentParser(description=description)

    parser.add_argument('--ul_lon', action='store', dest='ul_lon',
                        required=True,
                        help="geographic upper left longitude")

    parser.add_argument('--ul_lat', action='store', dest='ul_lat',
                        required=True,
                        help="geographic upper left latitude")

    parser.add_argument('--lr_lon', action='store', dest='lr_lon',
                        required=True,
                        help="geographic lower right longitude")

    parser.add_argument('--lr_lat', action='store', dest='lr_lat',
                        required=True,
                        help="geographic lower right latitude")

    parser.add_argument('--zone', action='store', dest='zone',
                        required=True, help="utm zone to project to")

    parser.add_argument('--south', action='store_true', dest='south',
                        default=False, help="use the southern utm zone")

    parser.add_argument('--pixel_size', action='store',
                        dest='pixel_size',
                        required=False,
                        default=30.0,
                        help=("output pixel size in meters, used to step along"
                              " the projected area boundary to determine the"
                              " min and max extents"))

    parser.add_argument('--pixel_size_units', action='store',
                        dest='pixel_size_units',
                        required=False,
                        default='meters',
                        help="units for the pixel size value")

    parser.add_argument('--target_proj4', action='store',
                        dest='target_proj4',
                        default=None,
                        help="target proj4 projection parameters")

    # Get the command line arguments
    args = parser.parse_args()

    # Setup the geographic projection parameters using a proj4 string and
    # default the target projection
    target_proj4 = None

    if args.target_proj4 is None:
        # Assume the user wants one of the UTM projections
        target_proj4 = ("+proj=utm +zone=%d +datum=WGS84 +units=m +no_defs"
                        % int(args.zone))

        if args.south:
            # Southern UTM projection was requested
            target_proj4 = ("+proj=utm +zone=%d +south +datum=WGS84 +units=m"
                            " +no_defs" % int(args.zone))
    else:
        target_proj4 = args.target_proj4

    # Get the minbox values
    (min_x, min_y, max_x, max_y) = projection_minbox(float(args.ul_lon),
                                                     float(args.ul_lat),
                                                     float(args.lr_lon),
                                                     float(args.lr_lat),
                                                     target_proj4,
                                                     float(args.pixel_size),
                                                     args.pixel_size_units)

    sys.exit(0)
