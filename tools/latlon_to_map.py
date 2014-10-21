#! /usr/bin/env python

'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Allows a user to convert latitude and longitude coordinate values to map
  coordinate values in the specified projection.

Note:
  Not tested with all projections.  Assumption is that GDAL does things
  correctly when a valid projection is provided.

History:
  Created Sept/2014 by Ron Dilley, USGS/EROS
'''

import sys
from cStringIO import StringIO
from argparse import ArgumentParser
from osgeo import gdal, osr
import logging


if __name__ == '__main__':

    description = "Convert geographic coordinates to map coordinates"
    parser = ArgumentParser(description=description)

    parser.add_argument('--latitude', action='store', dest='latitude',
                        required=True, help="geographic latitude value")

    parser.add_argument('--longitude', action='store', dest='longitude',
                        required=True, help="geographic longitude value")

    parser.add_argument('--zone', action='store', dest='zone',
                        required=True, help="utm zone to project to")

    parser.add_argument('--south', action='store_true', dest='south',
                        default=False, help="use the southern utm zone")

    parser.add_argument('--proj4', action='store', dest='proj4',
                        default=None, help="proj4 projection parameters")

    # Get the command line arguments
    args = parser.parse_args()

    # Configure the logging
    format = ('%(asctime)s.%(msecs)03d %(process)d'
              ' %(levelname)-8s'
              ' %(filename)s:%(lineno)d:%(funcName)s'
              ' -- %(message)s')
    datefmt = '%Y-%m-%d %H:%M:%S'
    level = logging.DEBUG

    logging.basicConfig(format=format, datefmt=datefmt, level=level)

    logger = logging.getLogger(__name__)

    # Setup the geographic projection parameters using a proj4 string and
    # default the target projection
    geo_proj4 = "+proj=longlat +datum=WGS84 +no_defs"
    target_proj4 = None

    if args.proj4 is None:
        # Assume the user want one of the UTM projections
        target_proj4 = ("+proj=utm +zone=%d +datum=WGS84 +units=m +no_defs"
                        % int(args.zone))

        if args.south:
            # Southern UTM projection was requested
            target_proj4 = ("+proj=utm +zone=%d +south +datum=WGS84 +units=m"
                            " +no_defs" % int(args.zone))
    else:
        target_proj4 = args.proj4

    logger.info("Using source [%s]" % geo_proj4)
    logger.info("Using target [%s]" % target_proj4)

    # Create and initialize the target SRS
    target_srs = osr.SpatialReference()
    target_srs.ImportFromProj4(target_proj4)

    # Create and initialize the source SRS
    geo_srs = osr.SpatialReference()
    geo_srs.ImportFromProj4(geo_proj4)

    # Create the transformation object
    transform = osr.CoordinateTransformation(geo_srs, target_srs)

    # Transform the point
    (map_x, map_y, map_z) = transform.TransformPoint(float(args.longitude),
                                                     float(args.latitude))

    logger.info("Map X [%f]" % map_x)
    logger.info("Map Y [%f]" % map_y)

    sys.exit(0)
