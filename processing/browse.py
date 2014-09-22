#! /usr/bin/env python

'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Create a browse image from the surface reflectance product.
  All operations assume everything is in the current directory.

History:
  Original Development (cdr_ecv.py) by David V. Hill, USGS/EROS
  Created Jan/2014 by Ron Dilley, USGS/EROS
    - Gutted the original implementation from cdr_ecv.py and placed it in this
      file.
'''

# TODO - Currently we are not generating browse products.  And this code needs
#        to be updated for the raw_binary format and processing.  Commented
#        gdal commands have been added that may be the solution for various
#        steps, or will be close to the final solution.
#        This code should also be modified to cleanup all of it's temporary
#        files.  The calling code should only see the product.

import os
import sys
import glob

# espa-common objects and methods
from espa_constants import *

# imports from espa/espa_common
try:
    from espa_logging import EspaLogging
except:
    from espa_common.espa_logging import EspaLogging

try:
    import settings
except:
    from espa_common import settings

# local objects and methods
import util


# TODO - At some point in the future we should allow browse generation from
#        L1T, and TOA as well.  Should also consider support for applying
#        color ramps against indices.
# ============================================================================
def do_sr_browse(sr_filename, scene,
                 resolution=settings.DEFAULT_BROWSE_RESOLUTION):
    '''
    Description:
      Creates a browse image from the surface relfectance file
    '''

    logger = EspaLogging.get_logger('espa.processing')

    logger.info("Creating browse product")

    browse_filename = "%s-sr-browse.tif" % scene

    # ------------------------------------------------------------------------
    # Convert to GeoTIFF
    cmd = ['gdal_translate',
           '-a_nodata', '-9999',
           '-a_nodata', '12000',
           '-of', 'GTiff',
           '-sds', sr_filename, 'out.tiff']
    cmd = ' '.join(cmd)
    logger.info(' '.join(['Running:', cmd]))
    output = util.execut_cmd(cmd)
    if len(output) > 0:
        logger.info(output)

    # ------------------------------------------------------------------------
    # Scale each browse band to 8bit data range
    base_translate_cmd = ['gdal_translate',
                          '-ot', 'Byte',
                          '-scale', '0', '10000', '0', '255',
                          '-of', 'GTIFF']
# gdal_translate -ot Byte -scale 0 10000 0 255 -of ENVI
# LT50460282002042EDC01_toa_band5.img browse_5.img
    cmd = ' '.join([base_translate_cmd, 'out.tiff5', 'browse.tiff5'])
    logger.info(' '.join(['Running:', cmd]))
    output = util.execut_cmd(cmd)
    if len(output) > 0:
        logger.info(output)

# gdal_translate -ot Byte -scale 0 10000 0 255 -of ENVI
# LT50460282002042EDC01_toa_band4.img browse_4.img
    cmd = ' '.join([base_translate_cmd, 'out.tiff4', 'browse.tiff4'])
    logger.info(' '.join(['Running:', cmd]))
    output = util.execut_cmd(cmd)
    if len(output) > 0:
        logger.info(output)

# gdal_translate -ot Byte -scale 0 10000 0 255 -of ENVI
# LT50460282002042EDC01_toa_band3.img browse_3.img
    cmd = ' '.join([base_translate_cmd, 'out.tiff3', 'browse.tiff3'])
    logger.info(' '.join(['Running:', cmd]))
    output = util.execut_cmd(cmd)
    if len(output) > 0:
        logger.info(output)

    # ------------------------------------------------------------------------
    # Create the 3 band composite
# gdal_merge_simple -of ENVI -in browse_5.img -in browse_4.img
# -in browse_3.img -out final.img
    cmd = ' '.join(['gdal_merge_simple',
                    '-in', 'browse.tiff5',
                    '-in', 'browse.tiff4',
                    '-in', 'browse.tiff3',
                    '-out', 'final.tif'])
    logger.info(' '.join(['Running:', cmd]))
    output = util.execut_cmd(cmd)
    if len(output) > 0:
        logger.info(output)

    # ------------------------------------------------------------------------
    # Project to geographic
# gdalwarp -of ENVI -dstalpha -srcnodata 0 -t_srs EPSG:4326 final.img
# warped.img
    cmd = ' '.join(['gdalwarp',
                    '-dstalpha',
                    '-srcnodata', '0',
                    '-t_srs', 'EPSG:4326',
                    'final.tif', 'warped.tif'])
    logger.info(' '.join(['Running:', cmd]))
    output = util.execut_cmd(cmd)
    if len(output) > 0:
        logger.info(output)

    # ------------------------------------------------------------------------
    # Resize and rename
# gdal_translate -ot INT16 -of ENVI -outsize 50 50 -a_nodata -9999
# warped.img browse.img

# Should probably use gdalwarp to set the resolution, because outsize in
# gdal_translate is a percentage.  This step may not even be needed then,
# because it could be handled in the previous gdalwarp step.
    cmd = ' '.join(['gdal_translate',
                    '-co', 'COMPRESS=DEFLATE',
                    '-co', 'PREDICTOR=2',
                    '-outsize', str(resolution), str(resolution),
                    '-a_nodata', '-9999',
                    '-of', 'GTIFF',
                    'warped.tif', browse_filename])
    logger.info(' '.join(['Running:', cmd]))
    output = util.execut_cmd(cmd)
    if len(output) > 0:
        logger.info(output)

    # ------------------------------------------------------------------------
    # Cleanup
    remove_files = ['warped.tif', 'final.tif']
    remove_files.extend(glob.glob('*tiff*'))

    cmd = ' '.join(['rm', '-rf'] + remove_files)
    logger.info(' '.join(['Running:', cmd]))
    output = util.execut_cmd(cmd)
    if len(output) > 0:
        logger.info(output)

    logger.info("Browse product generation complete...")
# END - do_sr_browse
