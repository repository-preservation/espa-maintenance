
'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Provides routines for interfacing with parameters in a dictionary.

History:
  Created Jan/2014 by Ron Dilley, USGS/EROS
'''

import os

# espa-common objects and methods
from espa_constants import *

# imports from espa/espa_common
try:
    from logger_factory import EspaLogging
except:
    from espa_common.logger_factory import EspaLogging

try:
    import sensor
except:
    from espa_common import sensor


# This contains the valid sensors and data types which are supported
valid_landsat_sensors = ['tm', 'etm']
valid_modis_sensors = ['terra', 'aqua']
valid_sensors = valid_landsat_sensors + valid_modis_sensors
valid_output_formats = ['envi', 'gtiff', 'hdf-eos2']


# ============================================================================
def add_orderid_parameter(parser):
    '''
    Description:
      Adds the orderid parameter to the command line parameters
    '''

    parser.add_argument('--orderid',
                        action='store', dest='orderid', required=True,
                        help="order ID associated with this request")
# END - add_orderid_parameter


# ============================================================================
def add_scene_parameter(parser):
    '''
    Description:
      Adds the scene parameter to the command line parameters
    '''

    parser.add_argument('--scene',
                        action='store', dest='scene', required=True,
                        help="scene ID to process")
# END - add_scene_parameter


# ============================================================================
def add_product_type_parameter(parser):
    '''
    Description:
      Adds the product_type parameter to the command line parameters
    '''

    parser.add_argument('--product_type',
                        action='store', dest='product_type', required=True,
                        help="the type of product to process")
# END - add_product_type_parameter


# ============================================================================
def add_work_directory_parameter(parser):
    '''
    Description:
      Adds the work_directory parameter to the command line parameters
    '''

    parser.add_argument('--work_directory',
                        action='store', dest='work_directory',
                        default=os.curdir,
                        help="work directory on the localhost")
# END - add_work_directory_parameter


# ============================================================================
def add_debug_parameter(parser):
    '''
    Description:
      Adds the debug parameter to the command line parameters
    '''

    parser.add_argument('--debug',
                        action='store_true', dest='debug', default=False,
                        help="turn debug logging on")
# END - add_debug_parameter


# ============================================================================
def add_keep_log_parameter(parser):
    '''
    Description:
      Adds the keep_log parameter to the command line parameters
    '''

    parser.add_argument('--keep_log',
                        action='store_true', dest='keep_log', default=False,
                        help="keep the log file")

# END - add_keep_log_parameter


# ============================================================================
def add_include_source_data_parameter(parser):
    '''
    Description:
      Adds the include_source_data parameter to the command line parameters
    '''

    parser.add_argument('--include_source_data',
                        action='store_true', dest='include_source_data',
                        default=False,
                        help="include source data in final product")
# END - add_include_source_data_parameter


# ============================================================================
def add_include_source_metadata_parameter(parser):
    '''
    Description:
      Adds the include_source_metadata parameter to the command line
      parameters
    '''

    parser.add_argument('--include_source_metadata',
                        action='store_true', dest='include_source_metadata',
                        default=False,
                        help="include source metadata in final product")
# END - add_include_source_metadata_parameter


# ============================================================================
def add_science_product_parameters(parser):
    '''
    Description:
      Adds the science product parameters to the command line parameters
    '''

    parser.add_argument('--include_customized_source_data',
                        action='store_true',
                        dest='include_customized_source_data',
                        default=False,
                        help="include radiance product")

    parser.add_argument('--include_sr',
                        action='store_true', dest='include_sr',
                        default=False,
                        help="build surface reflectance product")

    parser.add_argument('--include_sr_toa',
                        action='store_true', dest='include_sr_toa',
                        default=False,
                        help="build top of atmosphere product")

    parser.add_argument('--include_sr_thermal',
                        action='store_true', dest='include_sr_thermal',
                        default=False,
                        help="build SR thermal product")

    parser.add_argument('--include_sr_browse',
                        action='store_true', dest='include_sr_browse',
                        default=False,
                        help="build SR browse product")

    parser.add_argument('--include_cfmask',
                        action='store_true', dest='include_cfmask',
                        default=False,
                        help="build cfmask product")

    parser.add_argument('--include_sr_nbr',
                        action='store_true', dest='include_sr_nbr',
                        default=False,
                        help="build SR NBR index")

    parser.add_argument('--include_sr_nbr2',
                        action='store_true', dest='include_sr_nbr2',
                        default=False,
                        help="build SR NBR2 index")

    parser.add_argument('--include_sr_ndvi',
                        action='store_true', dest='include_sr_ndvi',
                        default=False,
                        help="build SR NDVI index")

    parser.add_argument('--include_sr_ndmi',
                        action='store_true', dest='include_sr_ndmi',
                        default=False,
                        help="build SR NDMI index")

    parser.add_argument('--include_sr_savi',
                        action='store_true', dest='include_sr_savi',
                        default=False,
                        help="build SR SAVI index")

    parser.add_argument('--include_sr_msavi',
                        action='store_true', dest='include_sr_msavi',
                        default=False,
                        help="build SR MSAVI index")

    parser.add_argument('--include_sr_evi',
                        action='store_true', dest='include_sr_evi',
                        default=False,
                        help="build SR EVI index")

    parser.add_argument('--include_dswe',
                        action='store_true', dest='include_dswe',
                        default=False,
                        help="build surface water extent product")

    parser.add_argument('--include_solr_index',
                        action='store_true', dest='include_solr_index',
                        default=False,
                        help="build SOLR index product")
# END - add_science_product_parameters


# ============================================================================
def add_include_statistics_parameter(parser):
    '''
    Description:
      Adds the include_statistics parameter to the command line parameters
    '''

    parser.add_argument('--include_statistics',
                        action='store_true', dest='include_statistics',
                        default=False,
                        help="compute minimum, maximum, mean, and stddev"
                             " values for each appropriate science product")
# END - add_include_statistics_parameter


# ============================================================================
def add_output_format_parameter(parser, output_formats):
    '''
    Description:
      Adds the data_source parameter to the command line parameters with
      specific choices
    '''

    parser.add_argument('--output_format',
                        action='store', dest='output_format',
                        default='envi',
                        choices=output_formats,
                        help="one of %s" % ', '.join(output_formats))
# END - add_output_format_parameter


# ============================================================================
def add_source_parameters(parser):
    '''
    Description:
      Adds the source host and directory parameters to the command line
      parameters
    '''

    parser.add_argument('--source_host',
                        action='store', dest='source_host',
                        default='localhost',
                        help="source host for the location of the data")

    parser.add_argument('--source_username',
                        action="store", dest="source_username",
                        default=None,
                        help="source ftp server username")

    parser.add_argument('--source_pw',
                        action="store", dest="source_pw",
                        default=None,
                        help="source ftp server password")

    parser.add_argument('--source_directory',
                        action='store', dest='source_directory',
                        default=None,
                        help="directory on the source host")
# END - add_source_parameters


# ============================================================================
def add_destination_parameters(parser):
    '''
    Description:
      Adds the destination host and directory parameters to the command line
      parameters
    '''

    parser.add_argument('--destination_host',
                        action='store', dest='destination_host',
                        default='localhost',
                        help="destination host for the location of the data")

    parser.add_argument('--destination_username',
                        action="store", dest="destination_username",
                        default=None,
                        help="destination ftp server username")

    parser.add_argument('--destination_pw',
                        action="store", dest="destination_pw",
                        default=None,
                        help="destination ftp server password")

    parser.add_argument('--destination_directory',
                        action='store', dest='destination_directory',
                        default=os.curdir,
                        help="directory on the destination host")
# END - add_destination_parameters


# ============================================================================
def add_std_plotting_parameters(parser, bg_color, marker, marker_size):
    '''
    Description:
      Adds the destination host and directory parameters to the command line
      parameters
    '''

    parser.add_argument('--bg_color',
                        action='store', dest='bg_color', default=bg_color,
                        help="color specification for plot and legend"
                             " background")

    parser.add_argument('--marker',
                        action='store', dest='marker', default=marker,
                        help="marker specification for plotted points")

    parser.add_argument('--marker_size',
                        action='store', dest='marker_size',
                        default=marker_size,
                        help="marker size specification for plotted points")

# END - add_std_plotting_parameters


# ============================================================================
def add_reprojection_parameters(parser, projection_values, ns_values,
                                pixel_size_units, image_extents_units,
                                resample_methods, datum_values):
    '''
    Description:
      Adds the reprojection parameters to the command line parameters
    '''

    parser.add_argument('--projection',
                        action='store', dest='projection', default=None,
                        help="proj.4 string for desired output product"
                             " projection")

    parser.add_argument('--reproject',
                        action='store_true', dest='reproject', default=False,
                        help="perform reprojection on the products")

    parser.add_argument('--target_projection',
                        action='store', dest='target_projection',
                        choices=projection_values,
                        help="one of (%s)" % ', '.join(projection_values))

    parser.add_argument('--utm_zone',
                        action='store', dest='utm_zone',
                        help="UTM zone to use")
    parser.add_argument('--utm_north_south',
                        action='store', dest='utm_north_south',
                        choices=ns_values,
                        help="one of (%s)" % ', '.join(ns_values))

    # Default to the first entry which should be WGS84
    parser.add_argument('--datum',
                        action='store', dest='datum', default=datum_values[0],
                        help="one of (%s), only used with albers projection"
                             % ', '.join(datum_values))

    parser.add_argument('--longitude_pole',
                        action='store', dest='longitude_pole',
                        help="longitude of the pole projection parameter")

    parser.add_argument('--latitude_true_scale',
                        action='store', dest='latitude_true_scale',
                        help="latitude true of scale projection parameter")

    parser.add_argument('--origin_lat',
                        action='store', dest='origin_lat',
                        help="origin of latitude projection parameter")

    parser.add_argument('--central_meridian',
                        action='store', dest='central_meridian',
                        help="central meridian projection parameter")

    parser.add_argument('--std_parallel_1',
                        action='store', dest='std_parallel_1',
                        help="first standard parallel projection parameter")
    parser.add_argument('--std_parallel_2',
                        action='store', dest='std_parallel_2',
                        help="second standard parallel projection parameter")

    parser.add_argument('--false_northing',
                        action='store', dest='false_northing',
                        help="false northing projection parameter")
    parser.add_argument('--false_easting',
                        action='store', dest='false_easting',
                        help="false easting projection parameter")

    parser.add_argument('--resize',
                        action='store_true', dest='resize', default=False,
                        help="perform resizing of the pixels on the products")
    parser.add_argument('--pixel_size',
                        action='store', dest='pixel_size',
                        help="desired pixel size for output products")
    parser.add_argument('--pixel_size_units',
                        action='store', dest='pixel_size_units',
                        choices=pixel_size_units,
                        help="units pixel size is specified in: one of (%s)"
                             % ', '.join(pixel_size_units))

    parser.add_argument('--image_extents',
                        action='store_true', dest='image_extents',
                        default=False,
                        help="specify desired output image extents")
    parser.add_argument('--image_extents_units',
                        action='store', dest='image_extents_units',
                        choices=pixel_size_units,
                        help=("units image extents are specified in:"
                              " one of (%s)"
                              % ', '.join(image_extents_units)))

    parser.add_argument('--minx',
                        action='store', dest='minx',
                        help="minimum X for the image extent")
    parser.add_argument('--miny',
                        action='store', dest='miny',
                        help="minimum Y for the image extent")
    parser.add_argument('--maxx',
                        action='store', dest='maxx',
                        help="maximum X for the image extent")
    parser.add_argument('--maxy',
                        action='store', dest='maxy',
                        help="maximum Y for the image extent")

    parser.add_argument('--resample_method',
                        action='store', dest='resample_method', default='near',
                        choices=resample_methods,
                        help="one of (%s)" % ', '.join(resample_methods))
# END - add_reprojection_parameters


# ============================================================================
def test_for_parameter(parms, key):
    '''
    Description:
      Tests to see if a specific parameter is present.

    Returns:
       True - If the parameter is present in the dictionary
      False - If the parameter is *NOT* present in the dictionary or does not
              have a value
    '''

    if (key not in parms) or (parms[key] == '') or (parms[key] is None):
        return False

    return True
# END - test_for_parameter


# ============================================================================
def convert_to_command_line_options(parms):
    '''
    Description:
      As simply stated in the routine name... Convert the JSON dictionary
      version of the parameters into command line parameters to use with the
      executables that will be called.
    '''

    cmd_line = ['--orderid', '\"%s\"' % parms['orderid']]

    if test_for_parameter(parms, 'scene'):
        cmd_line.extend(['--scene', '\"%s\"' % parms['scene']])

    if test_for_parameter(parms, 'product_type'):
        p_type = parms['product_type']
        if p_type != 'plot':
            cmd_line.extend(['--product_type', '\"%s\"' % p_type])
        else:
            # Plotting doesn't need this command line parameter
            pass

    for (key, value) in parms['options'].items():
        if value is True:
            cmd_line.append('--%s' % key)
        elif value is not False and value is not None:
            cmd_line.extend(['--%s' % key, '\"%s\"' % value])

    return cmd_line
# END - convert_parms_to_command_line_options


# ============================================================================
def validate_reprojection_parameters(parms, scene, projections, ns_values,
                                     pixel_size_units, image_extents_units,
                                     resample_methods, datum_values):
    '''
    Description:
      Perform a check on the possible reprojection parameters

    Note:
      We blindly convert values to float or int without checking them.  It is
      assumed that the web tier has validated them.
    '''

    logger = EspaLogging.get_logger('espa.processing')

    # Create this and set to None if not present
    if not test_for_parameter(parms, 'projection'):
        logger.warning("'projection' parameter missing defaulting to None")
        parms['projection'] = None

    # Create this and set to 'near' if not present
    if not test_for_parameter(parms, 'resample_method'):
        logger.warning("'resample_method' parameter missing defaulting to"
                       " near")
        parms['resample_method'] = 'near'

    # Make sure these have at least a False value
    required_parameters = ['reproject', 'image_extents', 'resize']

    for parameter in required_parameters:
        if not test_for_parameter(parms, parameter):
            logger.warning("'%s' parameter missing defaulting to False"
                           % parameter)
            parms[parameter] = False

    if parms['reproject']:
        if not test_for_parameter(parms, 'target_projection'):
            raise RuntimeError("Missing target_projection parameter")
        else:
            # Convert to lower case
            target_projection = parms['target_projection'].lower()
            parms['target_projection'] = target_projection

            # Verify a valid projection
            if target_projection not in projections:
                raise ValueError("Invalid target_projection [%s]:"
                                 " Argument must be one of (%s)"
                                 % (target_projection, ', '.join(projections)))

            # ................................................................
            if target_projection == "sinu":
                if not test_for_parameter(parms, 'central_meridian'):
                    raise RuntimeError("Missing central_meridian parameter")
                else:
                    parms['central_meridian'] = \
                        float(parms['central_meridian'])
                if not test_for_parameter(parms, 'false_easting'):
                    raise RuntimeError("Missing false_easting parameter")
                else:
                    parms['false_easting'] = float(parms['false_easting'])
                if not test_for_parameter(parms, 'false_northing'):
                    raise RuntimeError("Missing false_northing parameter")
                else:
                    parms['false_northing'] = float(parms['false_northing'])

                if not test_for_parameter(parms, 'datum'):
                    parms['datum'] = None

            # ................................................................
            if target_projection == 'aea':
                if not test_for_parameter(parms, 'std_parallel_1'):
                    raise RuntimeError("Missing std_parallel_1 parameter")
                else:
                    parms['std_parallel_1'] = float(parms['std_parallel_1'])
                if not test_for_parameter(parms, 'std_parallel_2'):
                    raise RuntimeError("Missing std_parallel_2 parameter")
                else:
                    parms['std_parallel_2'] = float(parms['std_parallel_2'])
                if not test_for_parameter(parms, 'origin_lat'):
                    raise RuntimeError("Missing origin_lat parameter")
                else:
                    parms['origin_lat'] = float(parms['origin_lat'])
                if not test_for_parameter(parms, 'central_meridian'):
                    raise RuntimeError("Missing central_meridian parameter")
                else:
                    parms['central_meridian'] = \
                        float(parms['central_meridian'])
                if not test_for_parameter(parms, 'false_easting'):
                    raise RuntimeError("Missing false_easting parameter")
                else:
                    parms['false_easting'] = float(parms['false_easting'])
                if not test_for_parameter(parms, 'false_northing'):
                    raise RuntimeError("Missing false_northing parameter")
                else:
                    parms['false_northing'] = float(parms['false_northing'])

                # The datum must be in uppercase for the processing code to
                # work so if it is present here, we force it
                if not test_for_parameter(parms, 'datum'):
                    raise RuntimeError("Missing datum parameter")
                else:
                    parms['datum'] = parms['datum'].upper()
                if parms['datum'] not in datum_values:
                    raise ValueError("Invalid datum [%s]:"
                                     " Argument must be one of (%s)"
                                     % (parms['datum'],
                                        ', '.join(datum_values)))

            # ................................................................
            if target_projection == 'utm':
                if not test_for_parameter(parms, 'utm_zone'):
                    raise RuntimeError("Missing utm_zone parameter")
                else:
                    zone = int(parms['utm_zone'])
                    if zone < 0 or zone > 60:
                        raise ValueError("Invalid utm_zone [%d]:"
                                         " Value must be 0-60" % zone)
                    parms['utm_zone'] = zone
                if not test_for_parameter(parms, 'utm_north_south'):
                    raise RuntimeError("Missing utm_north_south parameter")
                elif parms['utm_north_south'] not in ns_values:
                    raise ValueError("Invalid utm_north_south [%s]:"
                                     " Argument must be one of (%s)"
                                     % (parms['utm_north_south'],
                                        ', '.join(ns_values)))

                if not test_for_parameter(parms, 'datum'):
                    parms['datum'] = None

            # ................................................................
            if target_projection == 'ps':
                if not test_for_parameter(parms, 'latitude_true_scale'):
                    # Must be tested before origin_lat
                    raise RuntimeError("Missing latitude_true_scale parameter")
                else:
                    value = float(parms['latitude_true_scale'])
                    if ((value < 60.0 and value > -60.0)
                            or value > 90.0 or value < -90.0):
                        raise ValueError("Invalid latitude_true_scale [%f]:"
                                         " Value must be between"
                                         " (-60.0 and -90.0) or"
                                         " (60.0 and 90.0)" % value)
                    parms['latitude_true_scale'] = value
                if not test_for_parameter(parms, 'longitude_pole'):
                    raise RuntimeError("Missing longitude_pole parameter")
                else:
                    parms['longitude_pole'] = float(parms['longitude_pole'])
                if not test_for_parameter(parms, 'origin_lat'):
                    # If the user did not specify the origin_lat value, then
                    # set it based on the latitude true scale
                    lat_ts = float(parms['latitude_true_scale'])
                    if lat_ts < 0:
                        parms['origin_lat'] = -90.0
                    else:
                        parms['origin_lat'] = 90.0
                else:
                    value = float(parms['origin_lat'])
                    if value != -90.0 and value != 90.0:
                        raise ValueError("Invalid origin_lat [%f]:"
                                         " Value must be -90.0 or 90.0"
                                         % value)
                    parms['origin_lat'] = value

                if not test_for_parameter(parms, 'false_easting'):
                    raise RuntimeError("Missing false_easting parameter")
                else:
                    parms['false_easting'] = float(parms['false_easting'])
                if not test_for_parameter(parms, 'false_northing'):
                    raise RuntimeError("Missing false_northing parameter")
                else:
                    parms['false_northing'] = float(parms['false_northing'])

                if not test_for_parameter(parms, 'datum'):
                    parms['datum'] = None

            # ................................................................
            if target_projection == 'lonlat':

                if not test_for_parameter(parms, 'datum'):
                    parms['datum'] = None

    # ------------------------------------------------------------------------
    if parms['resample_method'] not in resample_methods:
        raise ValueError("Invalid resample_method [%s]:"
                         " Argument must be one of (%s)"
                         % (parms['resample_method'],
                            ', '.join(resample_methods)))

    # ------------------------------------------------------------------------
    if parms['image_extents']:
        if not test_for_parameter(parms, 'image_extents_units'):
            raise RuntimeError("Missing image_extents_units parameter")
        else:
            if parms['image_extents_units'] not in image_extents_units:
                raise ValueError("Invalid image_extents_units [%s]:"
                                 " Argument must be one of (%s)"
                                 % (parms['image_extents_units'],
                                    ', '.join(image_extents_units)))
        if not test_for_parameter(parms, 'minx'):
            raise RuntimeError("Missing minx parameter")
        else:
            parms['minx'] = float(parms['minx'])
        if not test_for_parameter(parms, 'miny'):
            raise RuntimeError("Missing miny parameter")
        else:
            parms['miny'] = float(parms['miny'])
        if not test_for_parameter(parms, 'maxx'):
            raise RuntimeError("Missing maxx parameter")
        else:
            parms['maxx'] = float(parms['maxx'])
        if not test_for_parameter(parms, 'maxy'):
            raise RuntimeError("Missing maxy parameter")
        else:
            parms['maxy'] = float(parms['maxy'])
    else:
        # Default these
        parms['minx'] = None
        parms['miny'] = None
        parms['maxx'] = None
        parms['maxy'] = None
        parms['image_extents_units'] = None

    # ------------------------------------------------------------------------
    if parms['resize']:
        if not test_for_parameter(parms, 'pixel_size'):
            raise RuntimeError("Missing pixel_size parameter")
        else:
            parms['pixel_size'] = float(parms['pixel_size'])
        if not test_for_parameter(parms, 'pixel_size_units'):
            raise RuntimeError("Missing pixel_size_units parameter")
        else:
            if parms['pixel_size_units'] not in pixel_size_units:
                raise ValueError("Invalid pixel_size_units [%s]:"
                                 " Argument must be one of (%s)"
                                 % (parms['pixel_size_units'],
                                    ', '.join(pixel_size_units)))
    else:
        # Default this
        parms['pixel_size'] = None
        parms['pixel_size_units'] = None

    # ------------------------------------------------------------------------
    if ((parms['reproject'] or parms['image_extents'])
            and not parms['resize']):
        # Sombody asked for reproject or extents, but didn't specify a pixel
        # size

        units = 'meters'
        if parms['reproject'] and parms['target_projection'] == 'lonlat':
            units = 'dd'

        # Default to the sensor specific meters or dd equivalent
        parms['pixel_size'] = sensor.instance(scene).default_pixel_size[units]
        parms['pixel_size_units'] = units

        logger.warning("'resize' parameter not provided but required for"
                       " reprojection or image extents"
                       " (Defaulting pixel_size(%f) and pixel_size_units(%s)"
                       % (parms['pixel_size'], parms['pixel_size_units']))
# END - validate_reprojection_parameters
