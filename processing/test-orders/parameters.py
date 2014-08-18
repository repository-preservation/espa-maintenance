
import os
import sys
import re
import logging
import json
from argparse import ArgumentParser


# Exceptions - TODO TODO TODO This should be else-where
class DeveloperViolation(Exception):
    '''
    Description:
        TODO TODO TODO
    '''
    pass


# Exceptions provided by this method
class ParameterViolation(Exception):
    '''
    Description:
        TODO TODO TODO
    '''
    pass


class OptionViolation(Exception):
    '''
    Description:
        TODO TODO TODO
    '''
    pass


def get_sensor_code(parms):
    '''
    Description:
        TODO TODO TODO
    '''

    if 'scene' not in parms.keys():
        message = "[scene] is missing from order parameters"
        raise ParameterViolation(message)

    _id = parms['scene'].lower().strip()

    sensors = {
        'tm': r'^lt[4|5]\d{3}\d{3}\d{4}\d{3}\w{3}\d{2}$',
        'etm': r'^le7\d{3}\d{3}\d{4}\d{3}\w{3}\d{2}$',

        'mod09a1': r'^mod09a1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
        'mod09ga': r'^mod09ga\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
        'mod09gq': r'^mod09gq\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
        'mod09q1': r'^mod09q1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',

        'mod13a1': r'^mod13a1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
        'mod13a2': r'^mod13a2\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
        'mod13a3': r'^mod13a3\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
        'mod13q1': r'^mod13q1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',

        'myd09a1': r'^myd09a1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
        'myd09ga': r'^myd09ga\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
        'myd09gq': r'^myd09gq\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
        'myd09q1': r'^myd09q1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',

        'myd13a1': r'^myd13a1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
        'myd13a2': r'^myd13a2\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
        'myd13a3': r'^myd13a3\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
        'myd13q1': r'^myd13q1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$'
    }

    for sensor_code in sensors.keys():
        if re.match(sensors[sensor_code], _id):
            return sensor_code

    msg = "[%s] is not a supported sensor product" % parms['scene']
    raise NotImplementedError(msg)
# END - get_sensor_code


def get_default_resize_parameters(sensor_code):
    '''
    Description:
        TODO TODO TODO
    '''

    sensors = {
        'tm': {'pixel_size': 30.0, 'pixel_size_units': 'meters'},
        'etm': {'pixel_size': 30.0, 'pixel_size_units': 'meters'},

        # TODO TODO TODO - I don't know if any modis are correct
        'mod09a1': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},
        'mod09ga': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},
        'mod09gq': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},
        'mod09q1': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},

        'mod13a1': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},
        'mod13a2': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},
        'mod13a3': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},
        'mod13q1': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},

        'myd09a1': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},
        'myd09ga': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},
        'myd09gq': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},
        'myd09q1': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},

        'myd13a1': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},
        'myd13a2': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},
        'myd13a3': {'pixel_size': 500.0, 'pixel_size_units': 'meters'},
        'myd13q1': {'pixel_size': 500.0, 'pixel_size_units': 'meters'}
    }

    for sensor in sensors.keys():
        if sensor == sensor_code:
            return sensors[sensor]

    msg = "[%s] is not a supported sensor" % sensor_code
    raise NotImplementedError(msg)
# END - get_default_resize_parameters


class Parameters(dict):
    '''
    Definition:
      This class provides all of the base functionality for order parameters.
    '''

    valid_parameters = None
    valid_options = None

    def __init__(self, *args, **kwarg):
        '''
        Definition:
          Provides the constructor which is just a pass-through to create the
          underlying dict object.
        '''
        super(Parameters, self).__init__(*args, **kwarg)

    def _is_valid_parameter(self, parameter):
        '''
        Description:
            TODO TODO TODO
        '''

        if not self.valid_parameters:
            message = "You must specify the valid parameters in the sub-class"
            raise DeveloperViolation(message)

        if parameter not in self.valid_parameters:
            message = "[%s] is not a valid parameter" % parameter
            raise ParameterViolation(message)

    def _is_valid_option(self, option):
        '''
        Description:
            TODO TODO TODO
        '''

        if not self.valid_options:
            message = "You must specify the valid options in the sub-class"
            raise DeveloperViolation(message)

        if option not in self.valid_options.keys():
            message = "[%s] is not a valid option" % option
            raise OptionViolation(message)

    def _find_required_parameters(self, parameters):
        '''
        Description:
            TODO TODO TODO
        '''

        if not self.valid_parameters:
            message = "You must specify the valid parameters in the sub-class"
            raise DeveloperViolation(message)

        for parameter in self.valid_parameters:
            if parameter not in parameters:
                message = "[%s] is missing from order parameters" % parameter
                raise ParameterViolation(message)

    def _find_required_options(self, options):
        '''
        Description:
            TODO TODO TODO
        '''

        logger = logging.getLogger()

        if not self.valid_options:
            message = "You must specify the valid options in the sub-class"
            raise DeveloperViolation(message)

        # TODO TODO TODO - Verify assumption
        # I think all of the options can be defaulted so right now this is
        # very simple
        for option in self.valid_options:
            if option not in options:
                message = ("[%s] is missing from order options and will be"
                           " defaulted to [%s]"
                           % (option, str(self.valid_options[option])))
                logger.warning(message)
                self['options'][option] = self.valid_options[option]


class Options(dict):
    '''
    Definition:
      This class provides all of the base functionality for order options.
    '''

    options = None

    def __init__(self, *args, **kwarg):
        '''
        Definition:
          Provides the constructor which is just a pass-through to create the
          underlying dict object.
        '''

        super(Parameters, self).__init__(*args, **kwarg)

    def _is_valid_option(self, option):
        '''
        Description:
            TODO TODO TODO
        '''

        if not self.options:
            message = "You must specify the options in the sub-class"
            raise DeveloperViolation(message)

        if type(self.options) != dict:
            message = "the specified options must be a dict"
            raise DeveloperViolation(message)

        if option not in self.options.keys():
            message = "[%s] is not a valid option" % option
            raise OptionViolation(message)

    # TODO TODO TODO - IMPLEMENT ME


class Projection(object):
    '''
    Description:
        TODO TODO TODO
    '''

    _target_projection = None
    _options = None
    _defaults = None
    _required_options = None

    def __init__(self, *args, **kwarg):
        '''
        Description:
            TODO TODO TODO
        '''

        if self._required_options:
            options = dict(*args, **kwarg)
            self._options = dict()
            for option in self._required_options:
                if option not in options.keys():
                    message = ("[%s] is required for target_projection"
                               % option)
                    raise OptionViolation(message)
                else:
                    option = str(option)
                    value = options[option]
                    if type(value) is unicode:
                        if value.isnumeric():
                            try:
                                value = int(value)
                            except ValueError:
                                value = float(value)
                        else:
                            value = str(value)
                    else:
                        # It is already what I want
                        pass
                    self._options.update({option: value})
        else:
            self._options = None

    def proj4(self):
        '''
        Description:
            TODO TODO TODO
        '''

        message = "You must implement this in the sub-class"
        raise NotImplementedError(message)

    def defaults(self):
        '''
        Description:
            TODO TODO TODO
        '''

        d = dict()
        d.update({'target_projection': self._target_projection})
        if self._defaults is not None:
            d.update(self._defaults)

        return d

    def to_dict(self):
        '''
        Description:
            TODO TODO TODO
        '''

        d = dict()

        if self._target_projection:
            d.update({'target_projection': self._target_projection})
            if self._options is not None:
                d.update(self._options)

        return d
# END - Projection


class GeographicProjection(Projection):
    '''
    Description:
        TODO TODO TODO
    '''

    def __init__(self, *args, **kwarg):
        '''
        Description:
            TODO TODO TODO
        '''

        self._target_projection = 'lonlat'
        self._defaults = None
        self._required_options = None

        super(GeographicProjection, self).__init__(*args, **kwarg)

    def proj4(self):
        '''
        Description:
          Builds a proj.4 string for geographic
          gdalsrsinfo 'EPSG:4326'

        Example:
          +proj=longlat +datum=WGS84 +no_defs
        '''

        return "'+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'"
# END - GeographicProjection


class UTMProjection(Projection):
    '''
    Description:
        TODO TODO TODO
    '''

    def __init__(self, *args, **kwarg):
        '''
        Description:
            TODO TODO TODO
        '''

        self._target_projection = 'utm'
        self._defaults = {
            'utm_north_south': None,
            'utm_zone': None
        }
        self._required_options = ['utm_north_south', 'utm_zone']

        super(UTMProjection, self).__init__(*args, **kwarg)

    def proj4(self):
        '''
        Description:
          Builds a proj.4 string for utm

        Examples:
          +proj=utm +zone=60 +ellps=WGS84 +datum=WGS84 +units=m +no_defs

          +proj=utm +zone=39 +south +ellps=WGS72
          +towgs84=0,0,1.9,0,0,0.814,-0.38 +units=m +no_defs
        '''

        # TODO - Found this example on the web for south (39), that
        # TODO - specifies the datum instead of "towgs"
        # TODO - gdalsrsinfo EPSG:32739
        # TODO - +proj=utm +zone=39 +south +datum=WGS84 +units=m +no_defs
        # TODO - It also seems that northern doesn't need the ellipsoid either
        # TODO - gdalsrsinfo EPSG:32660
        # TODO - +proj=utm +zone=60 +datum=WGS84 +units=m +no_defs

        proj4 = ("'+proj=utm +zone=%i +ellps=WGS84 +datum=WGS84"
                 " +units=m +no_defs'" % self._options['utm_zone'])
        if self._options['utm_north_south'].lower() == 'south':
            proj4 = ("'+proj=utm +zone=%i +south +ellps=WGS72"
                     " +towgs84=0,0,1.9,0,0,0.814,-0.38 +units=m +no_defs'"
                     % self._options['utm_zone'])

        return proj4
# END - UTMProjection


def get_projection_instance(options):
    '''
    Description:
        TODO TODO TODO
    '''

    if 'target_projection' in options.keys():
        target_projection = options['target_projection']
        if target_projection == 'lonlat':
            return GeographicProjection(options)
        elif target_projection == 'utm':
            return UTMProjection(options)
        # TODO TODO TODO - Write more projections
        else:
            message = ("[%s] projection not implemented"
                       % target_projection)
            raise NotImplementedError(message)
    else:
        return None
# END - get_projection_instance


class ImageExtents(object):
    '''
    Description:
        TODO TODO TODO
    '''

    _options = None
    _defaults = None
    _required_options = ['maxx', 'maxy', 'minx', 'miny']

    def __init__(self, *args, **kwarg):
        '''
        Description:
            TODO TODO TODO
        '''

        options = dict(*args, **kwarg)

        self._options = dict()
        for option in self._required_options:
            if option not in options.keys():
                message = ("[%s] is required for image_extents" % option)
                raise OptionViolation(message)
            else:
                option = str(option)
                try:
                    value = float(options[option])
                except ValueError:
                    message = ("[%s] is required to be numeric" % option)
                    raise OptionViolation(message)

                self._options.update({option: value})

    def gdal_warp_options(self):
        '''
        Description:
            TODO TODO TODO
        '''

        return ('-te %f %f %f %f' % (self._options['minx'],
                                     self._options['miny'],
                                     self._options['maxx'],
                                     self._options['maxy']))

    def defaults(self):
        '''
        Description:
            TODO TODO TODO
        '''

        d = dict()

        d.update({'image_extents': False})

        return d

    def to_dict(self):
        '''
        Description:
            TODO TODO TODO
        '''

        d = dict()

        d.update({'image_extents': True})
        d.update(self._options)

        return d
# END - ImageExtents


def get_image_extents_instance(options):
    '''
    Description:
        TODO TODO TODO
    '''

    if 'image_extents' in options.keys():
        if options['image_extents']:
            return ImageExtents(options)
        else:
            return None
    else:
        return None
# END - get_image_extents_instance


class Resize(object):
    '''
    Description:
        TODO TODO TODO
    '''

    _options = None
    _defaults = None
    _required_options = ['pixel_size', 'pixel_size_units']

    def __init__(self, options, default_pixel_options):
        '''
        Description:
            TODO TODO TODO
        '''

        self._defaults = default_pixel_options

        # Initially set them to the defaults
        self._options = dict(self._defaults)

        # Override them with the specified options
        for option in self._required_options:
            if option not in options.keys():
                message = ("[%s] is required for resize" % option)
                raise OptionViolation(message)
            else:
                option = str(option)
                value = options[option]
                if type(value) is unicode:
                    if value.isnumeric():
                        value = float(value)
                    else:
                        value = str(value)
                else:
                    # It is already what I want
                    pass

                self._options.update({option: value})

    def gdal_warp_options(self):
        '''
        Description:
            TODO TODO TODO
        '''

        return ('-tr %f %f' % (self._options['pixel_size'],
                               self._options['pixel_size']))

    def defaults(self):
        '''
        Description:
            TODO TODO TODO
        '''

        d = dict()

        d.update({'resize': False})

        for option in self._defaults:
            d.update({option: None})

        return d

    def to_dict(self):
        '''
        Description:
            TODO TODO TODO
        '''

        d = dict()

        d.update({'resize': True})
        d.update(self._options)

        return d
# END - Resize


# TODO TODO TODO - Add more Resize Sensors


def get_resize_instance(options, default_pixel_options):
    '''
    Description:
        TODO TODO TODO
    '''

    if 'resize' in options.keys():
        if options['resize']:
            return Resize(options, default_pixel_options)
        else:
            return None
    else:
        return None
# END - resize_instance


class CustomizationParameters(object):
    '''
    Description:
        TODO TODO TODO
    '''

    _reproject = None
    _defaults = {'reproject': False}

    _projection = None
    _image_extents = None
    _resize = None

    def __init__(self, options, sensor_code):
        '''
        Description:
            TODO TODO TODO
        '''

        logger = logging.getLogger()

        default_pixel_options = get_default_resize_parameters(sensor_code)

        self._reproject = options['reproject']

        if self._reproject:
            # Get a tprojection if one was requested
            self._projection = get_projection_instance(options)

            # Get image_extents if one was requested
            self._image_extents = get_image_extents_instance(options)

            # Get resize options
            self._resize = get_resize_instance(options, default_pixel_options)

        # Resize was not specified but is required (so default it)
        if (((self._projection is not None)
             or (self._image_extents is not None))
            and (self._resize is None)):

            pixel_options = json.dumps(default_pixel_options, indent=4,
                                        sort_keys=True)
            message = ("[resize] is required but not specified defaulting"
                       " to\n%s" % pixel_options)
            logger.warning(message)

            # Specify resize in the options
            options['resize'] = True
            options.update(default_pixel_options)

            # Get resize options
            self._resize = get_resize_instance(options, default_pixel_options)

    def gdal_warp_options(self):
        '''
        Description:
            TODO TODO TODO
        '''

        warp_cmd = ''

        if self._projection is not None:
            warp_cmd = ' '.join([warp_cmd, self._projection.proj4()])

        if self._image_extents is not None:
            warp_cmd = ' '.join([warp_cmd,
                                 self._image_extents.gdal_warp_options()])

        if self._resize is not None:
            warp_cmd = ' '.join([warp_cmd, self._resize.gdal_warp_options()])

        return warp_cmd

    def defaults(self):
        '''
        Description:
            TODO TODO TODO
        '''

        d = dict()

        d.update(self._defaults)

        if self._projection is not None:
            d.update(self._projection.defaults())

        if self._image_extents is not None:
            d.update(self._image_extents.defaults())

        if self._resize is not None:
            d.update(self._resize.defaults())

        return d

    def to_dict(self):
        '''
        Description:
            TODO TODO TODO
        '''

        d = dict()

        if self._reproject:
            d.update({'reproject': self._reproject})

            if self._projection is not None:
                d.update(self._projection.to_dict())

            if self._image_extents is not None:
                d.update(self._image_extents.to_dict())

            if self._resize is not None:
                d.update(self._resize.to_dict())

        return d
# END - CustomizationParameters


def get_customization_instance(options, sensor_code):
    '''
    Description:
        TODO TODO TODO
    '''

    if 'reproject' in options.keys():
        return CustomizationParameters(options, sensor_code)
    else:
        return None
# END - get_customization_instance


class OrderParameters(Parameters):
    '''
    Description:
        TODO TODO TODO
    '''

    valid_dev_options = {
        'debug': False,
        'keep_log': False
    }
    valid_source_options = {
        'source_host': None,
        'source_username': None,
        'source_pw': None,
        'source_directory': None
    }
    valid_destination_options = {
        'destination_host': None,
        'destination_username': None,
        'destination_pw': None,
        'destination_directory': None
    }
    valid_output_options = {
        'output_format': 'envi'
    }
    valid_common_include_options = {
        'include_customized_source_data': False,
        'include_source_data': False
    }

    def __init__(self, *args, **kwarg):
        '''
        Description:
            TODO TODO TODO
        '''

        super(OrderParameters, self).__init__(*args, **kwarg)

        self.valid_parameters = ['orderid', 'scene', 'xmlrpcurl', 'options']

        # -----------
        parameters = self.keys()

        # Make sure all of the provided parameters are allowed
        for parameter in parameters:
            self._is_valid_parameter(parameter)

        # Make sure all of the required parameters are present
        self._find_required_parameters(parameters)

        # -----------
        options = self['options'].keys()

        # TODO TODO TODO - Option validation needs an overhall, it should
        #                  probably be split into associated groups.

        # Make sure all of the provided options are allowed
        for option in options:
            self._is_valid_option(option)

        # Make sure all of the required options are present
        self._find_required_options(options)


class LandsatOrderParameters(OrderParameters):
    '''
    Description:
        TODO TODO TODO
    '''

    def __init__(self, *args, **kwarg):
        '''
        Description:
            TODO TODO TODO
        '''

        # Create empty options
        self.valid_options = dict()

        # Use update to add the options we need
        self.valid_options.update(self.valid_dev_options)
        self.valid_options.update(self.valid_source_options)
        self.valid_options.update(self.valid_destination_options)
        self.valid_options.update(self.valid_output_options)
        self.valid_options.update(self.valid_common_include_options)

        self.valid_options.update({
            'include_cfmask': False,
            'include_dswe': False,
            'include_solr_index': False,
            'include_source_data': False,
            'include_source_metadata': False,
            'include_sr': False,
            'include_sr_browse': False,
            'include_sr_evi': False,
            'include_sr_msavi': False,
            'include_sr_nbr': False,
            'include_sr_nbr2': False,
            'include_sr_ndmi': False,
            'include_sr_ndvi': False,
            'include_sr_savi': False,
            'include_sr_thermal': False,
            'include_sr_toa': False,
            'include_statistics': False,

            'image_extents': False,
            'maxx': None,
            'maxy': None,
            'minx': None,
            'miny': None,

            'reproject': False,

            'datum': "wgs84",
            'target_projection': None,
            'resample_method': 'near',

            'resize': False,
            'pixel_size': 30.0,
            'pixel_size_units': 'meters',

            'false_easting': None,
            'false_northing': None,
            'std_parallel_1': None,
            'std_parallel_2': None,
            'central_meridian': None,
            'latitude_true_scale': None,
            'longitude_pole': None,
            'origin_lat': None,
            'utm_north_south': None,
            'utm_zone': None
        })

        super(LandsatOrderParameters, self).__init__(*args, **kwarg)


class ModisOrderParameters(OrderParameters):
    '''
    Description:
        TODO TODO TODO
    '''

    def __init__(self, *args, **kwarg):
        '''
        Description:
            TODO TODO TODO
        '''

        # Create empty options
        self.valid_options = dict()

        # Use update to add the options we need
        self.valid_options.update(self.valid_dev_options)
        self.valid_options.update(self.valid_source_options)
        self.valid_options.update(self.valid_destination_options)
        self.valid_options.update(self.valid_output_options)
        self.valid_options.update(self.valid_common_include_options)

        self.valid_options.update({
            'image_extents': False,
            'maxx': None,
            'maxy': None,
            'minx': None,
            'miny': None,

            'reproject': False,

            'datum': "wgs84",
            'target_projection': None,
            'resample_method': 'near',

            'resize': False,
            'pixel_size': 30.0,
            'pixel_size_units': 'meters',

            'false_easting': None,
            'false_northing': None,
            'std_parallel_1': None,
            'std_parallel_2': None,
            'central_meridian': None,
            'latitude_true_scale': None,
            'longitude_pole': None,
            'origin_lat': None,
            'utm_north_south': None,
            'utm_zone': None
        })

        super(ModisOrderParameters, self).__init__(*args, **kwarg)


def instance(parms):
    '''
    Description:
        TODO TODO TODO
    '''

    if 'scene' not in parms.keys():
        message = "[scene] is missing from order parameters"
        raise ParameterViolation(message)

    _id = parms['scene'].lower().strip()

    # TODO TODO TODO - These regular expresions should probably be in
    #                  common/settings.py
    instances = {
        'tm': (r'^lt[4|5]\d{3}\d{3}\d{4}\d{3}\w{3}\d{2}$',
               LandsatOrderParameters),
        'etm': (r'^le7\d{3}\d{3}\d{4}\d{3}\w{3}\d{2}$',
                LandsatOrderParameters),

        'mod09a1': (r'^mod09a1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),
        'mod09ga': (r'^mod09ga\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),
        'mod09gq': (r'^mod09gq\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),
        'mod09q1': (r'^mod09q1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),

        'mod13a1': (r'^mod13a1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),
        'mod13a2': (r'^mod13a2\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),
        'mod13a3': (r'^mod13a3\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),
        'mod13q1': (r'^mod13q1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),

        'myd09a1': (r'^myd09a1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),
        'myd09ga': (r'^myd09ga\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),
        'myd09gq': (r'^myd09gq\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),
        'myd09q1': (r'^myd09q1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),

        'myd13a1': (r'^myd13a1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),
        'myd13a2': (r'^myd13a2\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),
        'myd13a3': (r'^myd13a3\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters),
        'myd13q1': (r'^myd13q1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisOrderParameters)
    }

    for key in instances.iterkeys():
        if re.match(instances[key][0], _id):
            return instances[key][1](parms)

    msg = "[%s] is not a supported sensor product" % parms['scene']
    raise NotImplementedError(msg)


if __name__ == '__main__':
    '''
    Description:
      This is test code for using the parameters module.
    '''

    logging.basicConfig(format=('%(asctime)s.%(msecs)03d %(process)d'
                                ' %(levelname)-8s'
                                ' %(filename)s:%(lineno)d:'
                                '%(funcName)s -- %(message)s'),
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO)

    logger = logging.getLogger(__name__)

    # Create a command line argument parser
    description = "Configures and executes a test order"
    parser = ArgumentParser(description=description)

    # Add parameters
    parser.add_argument('--order-file',
                        action='store', dest='order_file', required=True,
                        help="order file to process")

    # Parse the command line arguments
    args = parser.parse_args()

    # Avoid the creation of the *.pyc files
    os.environ['PYTHONDONTWRITEBYTECODE'] = '1'

    # Verify that the file is present
    if not os.path.isfile(args.order_file):
        logger.critical("Order file (%s) does not exist" % args.order_file)
        sys.exit(1)

    order_fd = open(args.order_file, 'r')

    # Read in the entire file
    order_string = order_fd.read()
    if not order_string:
        logger.critical("Empty file exiting")
        sys.exit(1)

    # This was the first attempt
    parms = instance(json.loads(order_string))
    if parms:
        print json.dumps(parms, indent=4, sort_keys=True)

    sensor_code = get_sensor_code(parms)
    print sensor_code
    # This is the new attempt
    warp_parms = get_customization_instance(parms['options'], sensor_code)
    if warp_parms:
        print warp_parms.gdal_warp_options()
        print json.dumps(warp_parms.defaults(), indent=4, sort_keys=True)
        print json.dumps(warp_parms.to_dict(), indent=4, sort_keys=True)

    sys.exit(0)
