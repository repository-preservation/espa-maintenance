
import os
import sys
import re
import logging
import json
from argparse import ArgumentParser

# imports from espa/espa_common
try:
    import sensor
except:
    from espa_common import sensor


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
            msg = "You must specify the valid parameters in the sub-class"
            raise DeveloperViolation(msg)

        if parameter not in self.valid_parameters:
            msg = "[%s] is not a valid parameter" % parameter
            raise ParameterViolation(msg)

    def _is_valid_option(self, option):
        '''
        Description:
            TODO TODO TODO
        '''

        logger = logging.getLogger()

        if not self.valid_options:
            msg = "You must specify the valid options in the sub-class"
            raise DeveloperViolation(msg)

        if option not in self.valid_options.keys():
            logger.warning("[%s] is not a valid option" % option)

    def _find_required_parameters(self, parameters):
        '''
        Description:
            TODO TODO TODO
        '''

        if not self.valid_parameters:
            msg = "You must specify the valid parameters in the sub-class"
            raise DeveloperViolation(msg)

        for parameter in self.valid_parameters:
            if parameter not in parameters:
                msg = "[%s] is missing from order parameters" % parameter
                raise ParameterViolation(msg)

    def _find_required_options(self, options):
        '''
        Description:
            TODO TODO TODO
        '''

        logger = logging.getLogger()

        if not self.valid_options:
            msg = "You must specify the valid options in the sub-class"
            raise DeveloperViolation(msg)

        # TODO TODO TODO - Verify assumption
        # I think all of the options can be defaulted so right now this is
        # very simple
        for option in self.valid_options:
            if option not in options:
                msg = ("[%s] is missing from order options and will be"
                       " defaulted to [%s]"
                       % (option, str(self.valid_options[option])))
                logger.warning(msg)
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
            msg = "You must specify the options in the sub-class"
            raise DeveloperViolation(msg)

        if type(self.options) != dict:
            msg = "the specified options must be a dict"
            raise DeveloperViolation(msg)

        if option not in self.options.keys():
            msg = "[%s] is not a valid option" % option
            raise OptionViolation(msg)

    # TODO TODO TODO - IMPLEMENT ME


class Projection(object):
    '''
    Description:
        TODO TODO TODO
    '''

    _target_projection = None
    _options = None
    _required_defaults = None

    def __init__(self, *args, **kwarg):
        '''
        Description:
            TODO TODO TODO
        '''

        if self._required_defaults:
            options = dict(*args, **kwarg)
            self._options = dict()
            for option in self._required_defaults.keys():
                if option not in options.keys():
                    msg = ("[%s] is required for target_projection" % option)
                    raise OptionViolation(msg)
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

        msg = "You must implement this in the sub-class"
        raise NotImplementedError(msg)

    def defaults(self):
        '''
        Description:
            TODO TODO TODO
        '''

        d = dict()
        d.update({'target_projection': self._target_projection})
        if self._required_defaults is not None:
            d.update(self._required_defaults)

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
        self._required_defaults = None

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
        self._required_defaults = {
            'utm_north_south': None,
            'utm_zone': None
        }

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
            msg = ("[%s] projection not implemented" % target_projection)
            raise NotImplementedError(msg)
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
                msg = ("[%s] is required for image_extents" % option)
                raise OptionViolation(msg)
            else:
                option = str(option)
                try:
                    value = float(options[option])
                except ValueError:
                    msg = ("[%s] is required to be numeric" % option)
                    raise OptionViolation(msg)

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

        logger = logging.getLogger(__name__)

        self._defaults = default_pixel_options
        logger.debug(self._defaults)

        # Initially set them to the defaults
        self._options = self._defaults

        # Override them with the specified options
        for option in self._required_options:
            if option not in options.keys():
                msg = ("[%s] is required for resize" % option)
                raise OptionViolation(msg)
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

    def __init__(self, options, sensor_inst):
        '''
        Description:
            TODO TODO TODO
        '''

        logger = logging.getLogger()

        # Get the defaults assuming not geographic
        pixel_sizes = sensor_inst.default_pixel_size
        default_pixel_options = {
            'pixel_size': pixel_sizes['meters'],
            'pixel_size_units': 'meters'
        }

        self._reproject = options['reproject']

        if self._reproject:
            # Get a projection if one was requested
            self._projection = get_projection_instance(options)

            # Get image_extents if one was requested
            self._image_extents = get_image_extents_instance(options)

            # If we have the geographic projection fixs the defaults
            if (self._projection is not None
                    and type(self._projection) is GeographicProjection):
                default_pixel_options = {
                    'pixel_size': pixel_sizes['dd'],
                    'pixel_size_units': 'dd'
                }

            # Get resize options
            self._resize = get_resize_instance(options, default_pixel_options)

        # Resize was not specified but is required (so default it)
        if (((self._projection is not None)
             or (self._image_extents is not None))
                and (self._resize is None)):

            pixel_options = json.dumps(default_pixel_options, indent=4,
                                       sort_keys=True)
            msg = ("[resize] is required but not specified defaulting"
                   " to\n%s" % pixel_options)
            logger.warning(msg)

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


def get_customization_instance(options, sensor_inst):
    '''
    Description:
        TODO TODO TODO
    '''

    if 'reproject' in options.keys():
        return CustomizationParameters(options, sensor_inst)
    else:
        return None
# END - get_customization_instance


class RequestBase(object):
    '''
    Description:
        Provides the super class implementation for product request parameters
        and options
    '''

    _valid_keys = None
    _items = None

    def __init__(self):
        '''
        Description:
          Initializes the internal attributes
        '''
        self._valid_keys = self.get_defaults()
        self._items = dict()

    def get_defaults(self):
        '''
        Description:
          Returns the defaults for this instance of the object
        '''
        msg = "Must be implemented in sub-class"
        raise DeveloperViolation(msg)

    def __str__(self):
        '''
        Description:
          To provide string conversion
        '''
        return self._items.__str__()

    def __repr__(self):
        '''
        Description:
          To provide representation conversion
        '''
        return self._items.__repr__()

    def __getitem__(self, key, **args):
        '''
        Description:
          Implemented to allow acting like a dictionary but with key
          validation
        '''
        if key not in self._valid_keys:
            msg = "[%s] is not a valid request item" % key
            raise ParameterViolation(msg)

        return self._items.__getitem__(key, **args)

    def __setitem__(self, key, value):
        '''
        Description:
          Implemented to allow acting like a dictionary but with key
          validation
        '''
        if key not in self._valid_keys:
            msg = "[%s] is not a valid request item" % key
            raise ParameterViolation(msg)

        self._items.__setitem__(key, value)

    def keys(self):
        '''
        Description:
          Implemented to allow acting like a dictionary
        '''
        return self._items

    def values(self):
        '''
        Description:
          Implemented to allow acting like a dictionary
        '''
        return [self._items[key] for key in self._items]

    def itervalues(self):
        '''
        Description:
          Implemented to allow acting like a dictionary
        '''
        return [self._items[key] for key in self._items]

    def to_json(self):
        '''
        Description:
          Convert the object to a json representation
        '''
        return json.dumps(self.json_serialize(self._items),
                          indent=4, sort_keys=True)

    def json_serialize(self, obj):
        '''
        Description:
          Serialize for json dumps to work.

        Note:
          It is intended that this method is only to be used by to_json()
        '''

        if isinstance(obj, (bool, int, long, float)):
            return obj
        elif isinstance(obj, dict):
            obj = obj.copy()
            for key in obj:
                obj[key] = self.json_serialize(obj[key])
            return obj
        elif isinstance(obj, list):
            return [self.json_serialize(item) for item in obj]
        elif isinstance(obj, tuple):
            return tuple(self.json_serialize([item for item in obj]))
        elif hasattr(obj, '__dict__'):
            return self.json_serialize(obj.__dict__)
        elif obj is None:
            return obj
        else:
            return str(obj)
# END - RequestBase


class RequestOptionsBase(RequestBase):

    def get_defaults(self):
        '''
        Description:
            Provides the specific options required for all options
        '''
        return ['destination_directory',
                'destination_host',
                'destination_pw',
                'destination_username',
                'include_customized_source_data',
                'include_source_data',
                'source_directory',
                'source_host',
                'source_pw',
                'source_username',
                'debug',
                'keep_log']

    def __init__(self):
        super(RequestOptionsBase, self).__init__()
# END - RequestOptionsBase


class RequestCustomizationOptions(RequestOptionsBase):
    '''
    Description:
        Provides custom implementations from Landsat requests
    '''

    def get_defaults(self):
        '''
        Description:
            Provides the specific options allowed for product customization
        '''

        option_list = super(RequestCustomizationOptions,
                            self).get_defaults()

        additional_options = ['central_meridian',
                              'datum',
                              'false_easting',
                              'false_northing',
                              'image_extents',
                              'latitude_true_scale',
                              'longitude_pole',
                              'maxx',
                              'maxy',
                              'minx',
                              'miny',
                              'origin_lat',
                              'output_format',
                              'pixel_size',
                              'pixel_size_units',
                              'reproject',
                              'resample_method',
                              'resize',
                              'std_parallel_1',
                              'std_parallel_2',
                              'target_projection',
                              'utm_north_south',
                              'utm_zone']

        option_list.extend(additional_options)
        print option_list

        return option_list

    def __init__(self):
        super(RequestCustomizationOptions, self).__init__()
# END - RequestCustomizationOptionsBase


class LandsatRequestOptions(RequestCustomizationOptions):
    '''
    Description:
        Provides custom implementations from Landsat requests
    '''

    def get_defaults(self):
        '''
        Description:
            Provides the specific options allowed for this sub-class
        '''

        option_list = super(LandsatRequestOptions, self).get_defaults()

        additional_options = ['include_cfmask',
                              'include_dswe',
                              'include_solr_index',
                              'include_source_metadata',
                              'include_sr',
                              'include_sr_browse',
                              'include_sr_evi',
                              'include_sr_msavi',
                              'include_sr_nbr',
                              'include_sr_nbr2',
                              'include_sr_ndmi',
                              'include_sr_ndvi',
                              'include_sr_savi',
                              'include_sr_thermal',
                              'include_sr_toa',
                              'include_statistics']

        option_list.extend(additional_options)

        return option_list

    def __init__(self, parameters):
        super(LandsatRequestOptions, self).__init__()

        # Add the parameters
        for parameter in parameters:
            self[parameter] = parameters[parameter]
# END - LandsatRequestOptions


class ModisRequestOptions(RequestCustomizationOptions):
    '''
    Description:
        Provides custom implementations from Landsat requests
    '''

    def get_defaults(self):
        '''
        Description:
            Provides the specific options allowed for this sub-class
        '''

        option_list = super(LandsatRequestOptions, self).get_defaults()

        # TODO TODO TODO - Does it need any?????
        additional_options = []  # Doesn't have any yet

        option_list.extend(additional_options)

        return option_list

    def __init__(self, parameters):
        super(LandsatRequestOptions, self).__init__()

        # Add the parameters
        for parameter in parameters:
            self[parameter] = parameters[parameter]
# END - ModisRequestOptions


class RequestParameters(RequestBase):
    '''
    Description:
      Provide the validation and access to parameters and specifics for
      processing requests.

    Note:
      This is intended to be all the user need to instantiate.
      It will provide or through its super classes provide the methods
      required to get the job done.
    '''

    def get_defaults(self):
        '''
        Description:
            Provides the specific parameters allowed for this sub-class
        '''
        return ['orderid', 'scene', 'xmlrpcurl', 'options']

    def __init__(self, parameters):
        '''
        Description:
          TODO TODO TODO
        '''

        super(RequestParameters, self).__init__()

        logger = logging.getLogger(__name__)

        # Add the parameters
        for parameter in parameters:
            if parameter != 'options':
                self[parameter] = parameters[parameter]
            else:
                # TODO TODO TODO - Need to figure out if it should be the
                #                  Landsat or Modis object
                self['options'] = LandsatRequestOptions(parameters['options'])
                pass

    def json_serialize(self, obj):
        '''
        Description:
          Override the super implementation so that we can convert the
          request options to a dictionary for processing
        '''

        if isinstance(obj, (LandsatRequestOptions, ModisRequestOptions)):
            return super(RequestParameters, self).json_serialize(dict(obj))
        else:
            return super(RequestParameters, self).json_serialize(obj)
# END - RequestParameters


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
        msg = "[scene] is missing from request parameters"
        raise ParameterViolation(msg)

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
    parser.add_argument('--request-file',
                        action='store', dest='request_file', required=True,
                        help="request file to process")

    parser.add_argument('--product-id',
                        action='store', dest='product_id', required=True,
                        help="product_id to process")

    # Parse the command line arguments
    args = parser.parse_args()

    # Avoid the creation of the *.pyc files
    os.environ['PYTHONDONTWRITEBYTECODE'] = '1'

    # Verify that the file is present
    if not os.path.isfile(args.request_file):
        logger.critical("Order file (%s) does not exist" % args.request_file)
        sys.exit(1)

    request_string = None
    with open(args.request_file, 'r') as request_fd:
        # Read in the entire file
        request_string = request_fd.read()
        if not request_string:
            logger.critical("Empty file exiting")
            sys.exit(1)

    request_string = request_string.replace('SCENE_ID', args.product_id)

#    # This was the first attempt
#    parms = instance(json.loads(request_string))
#    if parms:
#        print json.dumps(parms, indent=4, sort_keys=True)
#
#    if 'scene' not in parms.keys():
#        msg = "[scene] is missing from request parameters"
#        raise ParameterViolation(msg)
#
#    sensor_inst = sensor.instance(parms['scene'])
#    print sensor_inst.sensor_code
#    # This is the new attempt
#    warp_parms = get_customization_instance(parms['options'], sensor_inst)
#    if warp_parms:
#        print warp_parms.gdal_warp_options()
#        print json.dumps(warp_parms.defaults(), indent=4, sort_keys=True)
#        print json.dumps(warp_parms.to_dict(), indent=4, sort_keys=True)

    parms = json.loads(request_string)
    request_parms = RequestParameters(parms)
    print request_parms
    print request_parms.to_json()

    sys.exit(0)
