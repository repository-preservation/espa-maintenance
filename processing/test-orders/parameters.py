
import os
import sys
import re
import logging
import json


class ParameterViolation(Exception):
    pass

class OptionViolation(Exception):
    pass


class OrderParameters(dict):

    valid_parameters = ['orderid', 'scene', 'xmlrpcurl', 'options']
    valid_options = None


    def __init__(self, *args, **kwarg):
        super(OrderParameters, self).__init__(*args, **kwarg)

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


    def _is_valid_parameter(self, parameter):

        if parameter not in self.valid_parameters:
            message = "[%s] is not a valid parameter" % parameter
            raise ParameterViolation(message)


    def _is_valid_option(self, option):

        if option not in self.valid_options.keys():
            message = "[%s] is not a valid option" % option
            raise OptionViolation(message)


    def _find_required_parameters(self, parameters):

        for parameter in self.valid_parameters:
            if parameter not in parameters:
                message = "[%s] is missing from order parameters" % parameter
                raise ParameterViolation(message)


    def _find_required_options(self, options):

        logger = logging.getLogger()

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


class LandsatOrderParameters(OrderParameters):

    def __init__(self, *args, **kwarg):

        self.valid_options = {
            'debug': False,
            'keep_log': False,

            'source_host': None,
            'source_directory': None,
            'destination_host': None,
            'destination_directory': None,

            'include_cfmask': False,
            'include_customized_source_data': False,
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

            'output_format': 'envi',

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
        }

        super(LandsatOrderParameters, self).__init__(*args, **kwarg)


class ModisOrderParameters(OrderParameters):

    def __init__(self, *args, **kwarg):

        self.valid_options = {
            'debug': False,
            'keep_log': False,

            'source_host': None,
            'source_directory': None,
            'destination_host': None,
            'destination_directory': None,

            'include_customized_source_data': False,
            'include_source_data': False,

            'output_format': 'envi',

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
        }

        super(ModisOrderParameters, self).__init__(*args, **kwarg)


def instance(parms):
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
    "scene": "LT42181092013069PFS00",
    "scene": "LT52181092013069PFS00",
    "scene": "LE72181092013069PFS00",
    "scene": "MOD09A1.A2002041.h09v04.005.2007125045728",
    '''

    logger = logging.basicConfig(format=('%(asctime)s.%(msecs)03d %(process)d'
                                         ' %(levelname)-8s'
                                         ' %(filename)s:%(lineno)d:%(funcName)s'
                                         ' -- %(message)s'),
                                 datefmt='%Y-%m-%d %H:%M:%S')

    order_string = \
"""
{
    "options": {
        "keep_log": true
    },
    "orderid": "water",
    "scene": "LE72181092013069PFS00",
    "xmlrpcurl": "skip_xmlrpc"
}
"""

    parms = instance(json.loads(order_string))
    print json.dumps(parms, indent=4, sort_keys=True)

    sys.exit()
