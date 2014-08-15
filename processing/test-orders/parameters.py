
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

        parameters = self.keys()

        for parameter in parameters:
            self._is_valid_parameter(parameter)

        self._find_missing_parameters(parameters)

        options = self['options'].keys()

        for option in options:
            self._is_valid_option(option)

        self._find_missing_options(option)

    def _is_valid_parameter(self, parameter):

        if parameter not in self.valid_parameters:
            message = "[%s] is not a valid parameter" % parameter
            raise ParameterViolation(message)

    def _is_valid_option(self, option):

        if option not in self.valid_options.keys():
            message = "[%s] is not a valid option" % option
            raise OptionViolation(message)

    def _find_missing_parameters(self, parameters):

        for parameter in self.valid_parameters:
            if parameter not in parameters:
                message = "[%s] is missing from order parameters" % parameter
                raise ParameterViolation(message)

    def _find_missing_options(self, options):

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


class LandsatOrderParameters(OrderParameters):

    def __init__(self, *args, **kwarg):

        self.valid_options = {
            'destination_host': None,
            'destination_directory': None,
            'include_customized_source_data': False,
            'include_source_data': False,
            'include_source_metadata': False,
            'keep_log': False,
            'source_host': None,
            'source_directory': None
        }

        super(LandsatOrderParameters, self).__init__(*args, **kwarg)


class ModisOrderParameters(OrderParameters):

    def __init__(self, *args, **kwarg):

        self.valid_options = {
            'keep_log': False
        }

        super(ModisOrderParameters, self).__init__(*args, **kwarg)


def instance(parms):
    if 'scene' not in parms.keys():
        message = "[scene] is missing from order parameters"
        raise ParameterViolation(message)

    _id = parms['scene'].lower().strip()

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

    logger = logging.basicConfig()

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
