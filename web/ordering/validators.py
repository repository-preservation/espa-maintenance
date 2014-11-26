import lta
import lpdaac
from models import Order

from espa_common import sensor
from espa_common import utilities
from espa_common.validation import Validator


class ModisProductListValidator(Validator):
    '''Validates that a scene list has been provided and it contains at
    least one scene to process'''

    def get_verified_input_product_set(self, input_products):
        valid_products = list()

        for p in input_products:

            if isinstance(p, str):
                p = sensor.instance(p)

            if isinstance(p, sensor.Modis):
                if lpdaac.input_exists(p):
                    valid_products.append(p.product_id)

        return set(valid_products)

    def errors(self):
        '''Looks through the input_product_list if present and determines
        if there are valid scenes to process'''

        modis_products = list()

        if not 'input_products' in self.parameters:
            return super(ModisProductListValidator, self).errors()
        else:

            for product in self.parameters['input_products']:
                if isinstance(product, sensor.Modis):
                    modis_products.append(product)

            if len(modis_products) > 0:

                valid_ids = self.get_verified_input_product_set(modis_products)

                if len(valid_ids) == 0:
                    msg_parts = []
                    msg_parts.append("The following Modis tiles were not ")
                    msg_parts.append("found in the datapool:\n")

                    for mp in modis_products:
                        msg_parts.append("\t%s\n" % mp.product_id)

                    msg = ''.join(msg_parts)

                    self.add_error('input_products', [msg, ])
                else:
                    prod_ids = [p.product_id for p in modis_products]

                    difference = set(prod_ids) - set(valid_ids)

                    if len(difference) > 0:
                        for diff in difference:
                            msg = ("%s not found in Modis datapool" % diff)
                            self.add_error('input_products', [msg, ])

        return super(ModisProductListValidator, self).errors()


class LandsatProductListValidator(Validator):
    '''Validates that a scene list has been provided and it contains at
    least one scene to process'''

    def get_verified_input_product_set(self, products):
        '''Returns a subset ( set() )  of products that are orderable
        from Landsat

        Keyword args:
        products A list() of sensor.Landsat objects

        Returns:
        A set() of valid sensor.Landsat objects

        Note:
        This method duplicates type checks for sensor.Landsat from the
        errors() method so it's usable by a caller outside the validators
        module.
        '''

        valid_products = list()

        if len(products) > 0:

            request = list()

            for p in products:

                if isinstance(p, str):
                    p = sensor.instance(p)

                if isinstance(p, sensor.Landsat):
                    request.append(p.product_id)

            verified = lta.verify_scenes(request)

            for product_name, valid in verified.iteritems():
                if valid:
                    valid_products.append(product_name)

        return set(valid_products)

    def errors(self):
        '''Looks through the input_product_list if present and determines
        if there are valid scenes to process'''

        landsat_products = list()

        if not 'input_products' in self.parameters:
            return super(LandsatProductListValidator, self).errors()
        else:

            for product in self.parameters['input_products']:
                if isinstance(product, sensor.Landsat):
                    landsat_products.append(product)

            if len(landsat_products) > 0:

                valid = self.get_verified_input_product_set(landsat_products)

                if len(valid) == 0:
                    msg_parts = []
                    msg_parts.append("The following scenes were not found")
                    msg_parts.append(" in the Landsat inventory:\n")

                    for lp in landsat_products:

                        msg_parts.append("\t%s\n" % lp.product_id)

                    msg = ''.join(msg_parts)

                    self.add_error('input_products', [msg, ])
                else:

                    product_list = [s.product_id for s in landsat_products]
                    difference = set(product_list) - set(valid)

                    if len(difference) > 0:
                        for diff in difference:
                            msg = ("%s not found in Landsat inventory" % diff)
                            self.add_error('input_products', [msg, ])

        return super(LandsatProductListValidator, self).errors()


class ProductIsSelectedValidator(Validator):
    '''Validates that at least one product has been selected'''

    def errors(self):

        product_is_selected = None

        for key in Order.get_default_product_options().iterkeys():
            if key in self.parameters:
                product_is_selected = True

        if not product_is_selected:
            self.add_error('product_selected',
                           ['Please select at least one output product.', ])

        return super(ProductIsSelectedValidator, self).errors()


class OutputFormatValidator(Validator):
    '''Validates the requested output format'''

    def errors(self):
        valid_formats = ['gtiff', 'envi', 'hdf-eos2']

        if not ('output_format' in self.parameters
                and self.parameters['output_format']):

                self.add_error('output_format',
                               ['Please select an output format', ])
        elif self.parameters['output_format'] not in valid_formats:
            self.add_error('output_format',
                           ['Output format must be one of:%s' % valid_formats])


class FalseEastingValidator(Validator):
    '''Validates the false_easting parameter'''

    def errors(self):

        if (not 'false_easting' in self.parameters
                or not utilities.is_number(self.parameters['false_easting'])):

                msg = "Please provide a valid false easting value"
                self.add_error('false_easting', [msg, ])

        return super(FalseEastingValidator, self).errors()


class FalseNorthingValidator(Validator):
    '''Validates the false_northing parameter'''

    def errors(self):

        if (not 'false_northing' in self.parameters
                or not utilities.is_number(self.parameters['false_northing'])):

                msg = "Please provide a valid false northing value"
                self.add_error('false_northing', [msg, ])

        return super(FalseNorthingValidator, self).errors()


class CentralMeridianValidator(Validator):
    '''Validates the central_meridian parameter'''

    def errors(self):

        msg_parts = []
        msg_parts.append("Please provide a central meridian value ")
        msg_parts.append("between -180.0 to 180.0")
        msg = ''.join(msg_parts)

        cm = None

        if ('central_meridian' in self.parameters
                and utilities.is_number(self.parameters['central_meridian'])):

                cm = float(self.parameters['central_meridian'])
        else:
            self.add_error('central_meridian', [msg, ])

        if cm and (cm < -180.0 or cm > 180.0):
            self.add_error('central_meridian', [msg, ])

        return super(CentralMeridianValidator, self).errors()


class LatitudeTrueScaleValidator(Validator):
    '''Validates the latitude_true_scale parameter'''
    def errors(self):
        msg_parts = []
        msg_parts.append("Please provide a latitude true scale value ")
        msg_parts.append("between -60.0 to -90.0 or 60.0 to 90.0")
        msg = ''.join(msg_parts)

        ts = None

        if ('latitude_true_scale' in self.parameters and
                utilities.is_number(self.parameters['latitude_true_scale'])):

            ts = float(self.parameters['latitude_true_scale'])

        else:
            self.add_error('latitude_true_scale', [msg, ])

        # make sure ts is either in the range of 60 to 90 or -90 to -60
        has_err = False
        if ts is not None:

            # check upper and lower bounds first
            if ts > 90.0 or ts < -90.0:
                has_err = True
            elif ts > -60.0 and ts < 60.0:
                has_err = True

            if has_err is True:
                self.add_error('latitude_true_scale', [msg, ])

        return super(LatitudeTrueScaleValidator, self).errors()


class LongitudinalPoleValidator(Validator):
    '''Validates the longitudinal_pole parameter'''

    def errors(self):
        msg_parts = []
        msg_parts.append("Please provide a longitudinal pole value ")
        msg_parts.append("between -180.0 to 180.0")
        msg = ''.join(msg_parts)

        lp = None

        if ('longitude_pole' in self.parameters and
                utilities.is_number(self.parameters['longitude_pole'])):
                lp = float(self.parameters['longitude_pole'])
        else:
            self.add_error('longitude_pole', [msg, ])

        if lp and (lp > 180.0 or lp < -180.0):
            self.add_error('longitude_pole', [msg, ])

        return super(LongitudinalPoleValidator, self).errors()


class StandardParallel1Validator(Validator):
    '''Validates the std_parallel_1 parameter'''

    def errors(self):
        msg_parts = []
        msg_parts.append("Please provide a 1st standard parallel value ")
        msg_parts.append("between -90.0 to 90.0")
        msg = ''.join(msg_parts)

        sp = None
        if ('std_parallel_1' in self.parameters and
                utilities.is_number(self.parameters['std_parallel_1'])):
                sp = float(self.parameters['std_parallel_1'])
        else:
            self.add_error('std_parallel_1', [msg, ])

        if sp and (sp < -90.0 or sp > 90.0):
            self.add_error('std_parallel_1', [msg, ])

        return super(StandardParallel1Validator, self).errors()


class StandardParallel2Validator(Validator):
    '''Validates the std_parallel_2 parameter'''

    def errors(self):
        msg_parts = []
        msg_parts.append("Please provide a 2nd standard parallel value ")
        msg_parts.append("between -90.0 to 90.0")
        msg = ''.join(msg_parts)

        sp = None
        if ('std_parallel_2' in self.parameters and
                utilities.is_number(self.parameters['std_parallel_2'])):
                sp = float(self.parameters['std_parallel_2'])
        else:
            self.add_error('std_parallel_2', [msg, ])

        if sp and (sp < -90.0 or sp > 90.0):
            self.add_error('std_parallel_2', [msg, ])

        return super(StandardParallel2Validator, self).errors()


class OriginLatitudeValidator(Validator):
    '''Validates origin_lat'''

    def errors(self):
        msg_parts = []
        msg_parts.append("Please provide a latitude of origin ")
        msg_parts.append("between -90.0 to 90.0")
        msg = ''.join(msg_parts)

        lo = None
        if ('origin_lat' in self.parameters and
                utilities.is_number(self.parameters['origin_lat'])):
                lo = float(self.parameters['origin_lat'])
        else:
            self.add_error('origin_lat', [msg, ])

        if lo and (lo < -90.0 or lo > 90.0):
            self.add_error('origin_lat', [msg, ])

        return super(OriginLatitudeValidator, self).errors()


class DatumValidator(Validator):
    '''Validates datum for albers projection'''
    valid_datum = ['nad27', 'nad83', 'wgs84']

    def errors(self):

        if (not 'datum' in self.parameters or not
                self.parameters['datum'] in self.valid_datum):

            msg = "Please select a datum from one of:%s" % self.valid_datum
            self.add_error('datum', [msg, ])

        return super(DatumValidator, self).errors()


class UTMZoneValidator(Validator):
    '''Validates utm_zone for utm projection'''

    def errors(self):

        if (not 'utm_zone' in self.parameters
                or not str(self.parameters['utm_zone']).isdigit()
                or not int(self.parameters['utm_zone']) in range(1, 61)):
                msg = "Please provide a utm zone between 1 and 60"
                self.add_error('utm_zone', [msg, ])

        return super(UTMZoneValidator, self).errors()


class UTMNorthSouthValidator(Validator):
    '''Validates utm_north_south for utm projection'''
    def errors(self):

        if (not 'utm_north_south' in self.parameters or not
                self.parameters['utm_north_south'] in ('north', 'south')):

                msg = "Please select north or south for the UTM zone"
                self.add_error('utm_north_south', [msg, ])

        return super(UTMNorthSouthValidator, self).errors()


class ProjectionValidator(Validator):
    '''Validates parameters for reprojection'''

    valid_projections = ['aea', 'ps', 'sinu', 'lonlat', 'utm']

    def __init__(self, parameters, child_validators=None, name=None):
        '''Conditionally build and attach child validators'''
        # delegate the call to superclass since we are overriding the
        # __init__ method
        super(ProjectionValidator, self).__init__(parameters,
                                                  child_validators,
                                                  name)

        # check for projection value and add appropriate child validators
        proj = None

        if not 'target_projection' in self.parameters:
            self.add_error("projection", ['projection must be specified'])
        else:
            proj = self.parameters['target_projection']

        if proj and proj not in self.valid_projections:

            self.add_error("projection",
                           ['projection must be one of %s'
                               % self.valid_projections])
        else:

            if proj == 'aea':
                self.add_child(AlbersValidator(parameters))
            elif proj == 'ps':
                self.add_child(PolarStereographicValidator(parameters))
            elif proj == 'sinu':
                self.add_child(SinusoidalValidator(parameters))
            elif proj == 'lonlat':
                self.add_child(GeographicValidator(parameters))
            elif proj == 'utm':
                self.add_child(UTMValidator(parameters))

    def errors(self):
        '''No actual validation happening in this validator'''
        return super(ProjectionValidator, self).errors()


class UTMValidator(Validator):
    '''Validates parameters for utm projection'''

    def __init__(self, parameters, child_validators=None, name=None):
        super(UTMValidator, self).__init__(parameters,
                                           child_validators,
                                           name)

        self.add_child(UTMZoneValidator(parameters))
        self.add_child(UTMNorthSouthValidator(parameters))

    def errors(self):
        '''This validator does nothing'''
        return super(UTMValidator, self).errors()


class AlbersValidator(Validator):
    '''Validates parameters for albers projection'''

    def __init__(self, parameters, child_validators=None, name=None):
        super(AlbersValidator, self).__init__(parameters,
                                              child_validators,
                                              name)

        self.add_child(CentralMeridianValidator(parameters))
        self.add_child(FalseEastingValidator(parameters))
        self.add_child(FalseNorthingValidator(parameters))
        self.add_child(StandardParallel1Validator(parameters))
        self.add_child(StandardParallel2Validator(parameters))
        self.add_child(OriginLatitudeValidator(parameters))
        self.add_child(DatumValidator(parameters))

    def errors(self):
        '''Delegates calls to child validators'''
        return super(AlbersValidator, self).errors()


class SinusoidalValidator(Validator):
    '''Validates parameters for sinusoidal projection'''

    def __init__(self, parameters, child_validators=None, name=None):
        super(SinusoidalValidator, self).__init__(parameters,
                                                  child_validators,
                                                  name)

        self.add_child(CentralMeridianValidator(parameters))
        self.add_child(FalseEastingValidator(parameters))
        self.add_child(FalseNorthingValidator(parameters))

    def errors(self):
        '''Delegates calls to child validators'''
        return super(SinusoidalValidator, self).errors()


class GeographicValidator(Validator):
    '''Validates parameters for geographic projection'''

    def errors(self):
        '''This validator does nothing'''
        return super(GeographicValidator, self).errors()


class PolarStereographicValidator(Validator):
    '''Validates parameters for polar stereographic projection'''

    def __init__(self, parameters, child_validators=None, name=None):
        super(PolarStereographicValidator, self).__init__(parameters,
                                                          child_validators,
                                                          name)

        self.add_child(LongitudinalPoleValidator(parameters))
        self.add_child(LatitudeTrueScaleValidator(parameters))
        self.add_child(FalseEastingValidator(parameters))
        self.add_child(FalseNorthingValidator(parameters))

    def errors(self):
        '''Delegates calls to child validators'''
        return super(PolarStereographicValidator, self).errors()


class MeterPixelSizeValidator(Validator):
    '''Validates pixel sizes specified in meters'''
    def errors(self):

        msg = "Please enter a pixel size between 30 and 1000 meters"

        ps = None

        if ('pixel_size' in self.parameters
                and utilities.is_number(self.parameters['pixel_size'])):

            ps = float(self.parameters['pixel_size'])
        else:
            self.add_error('pixel_size', [msg, ])

        if ps and not (ps >= 30.0 or ps <= 1000.0):
            self.add_error('pixel_size', [msg, ])

        return super(MeterPixelSizeValidator, self).errors()


class DecimalDegreePixelSizeValidator(Validator):
    '''Validates pixel sizes specified in decimal degrees'''

    def errors(self):

        msg = ''.join(["Please enter a pixel size between",
                       " 0.0002695 to 0.0089831 decimal degrees"])

        msg1 = "Valid pixel size is 0.0002695 to 0.0089831 decimal degrees"

        ps = None

        if ('pixel_size' in self.parameters
                and utilities.is_number(self.parameters['pixel_size'])):

            ps = float(self.parameters['pixel_size'])
        else:
            self.add_error('pixel_size', [msg, ])

        if ps and (ps > 0.0089831 or ps < 0.0002695):
            self.add_error('pixel_size', [msg1, ])

        return super(DecimalDegreePixelSizeValidator, self).errors()


class PixelSizeValidator(Validator):
    '''Validates pixel sizes'''

    def __init__(self, parameters, child_validators=None, name=None):
        super(PixelSizeValidator, self).__init__(parameters,
                                                 child_validators,
                                                 name)

        if (not 'pixel_size_units' in parameters
                or not parameters['pixel_size_units']):

            msg = "Target pixel size units not recognized"
            self.add_error('pixel_size_units', [msg, ])
        else:
            units = parameters['pixel_size_units'].strip()

            if not units in ['dd', 'meters']:
                msg = "Unknown pixel size units provided:%s" % units
                self.add_error('pixel_size_units', [msg, ])
            elif units == 'dd':
                self.add_child(DecimalDegreePixelSizeValidator(parameters))
            else:
                self.add_child(MeterPixelSizeValidator(parameters))

    def errors(self):
        '''Delegates calls to child validators'''
        return super(PixelSizeValidator, self).errors()


class ImageExtentsValidator(Validator):
    '''Validates image extents'''

    def errors(self):

        P = self.parameters

        minx = None
        miny = None
        maxx = None
        maxy = None

        # make sure we got upper left x,y and lower right x,y vals
        if not 'minx' in P or not utilities.is_number(P['minx']):
            msg = "Please provide a valid upper left x value"
            self.add_error('minx', [msg, ])
        else:
            minx = float(P['minx'])

        if not 'maxx' in P or not utilities.is_number(P['maxx']):
            msg = "Please provide a valid lower right x value"
            self.add_error('maxx', [msg, ])
        else:
            maxx = float(P['maxx'])

        if not 'miny' in P or not utilities.is_number(P['miny']):
            msg = "Please provide a valid lower right y value"
            self.add_error('miny', [msg, ])
        else:
            miny = float(P['miny'])

        if not 'maxy' in P or not utilities.is_number(P['maxy']):
            msg = "Please provide a valid upper left y value"
            self.add_error('maxy', [msg, ])
        else:
            maxy = float(P['maxy'])

        if minx and miny and maxx and maxy:

        # make sure values make some sort of sense
        # once we go to decimal degree bounding boxes only (no meter values)
        # then we can validate the values in the bounding box
            if minx >= maxx:
                m = "Upper left x value must be less than lower right x value"
                self.add_error('minx', [m, ])
                self.add_error('maxx', [m, ])

            if miny >= maxy:
                m = "Lower right y value must be less than upper left y value"
                self.add_error('miny', [m, ])
                self.add_error('maxy', [m, ])

        return super(ImageExtentsValidator, self).errors()


class NewOrderFilesValidator(Validator):
    '''Validator to check request.FILES for new order form submission'''

    def __init__(self, parameters, child_validators=None, name=None):
        super(NewOrderFilesValidator, self).__init__(parameters,
                                                     child_validators,
                                                     name)

        for product in self.parameters['input_products']:
            if not isinstance(product, sensor.SensorProduct):
                msg = list()
                msg.append("parameters['input_products'] must be of type ")
                msg.append("list(espa_common.SensorProduct())\n")
                msg.append("Incorrect type value was:%s" % product)
                raise TypeError(''.join(msg))

        self.add_child(LandsatProductListValidator(parameters))
        self.add_child(ModisProductListValidator(parameters))

    def errors(self):
        '''Validates a file was provided on upload and delegates calls to
        its children, *ProductListValidator'''

        msg = ''.join(['Please provide an input product list with at least',
                       ' one product for processing'])

        if (not 'input_products' in self.parameters
                or len(self.parameters['input_products']) == 0):

            self.add_error('input_products', [msg, ])

        return super(NewOrderFilesValidator, self).errors()


class NewOrderPostValidator(Validator):
    '''Validator to check request.POST values for new order form submission'''

    def __init__(self, parameters, child_validators=None, name=None):
        '''This validator builds a adds several child validators that would
        be part of an HTML form submission.'''

        super(NewOrderPostValidator, self).__init__(parameters,
                                                    child_validators,
                                                    name)

        self.add_child(ProductIsSelectedValidator(parameters))
        self.add_child(OutputFormatValidator(parameters))

        if 'reproject' in parameters and parameters['reproject'] == 'on':
            self.add_child(ProjectionValidator(parameters))

        if 'resize' in parameters and parameters['resize'] == 'on':
            self.add_child(PixelSizeValidator(parameters))

        if ('image_extents' in parameters
                and parameters['image_extents'] == 'on'):

            self.add_child(ImageExtentsValidator(parameters))

    def errors(self):
        '''Trigger the child validators by overriding the error() method
        and calling the error() method defined in Validator superclass'''

        return super(NewOrderPostValidator, self).errors()


class NewOrderValidator(Validator):

    def __init__(self, parameters, child_validators=None, name=None):

        # this is necessary because Django usually includes all the normal
        # single value parameter values as a tuple.  The only thing coming
        # in that should be a list is input_products
        # This method will work fine for a normal dictionary as well
        for key, item in parameters.iteritems():
            if (type(item) is list
                    and key is not 'input_products' and len(item) > 0):

                parameters[key] = item[0]

        super(NewOrderValidator, self).__init__(parameters,
                                                child_validators,
                                                name)

        self.add_child(NewOrderPostValidator(parameters))
        self.add_child(NewOrderFilesValidator(parameters))

    def errors(self):
        '''Trigger the child validators by overriding the error() method
        and calling the error() method defined in Validator superclass'''

        return super(NewOrderValidator, self).errors()
