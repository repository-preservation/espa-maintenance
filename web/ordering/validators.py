from validation import Validator
import lta
import core
from models import Order


class SceneListValidator(Validator):
    '''Validates that a scene list has been provided and it contains at 
    least one scene to process'''

    def _get_scenelist(self, scenelist):
        
        if scenelist:
            for line in scenelist:
    
               if line.find('.tar.gz') != -1:
                   line = line[0:line.index('.tar.gz')]

               yield line

    def _line_header_ok(self, line):
        return len(line) >= 15 and \
            (line.startswith("LT") or line.startswith("LE"))

    def errors(self):
        '''Looks through the scenelist if present and determines if there
        are valid scenes to process'''
      
        if not 'scenelist' in self.parameters:
            return super(SceneListValidator, self).errors()
        else:
            scenelist = self._get_scenelist(self.parameters['scenelist'])

            scene_list = list()

            for line in scenelist:
                print(line)
                if self._line_header_ok(line):
                    scene_list.append(line)
                                
            if len(scene_list) > 0:
                # Run the submitted list by LTA so they can make sure
                # the items are in the inventory
                client = lta.OrderWrapperServiceClient()

                verified = client.verify_scenes(list(scene_list))

                for scene, valid in verified.iteritems():
                    if valid == 'false':
                        msg = "%s not found in Landsat inventory" % scene
                        self.add_error('scenelist', [msg])
            else:
                msg = "No scenes found in order file."
                self.add_error('scenelist', [msg,] )

        return super(SceneListValidator, self).errors()


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
        
        if not 'output_format' in self.parameters \
            and self.parameters['output_format']:
                self.add_error('output_format', 
                               ['Please select an output format', ])
        elif self.parameters['output_format'] not in valid_formats:
            self.add_error('output_format', 
                           ['Output format must be one of:%s' % valid_formats])
            

class FalseEastingValidator(Validator):
    '''Validates the false_easting parameter'''
    
    def errors(self):

        if not 'false_easting' in self.parameters\
            or not core.is_number(self.parameters['false_easting']):
                msg = "Please provide a valid false easting value"
                self.add_error('false_easting', [msg, ])

        return super(FalseEastingValidator, self).errors()


class FalseNorthingValidator(Validator):
    '''Validates the false_northing parameter'''
    
    def errors(self):

        if not 'false_northing' in self.parameters\
            or not core.is_number(self.parameters['false_northing']):
                msg = "Please provide a valid false northing value"
                self.add_error('false_northing', [msg, ])

        return super(FalseNorthingValidator, self).errors()


class CentralMeridianValidator(Validator):
    '''Validates the central_meridian parameter'''
    
    def errors(self):

        if not 'central_meridian' in self.parameters\
            or not core.is_number(self.parameters['central_meridian']):
                msg = "Please provide a valid central meridian value"
                self.add_error('central_meridian', [msg, ])

        return super(CentralMeridianValidator, self).errors()


class LatitudeTrueScaleValidator(Validator):
    '''Validates the latitude_true_scale parameter'''
    def errors(self):
        msg = "Please provide a valid Latitude True Scale\
               value in the ranges of -60.0 to -90.0 or 60.0 to 90.0"

        ts = None

        if 'latitude_true_scale' in self.parameters\
            and core.is_number(self.parameters['latitude_true_scale']):
            ts = float(self.parameters['latitude_true_scale'])
        else:
            self.add_error('latitude_true_scale', [msg, ])

        if ts and not (ts in range(60.0, 91.0) or ts in range(-90.0, -59.0)):
            self.add_error('latitude_true_scale', [msg, ])

        return super(LatitudeTrueScaleValidator, self).errors()


class LongitudinalPoleValidator(Validator):
    '''Validates the longitudinal_pole parameter'''
    
    def errors(self):

        if not 'longitude_pole' in self.parameters\
            or not core.is_number(self.parameters['longitude_pole']):
                msg = "Please provide a valid longitudinal pole value"
                self.add_error('longitude_pole', [msg, ])

        return super(LongitudinalPoleValidator, self).errors()


class StandardParallel1Validator(Validator):
    '''Validates the std_parallel_1 parameter'''
    
    def errors(self):

        if not 'std_parallel_1' in self.parameters\
            or not core.is_number(self.parameters['std_parallel_1']):
                msg = "Please provide a valid 1st standard parallel value"
                self.add_error('std_parallel_1', [msg, ])

        return super(StandardParallel1Validator, self).errors()


class StandardParallel2Validator(Validator):
    '''Validates the std_parallel_2 parameter'''
    
    def errors(self):

        if not 'std_parallel_2' in self.parameters\
            or not core.is_number(self.parameters['std_parallel_2']):
                msg = "Please provide a valid 2nd standard parallel value"
                self.add_error('std_parallel_1', [msg, ])

        return super(StandardParallel2Validator, self).errors()


class OriginLatitudeValidator(Validator):
    '''Validates origin_lat'''
    
    def errors(self):

        if not 'origin_lat' in self.parameters\
            or not core.is_number(self.parameters['origin_lat']):
                msg = "Please provide a valid latitude of origin value"
                self.add_error('origin_lat', [msg, ])

        return super(OriginLatitudeValidator, self).errors()


class DatumValidator(Validator):
    '''Validates datum for albers projection'''
    valid_datum = ['nad27', 'nad83', 'wgs84']

    def errors(self):

         if not 'datum' in self.parameters\
            or not self.parameters['datum'] in self.valid_datum:
                msg = "Please select a datum from one of:%s" % self.valid_datum
                self.add_error('datum', [msg, ])

         return super(DatumValidator, self).errors()


class UTMZoneValidator(Validator):
    '''Validates utm_zone for utm projection'''
    
    def errors(self):

        if not 'utm_zone' in self.parameters\
            or not str(self.parameters['utm_zone']).isdigit() \
            or not int(self.parameters['utm_zone']) in range(1, 61):
                msg = "Please provide a utm zone between 1 and 60"
                self.add_error('utm_zone', [msg, ])

        return super(UTMZoneValidator, self).errors()


class UTMNorthSouthValidator(Validator):
    '''Validates utm_north_south for utm projection'''
    def errors(self):

        if not 'utm_north_south' in self.parameters\
            or not self.parameters['utm_north_south'] in ('north', 'south'):
                msg = "Please select north or south for the UTM zone"
                self.add_error('utm_north_south', [msg, ])

        return super(UTMNorthSouthValidator, self).errors()


class ProjectionValidator(Validator):
    '''Validates parameters for reprojection'''

    valid_projections = ['aea', 'ps', 'sinu', 'longlat', 'utm']

    def __init__(self, parameters, child_validators=None, name=None):
        '''Conditionally build and attach child validators'''
        # delegate the call to superclass since we are overriding the
        # __init__ method
        super(ProjectionValidator, self).__init__(parameters,
                                                         child_validators,
                                                         name)

        # check for projection value and add appropriate child validators
        proj = None

        if not 'target_projection' in parameters:
            self.add_error("projection", ['projection must be specified'])
        else:
            proj = parameters['projection']

        if proj and proj not in self.valid_projections:

            self.add_error("projection",
                           ['projection must be one of %s'
                               % self.valid_projections])
        else:
            if proj is 'aea':
                self.add_child(AlbersValidator(parameters))
            elif proj is 'ps':
                self.add_child(PolarStereographicValidator(parameters))
            elif proj is 'sinu':
                self.add_child(SinusoidalValidator(parameters))
            elif proj is 'longlat':
                self.add_child(GeographicValidator(parameters))
            elif proj is 'utm':
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
        return super(AlbersValidator, self).errors()


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
         
        if 'pixel_size' in self.parameters\
            and core.is_number(self.parameters['pixel_size']):
            ps = float(self.parameters['pixel_size'])
        else:
            self.add_error('pixel_size', [msg, ])

        if ps and not ps in range(30.0, 1001.0):
            self.add_error('pixel_size', [msg, ])

        return super(MeterPixelSizeValidator, self).errors()


class DecimalDegreePixelSizeValidator(Validator):
    '''Validates pixel sizes specified in decimal degrees'''
    
    def errors(self):

        msg = ''.join(["Please enter a pixel size between",
                       " 0.0002695 to 0.0089831 decimal degrees"])
                       
        msg1 = "Valid pixel size is 0.0002695 to 0.0089831 decimal degrees"
        
        ps = None
         
        if 'pixel_size' in self.parameters\
            and core.is_number(self.parameters['pixel_size']):
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

        if not 'pixel_size_units' in self.parameters \
            or not self.parameters['pixel_size_units']:
            msg = "Target pixel size units not recognized"
            self.add_error('pixel_size_units', [msg, ])
        else:
            units = self.parameters['pixel_size_units'].strip()

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
        if not 'minx' in P or not core.is_number(P['minx']):
            msg = "Please provide a valid upper left x value"
            self.add_error('minx', [msg, ])
        else:
            minx = float(P['minx'])

        if not 'maxx' in P or not core.is_number(P['maxx']):
            msg = "Please provide a valid lower right x value"
            self.add_error('maxx', [msg, ])
        else:
            maxx = float(P['maxx'])

        if not 'miny' in P or not core.is_number(P['miny']):
            msg = "Please provide a valid lower right y value"
            self.add_error('miny', [msg, ])
        else:
            miny = float(P['miny'])

        if not 'maxy' in P or not core.is_number(P['maxy']):
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

        self.add_child(SceneListValidator(parameters))

    def errors(self):
        '''Validates a file was provided on upload and delegates calls to
        its child, SceneListValidator'''
        
        msg = ''.join(['Please provide a scene list file with at least',
                 ' one scene for processing'])
        
        if not 'scenelist' in self.parameters:
            self.add_error('files', [msg, ])

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

        if 'reproject' in self.parameters \
            and self.parameters['reproject'] == 'on':

            self.add_child(ProjectionValidator(parameters))

        if 'resize' in self.parameters and self.parameters['resize'] == 'on':

            self.add_child(PixelSizeValidator(parameters))

        if 'image_extents' in self.parameters \
            and self.parameters['image_extents'] == 'on':

            self.add_child(ImageExtentsValidator(parameters))

    def errors(self):
        '''Trigger the child validators by overriding the error() method
        and calling the error() method defined in Validator superclass'''

        return super(NewOrderPostValidator, self).errors()
        
        
class NewOrderValidator(Validator):
    
    def __init__(self, parameters, child_validators=None, name=None):
        super(NewOrderValidator, self).__init__(parameters,
                                                child_validators,
                                                name)
        
        self.add_child(NewOrderPostValidator(self.parameters))
        self.add_child(NewOrderFilesValidator(self.parameters))
        
    def errors(self):
        '''Trigger the child validators by overriding the error() method
        and calling the error() method defined in Validator superclass'''

        return super(NewOrderValidator, self).errors()
