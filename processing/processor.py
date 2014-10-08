
import os
import sys
import shutil

# imports from espa/espa_common
try:
    from logger_factory import EspaLogging
except:
    from espa_common.logger_factory import EspaLogging

try:
    import sensor
except:
    from espa_common import sensor

try:
    import settings
except:
    from espa_common import settings

try:
    import utilities
except:
    from espa_common import utilities

# local objects and methods
import espa_exception as ee
# import validators ---- THIS SEEMS TOO COMPLEX TO USE??????
#                   ---- Might be significantly easier with more sub dicts
import parameters
import metadata
import warp
import staging


# ===========================================================================
class ProductProcessor(object):

    _logger = None

    # -------------------------------------------
    def __init__(self):
        '''
        Description:
            Initialization for the object.
        '''

        self._logger = EspaLogging.get_logger('espa.processing')

    # -------------------------------------------
    def validate_parameters(self, parms):
        '''
        Description:
            Validates the parameters required for the processor.
        '''

        logger = self._logger

        # Test for presence of top-level parameters
        keys = ['orderid', 'scene', 'product_type', 'options']
        for key in keys:
            if not parameters.test_for_parameter(parms, key):
                raise RuntimeError("Missing required input parameter [%s]"
                                   % key)

        # TODO - Remove this once we have converted
        if not parameters.test_for_parameter(parms, 'product_id'):
            logger.warning("'product_id' parameter missing defaulting to"
                           " 'scene'")
            parms['product_id'] = parms['scene']

    # -------------------------------------------
    def initialize_processing_directory(self, parms):
        '''
        Description:
            Initializes the processing directory.  Creates the following
            directories.

            .../output
            .../stage
            .../work

        Note:
            order_id and product_id along with the ESPA_WORK_DIR environment
            variable provide the path to the processing locations.
        '''

        logger = self._logger

        order_id = parms['orderid']
        product_id = parms['product_id']

        base_env_var = 'ESPA_WORK_DIR'
        base_dir = ''

        if base_env_var not in os.environ:
            logger.warning("Environment variable $%s is not defined"
                           % base_env_var)
        else:
            base_dir = os.environ.get(base_env_var)

        # Get the absolute path to the directory, and default to the current
        # one
        if base_dir == '':
            # If the directory is empty, use the current working directory
            base_dir = os.getcwd()
        else:
            # Get the absolute path
            base_dir = os.path.abspath(base_dir)

        # Add the order_id to the base path
        self._order_dir = os.path.join(base_dir, str(order_id))

        # Add the product_id to the order path
        self._product_dir = os.path.join(self._order_dir, str(product_id))

        # Just incase remove it, and we don't care about errors since it
        # doesn't exist (probably only needed for developer runs)
        shutil.rmtree(self._product_dir, ignore_errors=True)

        # Specify the sub-directories of the product directory
        self._stage_dir = os.path.join(self._product_dir, 'stage')
        self._work_dir = os.path.join(self._product_dir, 'work')
        self._output_dir = os.path.join(self._product_dir, 'output')

        # Create each of the sub-directories
        try:
            staging.create_directory(self._stage_dir)
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.creating_stage_dir,
                                   str(e)), None, sys.exc_info()[2]

        try:
            staging.create_directory(self._work_dir)
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.creating_work_dir,
                                   str(e)), None, sys.exc_info()[2]

        try:
            staging.create_directory(self._output_dir)
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.creating_output_dir,
                                   str(e)), None, sys.exc_info()[2]

    # -------------------------------------------
    def process(self, parms):
        '''
        Description:
            Generates a product through a defined process.

        Note:
            Not implemented here.
        '''

        msg = ("[%s] Requires implementation in the child class"
               % self.process.__name__)
        raise NotImplementedError(msg)


# ===========================================================================
class CustomizationProcessor(ProductProcessor):

    _WGS84 = 'WGS84'
    _NAD27 = 'NAD27'
    _NAD83 = 'NAD83'

    _valid_projections = None
    _valid_ns = None
    _valid_resample_methods = None
    _valid_pixel_size_units = None
    _valid_image_extents_units = None
    _valid_datums = None

    # -------------------------------------------
    def __init__(self):
        super(CustomizationProcessor, self).__init__()

        self._valid_projections = ['sinu', 'aea', 'utm', 'ps', 'lonlat']
        self._valid_ns = ['north', 'south']
        self._valid_resample_methods = ['near', 'bilinear', 'cubic',
                                        'cubicspline', 'lanczos']
        self._valid_pixel_size_units = ['meters', 'dd']
        self._valid_image_extents_units = ['meters', 'dd']
        self._valid_datums = [self._WGS84, self._NAD27, self._NAD83]

    # -------------------------------------------
    def validate_parameters(self, parms):
        '''
        Description:
            Validates the parameters required for the processor.
        '''

        logger = self._logger

        # Call the base class parameter validation
        super(CustomizationProcessor, self).validate_parameters(parms)

        logger.info("Validating [CustomizationProcessor] parameters")

        # TODO TODO TODO - Validate the WARP parameters here
        # TODO TODO TODO - Pull the validation here??????
        parameters. \
            validate_reprojection_parameters(parms,
                                             parms['product_id'],
                                             self._valid_projections,
                                             self._valid_ns,
                                             self._valid_pixel_size_units,
                                             self._valid_image_extents_units,
                                             self._valid_resample_methods,
                                             self._valid_datums)


# ===========================================================================
class CDRProcessor(CustomizationProcessor):
    '''
    Description:
        Provides the super class implementation for processing products.
    '''

    _order_dir = None
    _product_dir = None
    _stage_dir = None
    _output_dir = None
    _work_dir = None

    # -------------------------------------------
    def __init__(self):
        super(CDRProcessor, self).__init__()

    # -------------------------------------------
    def validate_parameters(self, parms):
        '''
        Description:
            Validates the parameters required for all processors.
        '''

        logger = self._logger

        # Call the base class parameter validation
        super(CDRProcessor, self).validate_parameters(parms)

        logger.info("Validating [CDRProcessor] parameters")

    # -------------------------------------------
    def log_command_line(self, parms):
        '''
        Description:
            Builds and logs the processor command line
        '''

        logger = self._logger

        cmd = [os.path.basename(__file__)]
        cmd_line_options = \
            parameters.convert_to_command_line_options(parms)
        cmd.extend(cmd_line_options)
        cmd = ' '.join(cmd)
        logger.info("PROCESSOR COMMAND LINE [%s]" % cmd)

    # -------------------------------------------
    def stage_input_data(self, parms):
        '''
        Description:
            Stages the input data required for the processor.

        Note:
            Not implemented here.
        '''

        msg = ("[%s] Requires implementation in the child class"
               % self.stage_input_date.__name__)
        raise NotImplementedError(msg)

    # -------------------------------------------
    def build_science_products(self, parms):
        '''
        Description:
            Stages the input data required for the processor.

        Note:
            Not implemented here.
        '''

        msg = ("[%s] Requires implementation in the child class"
               % self.build_science_products.__name__)
        raise NotImplementedError(msg)

    # -------------------------------------------
    def process(self, parms):
        '''
        Description:
            Generates a product through a defined process.
        '''

        logger = self._logger

        # Validate the parameters
        self.validate_parameters(parms)

        # Log the command line that can be used for this processor
        self.log_command_line(parms)

        # Initialize the processing directory.
        self.initialize_processing_directory(parms)

        # Stage the required input data
        self.stage_input_data(parms)

        # Build science products
        self.build_science_products(parms)

        # Customize products
        # TODO TODO TODO
        # self.customize_products(parms)

        # Generate statistics products
        # TODO TODO TODO
        # self.generate_statistics(parms)

        # Deliver product
        # TODO TODO TODO

        # Cleanup the processing directory to free disk space for other
        # products to process.
        # TODO TODO TODO
        # self.cleanup_processing_directory()


# ===========================================================================
class LandsatProcessor(CDRProcessor):

    _metadata_filename = None
    _xml_filename = None

    # -------------------------------------------
    def __init__(self):
        super(LandsatProcessor, self).__init__()

    # -------------------------------------------
    # TODO TODO TODO - This may be in it's own CustomizationProcessor
    def validate_parameters(self, parms):
        '''
        Description:
            Validates the parameters required for the processor.
        '''

        logger = self._logger

        # Call the base class parameter validation
        super(LandsatProcessor, self).validate_parameters(parms)

        logger.info("Validating [LandsatProcessor] parameters")

        product_id = parms['product_id']
        options = parms['options']

        # Force these parameters to false if not provided
        # They are the required includes for science product generation
        required_includes = ['include_cfmask',
                             'include_customized_source_data',
                             'include_dswe',
                             'include_solr_index',
                             'include_source_data',
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
                             'include_sr_toa']

        for parameter in required_includes:
            if not parameters.test_for_parameter(options, parameter):
                logger.warning("'%s' parameter missing defaulting to False"
                               % parameter)
                options[parameter] = False

        # Determine if browse was requested and specify the default
        # resolution if a resolution was not specified
        if options['include_sr_browse']:
            if not parameters.test_for_parameter(options, 'browse_resolution'):
                logger.warning("'browse_resolution' parameter missing"
                               " defaulting to %d"
                               % settings.DEFAULT_BROWSE_RESOLUTION)
                options['browse_resolution'] = \
                    settings.DEFAULT_BROWSE_RESOLUTION

        # TODO TODO TODO - Shouldn't this really be it's own processor
        # Determine if SOLR was requested and specify the default collection
        # name if a collection name was not specified
        if options['include_solr_index']:
            if not parameters.test_for_parameter(options, 'collection_name'):
                logger.warning("'collection_name' parameter missing"
                               " defaulting to %s"
                               % settings.DEFAULT_SOLR_COLLECTION_NAME)
                options['collection_name'] = \
                    settings.DEFAULT_SOLR_COLLECTION_NAME

        # Force these parameters to false if not provided
        keys = ['include_statistics']

        for key in keys:
            if not parameters.test_for_parameter(options, key):
                logger.warning("'%s' parameter missing defaulting to False"
                               % key)
                options[key] = False

        # Extract information from the scene string
        sensor_name = sensor.instance(product_id).sensor_name.lower()

        # Add the sensor to the options
        options['sensor'] = sensor_name

        # Verify or set the source information
        if not parameters.test_for_parameter(options, 'source_host'):
            options['source_host'] = util.get_input_hostname(sensor_name)

        if not parameters.test_for_parameter(options, 'source_username'):
            options['source_username'] = None

        if not parameters.test_for_parameter(options, 'source_pw'):
            options['source_pw'] = None

        if not parameters.test_for_parameter(options, 'source_directory'):
            path = util.get_path(parms['scene'])
            row = util.get_row(parms['scene'])
            year = util.get_year(parms['scene'])
            options['source_directory'] = '%s/%s/%s/%s/%s' \
                % (settings.LANDSAT_BASE_SOURCE_PATH,
                   sensor_name, path, row, year)

        # Verify or set the destination information
        if not parameters.test_for_parameter(options, 'destination_host'):
            options['destination_host'] = util.get_output_hostname()

        if not parameters.test_for_parameter(options, 'destination_username'):
            options['destination_username'] = 'localhost'

        if not parameters.test_for_parameter(options, 'destination_pw'):
            options['destination_pw'] = 'localhost'

        if not parameters.test_for_parameter(options, 'destination_directory'):
            options['destination_directory'] = '%s/orders/%s' \
                % (settings.ESPA_BASE_OUTPUT_PATH, parms['orderid'])

    # -------------------------------------------
    def stage_input_data(self, parms):
        '''
        Description:
            Stages the input data required for the processor.
        '''

        product_id = parms['product_id']
        options = parms['options']

        # Stage the landsat data
        filename = staging.stage_landsat_data(product_id,
                                              options['source_host'],
                                              options['source_directory'],
                                              'localhost',
                                              self._stage_dir,
                                              options['source_username'],
                                              options['source_pw'])

        # Un-tar the input data to the work directory
        try:
            staging.untar_data(filename, self._work_dir)
            os.unlink(filename)
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.unpacking, str(e)), \
                None, sys.exc_info()[2]

    # -------------------------------------------
    def convert_to_raw_binary(self, parms):
        '''
        Description:
            Converts the Landsat(LPGS) input data to our internal raw binary
            format.
        '''

        logger = self._logger

        options = parms['options']

        # Build a command line arguments list
        cmd = ['convert_lpgs_to_espa',
               '--mtl', self._metadata_filename,
               '--xml', self._xml_filename]
        if not options['include_source_data']:
            cmd.append('--del_src_files')

        # Turn the list into a string
        cmd = ' '.join(cmd)
        logger.info(' '.join(['CONVERT LPGS TO ESPA COMMAND:', cmd]))

        output = ''
        try:
            output = utilities.execute_cmd(cmd)
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.reformat,
                                   str(e)), None, sys.exc_info()[2]
        finally:
            if len(output) > 0:
                logger.info(output)

    # -------------------------------------------
    def sr_command_line(self, parms):
        '''
        Description:
            Returns the command line required to generate surface reflectance.
        '''

        cmd = ' '.join(['do_ledaps.py', '--xml', self._xml_filename])

        # TODO TODO TODO - Make like olitirs version

        return cmd

    # -------------------------------------------
    def generate_sr_products(self, parms):
        '''
        Description:
            Returns the command line required to generate surface reflectance.
        '''

        logger = self._logger

        cmd = self.sr_command_line(parms)

        # Only if required
        if cmd is not None:

            logger.info(' '.join(['SURFACE REFLECTANCE COMMAND:', cmd]))

            output = ''
            try:
                output = utilities.execute_cmd(cmd)
            except Exception, e:
                raise ee.ESPAException(ee.ErrorCodes.surface_reflectance,
                                       str(e)), None, sys.exc_info()[2]
            finally:
                if len(output) > 0:
                    logger.info(output)

    # -------------------------------------------
    def cfmask_command_line(self, parms):
        '''
        Description:
            Returns the command line required to generate cfmask.
        '''

        logger = self._logger

        options = parms['options']

        cmd = None
        if options['include_cfmask'] or options['include_sr']:
            cmd = ' '.join(['cfmask', '--verbose', '--max_cloud_pixels',
                            settings.CFMASK_MAX_CLOUD_PIXELS,
                            '--xml', self._xml_filename])

        return cmd

    # -------------------------------------------
    def generate_cfmask(self, parms):
        '''
        Description:
            Returns the command line required to generate cfmask.
        '''

        logger = self._logger

        cmd = self.cfmask_command_line(parms)

        # Only if required
        if cmd is not None:

            logger.info(' '.join(['CFMASK COMMAND:', cmd]))

            output = ''
            try:
                output = utilities.execute_cmd(cmd)
            except Exception, e:
                raise ee.ESPAException(ee.ErrorCodes.cfmask,
                                       str(e)), None, sys.exc_info()[2]
            finally:
                if len(output) > 0:
                    logger.info(output)

    # -------------------------------------------
    def build_science_products(self, parms):
        '''
        Description:
            Build the science products requested by the user.
        '''

        logger = self._logger

        logger.info("[LandsatProcessor] Building Science Product")

        # Figure out the metadata filename
        try:
            landsat_metadata = \
                metadata.get_landsat_metadata(self._work_dir)
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.metadata,
                               str(e)), None, sys.exc_info()[2]
        self._metadata_filename = landsat_metadata['metadata_filename']
        del (landsat_metadata)  # Not needed anymore

        # Figure out the xml filename
        self._xml_filename = self._metadata_filename.replace('_MTL.txt',
                                                             '.xml')

        # Change to the working directory
        current_directory = os.getcwd()
        os.chdir(self._work_dir)

        try:
            self.convert_to_raw_binary(parms)

            self.generate_sr_products(parms)

            self.generate_cfmask(parms)

            # TODO TODO TODO
            #self.generate_sr_browse_data(parms)
            #self.generate_spectral_indices(parms)
            #self.generate_dswe(parms)

        finally:
            # Change back to the previous directory
            os.chdir(current_directory)


# ===========================================================================
class LandsatTMProcessor(LandsatProcessor):
    def __init__(self):
        super(LandsatTMProcessor, self).__init__()


# ===========================================================================
class LandsatETMProcessor(LandsatProcessor):
    def __init__(self):
        super(LandsatETMProcessor, self).__init__()


# ===========================================================================
class LandsatOLITIRSProcessor(LandsatProcessor):
    def __init__(self):
        super(LandsatOLITIRSProcessor, self).__init__()

    def sr_command_line(self, parms):
        '''
        Description:
            Returns the command line required to generate surface reflectance.
        '''

        logger = self._logger

        options = parms['options']

        cmd = ['do_l8_sr.py', '--xml', self._xml_filename]

        generate_sr = False

        # Check to see if SR is required
        if (options['include_sr']
                or options['include_sr_browse']
                or options['include_sr_nbr']
                or options['include_sr_nbr2']
                or options['include_sr_ndvi']
                or options['include_sr_ndmi']
                or options['include_sr_savi']
                or options['include_sr_msavi']
                or options['include_sr_evi']
                or options['include_dswe']):

            cmd.extend(['--process_sr', 'True'])
            generate_sr = True
        else:
            cmd.extend(['--process_sr', 'False'])

        # Check to see if Thermal or TOA is required
        if (options['include_sr_toa']
                or options['include_sr_thermal']
                or options['include_cfmask']):

            cmd.append('--write_toa')
            generate_sr = True

        # Only return a string if we will need to run SR processing
        if not generate_sr:
            cmd = None
        else:
            cmd = ' '.join(cmd)

        return cmd

    # -------------------------------------------
    def cfmask_command_line(self, parms):
        '''
        Description:
            Returns the command line required to generate cfmask.
        '''

        logger = self._logger

        options = parms['options']

        cmd = None
        if options['include_cfmask'] or options['include_sr']:
            cmd = ' '.join(['l8cfmask', '--verbose', '--max_cloud_pixels',
                            settings.CFMASK_MAX_CLOUD_PIXELS,
                            '--mtl', self._metadata_filename])
                            # TODO TODO TODO
                            # '--xml', self._xml_filename])

        return cmd


# ===========================================================================
class ModisProcessor(CDRProcessor):
    def __init__(self):
        super(ModisProcessor, self).__init__()


# ===========================================================================
class ModisAQUAProcessor(ModisProcessor):
    def __init__(self):
        super(ModisAQUAProcessor, self).__init__()


# ===========================================================================
class ModisTERRAProcessor(ModisProcessor):
    def __init__(self):
        super(ModisTERRAProcessor, self).__init__()


# ===========================================================================
class PlotProcessor(ProductProcessor):
    def __init__(self):
        super(PlotProcessor, self).__init__()


# ===========================================================================
def get_instance(product_id):
    '''
    Description:
        Provides a method to retrieve the proper processor for the specified
        product.
    '''

    if product_id == 'plot':
        return PlotProcessor()

    sensor_code = sensor.instance(product_id).sensor_code.lower()

    if sensor_code == 'lt4':
        raise NotImplementedError("A LT4 processor has not been implemented")
        return LandsatTMProcessor()
    elif sensor_code == 'lt5':
        raise NotImplementedError("A LT5 processor has not been implemented")
        return LandsatTMProcessor()
    elif sensor_code == 'le7':
        raise NotImplementedError("A LE7 processor has not been implemented")
        return LandsatETMProcessor()
    elif sensor_code == 'lc8':
        return LandsatOLITIRSProcessor()
    elif sensor_code == 'mod':
        raise NotImplementedError("A TERRA processor has not been implemented")
        return ModisTERRAProcessor()
    elif sensor_code == 'myd':
        raise NotImplementedError("A AQUA processor has not been implemented")
        return ModisAQUAProcessor()
    else:
        msg = "A processor for [%s] has not been implemented" % product_id
        raise NotImplementedError(msg)
