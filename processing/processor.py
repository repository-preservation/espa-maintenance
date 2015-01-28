
'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Implements the processors which generate the products the system is capable
  of producing.

History:
  Created Oct/2014 by Ron Dilley, USGS/EROS

    Date              Programmer               Reason
    ----------------  ------------------------ -------------------------------
    Oct/2014          Ron Dilley               Initial implementation
                                               Most of the code was taken from
                                               the previous implementation
                                               code/modules.

'''


import os
import sys
import shutil
import glob
import json
import datetime
from time import sleep
from cStringIO import StringIO
from collections import defaultdict
from matplotlib import pyplot as mpl_plot
from matplotlib import dates as mpl_dates
from matplotlib.ticker import MaxNLocator
import numpy as np

# imports from espa_common
from logger_factory import EspaLogging
import sensor
import settings
import utilities

# local objects and methods
import espa_exception as ee
import parameters
import metadata
import metadata_api
import warp
import staging
import statistics
import transfer
import distribution


# ===========================================================================
class ProductProcessor(object):
    '''
    Description:
        Provides the super class for all product request processing.  It
        performs the tasks needed by all processors.

        It initializes the logger object and keeps it around for all the
        child-classes to use.

        It implements initialization of the order and product directory
        structures.

        It also implements the cleanup of the product directory.
    '''

    _logger = None

    _parms = None

    _order_dir = None
    _product_dir = None
    _stage_dir = None
    _output_dir = None
    _work_dir = None

    _build_products = False

    _product_name = None

    # -------------------------------------------
    def __init__(self, parms):
        '''
        Description:
            Initialization for the object.
        '''

        self._logger = EspaLogging.get_logger(settings.PROCESSING_LOGGER)

        # Some minor enforcement for what parms should be
        if type(parms) is dict:
            self._parms = parms
        else:
            raise Exception("parameters was of type %s, dict required"
                            % type(parms))

        # Validate the parameters
        self.validate_parameters()

    # -------------------------------------------
    def get_output_hostname(self):
        '''
        Description:
            Determine the output hostname to use for espa products.
        Note:
            Today all output products use the landsat online cache which is
            provided by utilities.get_cache_hostname.
        '''

        return utilities.get_cache_hostname()

    # -------------------------------------------
    def get_output_directory(self):
        '''
        Description:
            Determine the output directory to use for espa products.
        Note:
            Today all output products go to the same directory.
        '''

        order_id = self._parms['orderid']

        return os.path.join(settings.ESPA_CACHE_DIRECTORY, order_id)

    # -------------------------------------------
    def validate_parameters(self):
        '''
        Description:
            Validates the parameters required for the processor.
        '''

        logger = self._logger

        # Test for presence of required top-level parameters
        keys = ['orderid', 'scene', 'product_type', 'options']
        for key in keys:
            if not parameters.test_for_parameter(self._parms, key):
                raise RuntimeError("Missing required input parameter [%s]"
                                   % key)

        # Set the download URL to None if not provided
        if not parameters.test_for_parameter(self._parms, 'download_url'):
            self._parms['download_url'] = None

        # TODO - Remove this once we have converted
        if not parameters.test_for_parameter(self._parms, 'product_id'):
            logger.warning("'product_id' parameter missing defaulting to"
                           " 'scene'")
            self._parms['product_id'] = self._parms['scene']

        # Validate the options
        options = self._parms['options']

        # Default this so the directory is not kept, it should only be
        # present and turned on for developers
        if not parameters.test_for_parameter(options, 'keep_directory'):
            options['keep_directory'] = False

        # Verify or set the destination information
        if not parameters.test_for_parameter(options, 'destination_host'):
            options['destination_host'] = self.get_output_hostname()

        if not parameters.test_for_parameter(options, 'destination_username'):
            options['destination_username'] = 'localhost'

        if not parameters.test_for_parameter(options, 'destination_pw'):
            options['destination_pw'] = 'localhost'

        if not parameters.test_for_parameter(options, 'destination_directory'):
            options['destination_directory'] = self.get_output_directory()

    # -------------------------------------------
    def log_order_parameters(self):
        '''
        Description:
            Log the order parameters in json format.
        '''

        logger = self._logger

        logger.info("MAPPER OPTION LINE %s"
                    % json.dumps(self._parms, sort_keys=True))

    # -------------------------------------------
    def initialize_processing_directory(self):
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

        product_id = self._parms['product_id']
        order_id = self._parms['orderid']

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
    def remove_product_directory(self):
        '''
        Description:
            Remove the product directory.
        '''

        options = self._parms['options']

        # We don't care about this failing, we just want to attempt to free
        # disk space to be nice to the whole system.  If this processing
        # request failed due to a processing issue.  Otherwise, with
        # successfull processing, hadoop cleans up after itself.
        if self._product_dir is not None and not options['keep_directory']:
            shutil.rmtree(self._product_dir, ignore_errors=True)

    # -------------------------------------------
    def get_product_name(self):
        '''
        Description:
            Build the product name from the product information and current
            time.

        Note:
            Not implemented here.
        '''

        msg = ("[%s] Requires implementation in the child class"
               % self.get_product_name.__name__)
        raise NotImplementedError(msg)

    # -------------------------------------------
    def distribute_product(self):
        '''
        Description:
            Does both the packaging and dsitribution of the product using
            the distribution module.
        '''

        logger = self._logger

        product_id = self._parms['product_id']
        opts = self._parms['options']

        product_name = self.get_product_name()

        # Deliver the product files
        # Attempt X times sleeping between each attempt
        sleep_seconds = settings.DEFAULT_SLEEP_SECONDS
        max_number_of_attempts = settings.MAX_DISTRIBUTION_ATTEMPTS
        attempt = 0
        destination_product_file = 'ERROR'
        destination_cksum_file = 'ERROR'
        while True:
            try:
                # Deliver product will also try each of its parts three times
                # before failing, so we pass our sleep seconds down to them
                (destination_product_file, destination_cksum_file) = \
                    distribution.deliver_product(product_id,
                                                 self._work_dir,
                                                 self._output_dir,
                                                 product_name,
                                                 opts['destination_host'],
                                                 opts['destination_directory'],
                                                 opts['destination_username'],
                                                 opts['destination_pw'],
                                                 sleep_seconds)

                # Always log where we placed the files
                logger.info("Delivered product to %s at location %s and cksum"
                            " location %s" % (opts['destination_host'],
                                              destination_product_file,
                                              destination_cksum_file))

                logger.info("*** Product Delivery Complete ***")
            except Exception, e:
                logger.error("An exception occurred delivering the product")
                logger.error("Exception Message: %s" % str(e))
                if attempt < max_number_of_attempts:
                    sleep(sleep_seconds)  # sleep before trying again
                    attempt += 1
                    # adjust for next set
                    sleep_seconds = int(sleep_seconds * 1.5)
                    continue
                else:
                    # May already be an ESPAException so don't override that
                    raise e
            break

        # Let the caller know where we put these on the destination system
        return (destination_product_file, destination_cksum_file)

    # -------------------------------------------
    def process_product(self):
        '''
        Description:
            Perform the processor specific processing to generate the request
            product.

        Note:
            Not implemented here.

        Note:
            Must return the destination product and cksum file names.
        '''

        msg = ("[%s] Requires implementation in the child class"
               % self.process_product.__name__)
        raise NotImplementedError(msg)

    # -------------------------------------------
    def process(self):
        '''
        Description:
            Generates a product through a defined process.
            This method must cleanup everything it creates by calling the
            remove_product_directory() method.

        Note:
            Must return the destination product and cksum file names.
        '''

        # Logs the order parameters that can be passed to the mapper for this
        # processor
        self.log_order_parameters()

        # Initialize the processing directory.
        self.initialize_processing_directory()

        try:
            (destination_product_file, destination_cksum_file) = \
                self.process_product()

        finally:
            # Remove the product directory
            # Free disk space to be nice to the whole system.
            self.remove_product_directory()

        return (destination_product_file, destination_cksum_file)


# ===========================================================================
class CustomizationProcessor(ProductProcessor):
    '''
    Description:
        Provides the super class implementation for customization processing,
        which warps the products to the user requested projection.
    '''

    _WGS84 = 'WGS84'
    _NAD27 = 'NAD27'
    _NAD83 = 'NAD83'

    _valid_projections = None
    _valid_ns = None
    _valid_resample_methods = None
    _valid_pixel_size_units = None
    _valid_image_extents_units = None
    _valid_datums = None

    _xml_filename = None

    # -------------------------------------------
    def __init__(self, parms):

        self._valid_projections = ['sinu', 'aea', 'utm', 'ps', 'lonlat']
        self._valid_ns = ['north', 'south']
        self._valid_resample_methods = ['near', 'bilinear', 'cubic',
                                        'cubicspline', 'lanczos']
        self._valid_pixel_size_units = ['meters', 'dd']
        self._valid_image_extents_units = ['meters', 'dd']
        self._valid_datums = [self._WGS84, self._NAD27, self._NAD83]

        super(CustomizationProcessor, self).__init__(parms)

    # -------------------------------------------
    def validate_parameters(self):
        '''
        Description:
            Validates the parameters required for the processor.
        '''

        logger = self._logger

        # Call the base class parameter validation
        super(CustomizationProcessor, self).validate_parameters()

        product_id = self._parms['product_id']
        options = self._parms['options']

        logger.info("Validating [CustomizationProcessor] parameters")

        # TODO TODO TODO - Pull the validation here??????
        parameters. \
            validate_reprojection_parameters(options,
                                             product_id,
                                             self._valid_projections,
                                             self._valid_ns,
                                             self._valid_pixel_size_units,
                                             self._valid_image_extents_units,
                                             self._valid_resample_methods,
                                             self._valid_datums)

        # Update the xml filename to be correct
        self._xml_filename = '.'.join([product_id, 'xml'])

    # -------------------------------------------
    def customize_products(self):
        '''
        Description:
            Performs the customization of the products.
        '''

        # Nothing to do if the user did not specify anything to build
        if not self._build_products:
            return

        product_id = self._parms['product_id']
        options = self._parms['options']

        # Reproject the data for each product, but only if necessary
        if (options['reproject']
                or options['resize']
                or options['image_extents']
                or options['projection'] is not None):

            # The warp method requires this parameter
            options['work_directory'] = self._work_dir

            warp.warp_espa_data(options, product_id, self._xml_filename)


# ===========================================================================
class CDRProcessor(CustomizationProcessor):
    '''
    Description:
        Provides the super class implementation for generating CDR products.
    '''

    # -------------------------------------------
    def __init__(self, parms):
        super(CDRProcessor, self).__init__(parms)

    # -------------------------------------------
    def validate_parameters(self):
        '''
        Description:
            Validates the parameters required for all processors.
        '''

        # Call the base class parameter validation
        super(CDRProcessor, self).validate_parameters()

    # -------------------------------------------
    def stage_input_data(self):
        '''
        Description:
            Stages the input data required for the processor.

        Note:
            Not implemented here.
        '''

        msg = ("[%s] Requires implementation in the child class"
               % self.stage_input_data.__name__)
        raise NotImplementedError(msg)

    # -------------------------------------------
    def build_science_products(self):
        '''
        Description:
            Build the science products requested by the user.

        Note:
            Not implemented here.
        '''

        msg = ("[%s] Requires implementation in the child class"
               % self.build_science_products.__name__)
        raise NotImplementedError(msg)

    # -------------------------------------------
    def cleanup_work_dir(self):
        '''
        Description:
            Cleanup all the intermediate non-products and the science
            products not requested.

        Note:
            Not implemented here.
        '''

        msg = ("[%s] Requires implementation in the child class"
               % self.cleanup_work_dir.__name__)
        raise NotImplementedError(msg)

    # -------------------------------------------
    def remove_products_from_xml(self):
        '''
        Description:
            Remove the specified products from the XML file.  The file is
            read into memory, processed, and written back out with out the
            specified products.
        '''

        # Nothing to do if the user did not specify anything to build
        if not self._build_products:
            return

        logger = self._logger

        options = self._parms['options']

        # Map order options to the products in the XML files
        order2xml_mapping = {
            'include_customized_source_data': ['L1T', 'L1G', 'L1GT'],
            'include_sr': 'sr_refl',
            'include_sr_toa': 'toa_refl',
            'include_sr_thermal': 'toa_bt',
            'include_cfmask': 'cfmask'
        }

        # If nothing to do just return
        if self._xml_filename is None:
            return

        # Remove generated products that were not requested
        products_to_remove = []
        if not options['include_customized_source_data']:
            products_to_remove.extend(
                order2xml_mapping['include_customized_source_data'])
        if not options['include_sr']:
            products_to_remove.append(
                order2xml_mapping['include_sr'])
        if not options['include_sr_toa']:
            products_to_remove.append(
                order2xml_mapping['include_sr_toa'])
        if not options['include_sr_thermal']:
            products_to_remove.append(
                order2xml_mapping['include_sr_thermal'])
        # These both need to be false before we delete the cfmask files
        # Because our defined SR product includes the cfmask band
        if not options['include_cfmask'] and not options['include_sr']:
            products_to_remove.append(
                order2xml_mapping['include_cfmask'])

        if products_to_remove is not None:
            espa_xml = metadata_api.parse(self._xml_filename, silence=True)
            bands = espa_xml.get_bands()

            file_names = []

            # Remove them from the file system first
            for band in bands.band:
                if band.product in products_to_remove:
                    # Add the .img file
                    file_names.append(band.file_name)
                    # Add the .hdr file
                    hdr_file_name = band.file_name.replace('.img', '.hdr')
                    file_names.append(hdr_file_name)

            # Only remove files if we found some
            if len(file_names) > 0:

                cmd = ' '.join(['rm', '-rf'] + file_names)
                logger.info(' '.join(["REMOVING INTERMEDIATE PRODUCTS NOT"
                                      " REQUESTED", 'COMMAND:', cmd]))

                try:
                    output = utilities.execute_cmd(cmd)
                except Exception, e:
                    raise ee.ESPAException(ee.ErrorCodes.remove_products,
                                           str(e)), None, sys.exc_info()[2]
                finally:
                    if len(output) > 0:
                        logger.info(output)

                # Remove them from the XML by creating a new list of all the
                # others
                bands.band[:] = [band for band in bands.band
                                 if band.product not in products_to_remove]

                try:
                    # Export the file with validation
                    with open(self._xml_filename, 'w') as xml_fd:
                        # Export to the file and specify the namespace/schema
                        xmlns = "http://espa.cr.usgs.gov/v1.1"
                        xmlns_xsi = "http://www.w3.org/2001/XMLSchema-instance"
                        schema_uri = ("http://espa.cr.usgs.gov/static/schema/"
                                      "espa_internal_metadata_v1_1.xsd")
                        metadata_api.export(xml_fd, espa_xml,
                                            xmlns=xmlns,
                                            xmlns_xsi=xmlns_xsi,
                                            schema_uri=schema_uri)

                except Exception, e:
                    raise ee.ESPAException(ee.ErrorCodes.remove_products,
                                           str(e)), None, sys.exc_info()[2]
                finally:
                    if len(output) > 0:
                        logger.info(output)
            # END - if file_names

            # Cleanup
            del bands
            del espa_xml
        # END - if products_to_remove

    # -------------------------------------------
    def generate_statistics(self):
        '''
        Description:
            Generates statistics if required for the processor.

        Note:
            Not implemented here.
        '''

        msg = ("[%s] Requires implementation in the child class"
               % self.generate_statistics.__name__)
        raise NotImplementedError(msg)

    # -------------------------------------------
    def distribute_statistics(self):
        '''
        Description:
            Generates statistics if required for the processor.

        Note:
            Not implemented here.
        '''

        logger = self._logger

        product_id = self._parms['product_id']
        options = self._parms['options']

        if options['include_statistics']:
            # Attempt X times sleeping between each attempt
            attempt = 0
            sleep_seconds = settings.DEFAULT_SLEEP_SECONDS
            dest_host = options['destination_host']
            dest_directory = options['destination_directory']
            dest_user = options['destination_username']
            dest_pw = options['destination_pw']
            while True:
                try:
                    distribution.distribute_statistics(product_id,
                                                       self._work_dir,
                                                       dest_host,
                                                       dest_directory,
                                                       dest_user,
                                                       dest_pw)

                    logger.info("*** Statistics Distribution Complete ***")
                except Exception, e:
                    logger.error("An exception occurred distributing"
                                 " statistics")
                    logger.error("Exception Message: %s" % str(e))
                    if attempt < settings.MAX_DELIVERY_ATTEMPTS:
                        sleep(sleep_seconds)  # sleep before trying again
                        attempt += 1
                        continue
                    else:
                        e_code = ee.ErrorCodes.distributing_product
                        raise ee.ESPAException(e_code,
                                               str(e)), None, sys.exc_info()[2]
                break

    # -------------------------------------------
    def reformat_products(self):
        '''
        Description:
            Reformat the customized products if required for the processor.
        '''

        # Nothing to do if the user did not specify anything to build
        if not self._build_products:
            return

        options = self._parms['options']

        # Convert to the user requested output format or leave it in ESPA ENVI
        # We do all of our processing using ESPA ENVI format so it can be
        # hard-coded here
        warp.reformat(self._xml_filename, self._work_dir, 'envi',
                      options['output_format'])

    # -------------------------------------------
    def process_product(self):
        '''
        Description:
            Perform the processor specific processing to generate the request
            product.
        '''

        # Stage the required input data
        self.stage_input_data()

        # Build science products
        self.build_science_products()

        # Remove science products and intermediate data not requested
        self.cleanup_work_dir()

        # Customize products
        self.customize_products()

        # Generate statistics products
        self.generate_statistics()

        # Distribute statistics
        self.distribute_statistics()

        # Reformat product
        self.reformat_products()

        # Package and deliver product
        (destination_product_file, destination_cksum_file) = \
            self.distribute_product()

        return (destination_product_file, destination_cksum_file)


# ===========================================================================
class LandsatProcessor(CDRProcessor):
    '''
    Description:
        Implements the common processing between all of the landsat
        processors.
    '''

    _metadata_filename = None
    _dem_filename = None

    # -------------------------------------------
    def __init__(self, parms):
        super(LandsatProcessor, self).__init__(parms)

        product_id = self._parms['product_id']

        # Setup the dem filename, even though we may not need it
        self._dem_filename = "%s_dem.img" % product_id

    # -------------------------------------------
    def validate_parameters(self):
        '''
        Description:
            Validates the parameters required for the processor.
        '''

        logger = self._logger

        # Call the base class parameter validation
        super(LandsatProcessor, self).validate_parameters()

        logger.info("Validating [LandsatProcessor] parameters")

        options = self._parms['options']

        # Force these parameters to false if not provided
        # They are the required includes for product generation
        required_includes = ['include_cfmask',
                             'include_customized_source_data',
                             'include_dem',
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
                             'include_sr_toa',
                             'include_statistics']

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

        # Determine if we need to build products
        if (not options['include_customized_source_data']
                and not options['include_sr']
                and not options['include_sr_toa']
                and not options['include_sr_thermal']
                and not options['include_sr_browse']
                and not options['include_cfmask']
                and not options['include_sr_nbr']
                and not options['include_sr_nbr2']
                and not options['include_sr_ndvi']
                and not options['include_sr_ndmi']
                and not options['include_sr_savi']
                and not options['include_sr_msavi']
                and not options['include_sr_evi']
                and not options['include_dswe']
                and not options['include_dem']
                and not options['include_solr_index']):

            logger.info("***NO SCIENCE PRODUCTS CHOSEN***")
            self._build_products = False
        else:
            self._build_products = True

    # -------------------------------------------
    def stage_input_data(self):
        '''
        Description:
            Stages the input data required for the processor.
        '''

        product_id = self._parms['product_id']
        download_url = self._parms['download_url']
        options = self._parms['options']

        file_name = ''.join([product_id,
                             settings.LANDSAT_INPUT_FILENAME_EXTENSION])
        destination_file = os.path.join(self._stage_dir, file_name)

        # Download the source data
        try:
            transfer.download_file_url(download_url, destination_file)
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.staging_data, str(e)), \
                None, sys.exc_info()[2]

        # Un-tar the input data to the work directory
        try:
            staging.untar_data(destination_file, self._work_dir)
            os.unlink(destination_file)

            # Figure out the metadata filename
            try:
                self._metadata_filename = \
                    metadata.get_landsat_metadata(self._work_dir, product_id)
            except Exception, e:
                raise ee.ESPAException(ee.ErrorCodes.metadata,
                                       str(e)), None, sys.exc_info()[2]

        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.unpacking, str(e)), \
                None, sys.exc_info()[2]

    # -------------------------------------------
    def convert_to_raw_binary(self):
        '''
        Description:
            Converts the Landsat(LPGS) input data to our internal raw binary
            format.
        '''

        logger = self._logger

        options = self._parms['options']

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
    def dem_command_line(self):
        '''
        Description:
            Returns the command line required to generate the DEM product.
            Evaluates the options requested by the user to define the command
            line string to use, or returns None indicating nothing todo.

        Note:
            Provides the L4, L5, L7, and L8 command line.
        '''

        options = self._parms['options']

        if (options['include_dem']
                or options['include_dswe']):

            cmd = ['do_create_dem.py',
                   '--mtl', self._metadata_filename,
                   '--dem', self._dem_filename]

        # Turn the list into a string
        cmd = ' '.join(cmd)

        return cmd

    # -------------------------------------------
    def generate_dem_product(self):
        '''
        Description:
            Generates a DEM product using the metadata from the input data.
        '''

        logger = self._logger

        cmd = self.dem_command_line()

        logger.info(' '.join(['DEM COMMAND:', cmd]))

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
    def sr_command_line(self):
        '''
        Description:
            Returns the command line required to generate surface reflectance.
            Evaluates the options requested by the user to define the command
            line string to use, or returns None indicating nothing todo.

        Note:
            Provides the L4, L5, and L7 command line.  L8 processing overrides
            this method.
        '''

        options = self._parms['options']

        cmd = ['do_ledaps.py', '--xml', self._xml_filename]

        execute_do_ledaps = False

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
            execute_do_ledaps = True
        else:
            # If we do not need the SR data, then don't waste the time
            # generating it
            cmd.extend(['--process_sr', 'False'])

        # Check to see if Thermal or TOA is required
        # include_sr is added here for sanity to match L8 and business logic
        if (options['include_sr_toa']
                or options['include_sr_thermal']
                or options['include_sr']
                or options['include_dswe']
                or options['include_cfmask']):

            execute_do_ledaps = True

        # Only return a string if we will need to run SR processing
        if not execute_do_ledaps:
            cmd = None
        else:
            cmd = ' '.join(cmd)

        return cmd

    # -------------------------------------------
    def generate_sr_products(self):
        '''
        Description:
            Generates surrface reflectance products.
        '''

        logger = self._logger

        cmd = self.sr_command_line()

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
    def cfmask_command_line(self):
        '''
        Description:
            Returns the command line required to generate cfmask.
            Evaluates the options requested by the user to define the command
            line string to use, or returns None indicating nothing todo.

        Note:
            Provides the L4, L5, and L7 command line.  L8 processing overrides
            this method.
        '''

        options = self._parms['options']

        cmd = None
        if (options['include_cfmask']
                or options['include_dswe']
                or options['include_sr']):
            cmd = ' '.join(['cfmask', '--verbose', '--max_cloud_pixels',
                            settings.CFMASK_MAX_CLOUD_PIXELS,
                            '--xml', self._xml_filename])

        return cmd

    # -------------------------------------------
    def generate_cfmask(self):
        '''
        Description:
            Generates cfmask.
        '''

        logger = self._logger

        cmd = self.cfmask_command_line()

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
    def spectral_indices_command_line(self):
        '''
        Description:
            Returns the command line required to generate spectral indices.
            Evaluates the options requested by the user to define the command
            line string to use, or returns None indicating nothing todo.

        Note:
            Provides the L4, L5, L7, and L8(LC8) command line.
        '''

        options = self._parms['options']

        cmd = None
        if (options['include_sr_nbr']
                or options['include_sr_nbr2']
                or options['include_sr_ndvi']
                or options['include_sr_ndmi']
                or options['include_sr_savi']
                or options['include_sr_msavi']
                or options['include_sr_evi']):

            cmd = ['do_spectral_indices.py', '--xml', self._xml_filename]

            # Add the specified index options
            if options['include_sr_nbr']:
                cmd.append('--nbr')
            if options['include_sr_nbr2']:
                cmd.append('--nbr2')
            if options['include_sr_ndvi']:
                cmd.append('--ndvi')
            if options['include_sr_ndmi']:
                cmd.append('--ndmi')
            if options['include_sr_savi']:
                cmd.append('--savi')
            if options['include_sr_msavi']:
                cmd.append('--msavi')
            if options['include_sr_evi']:
                cmd.append('--evi')

            cmd = ' '.join(cmd)

        return cmd

    # -------------------------------------------
    def generate_spectral_indices(self):
        '''
        Description:
            Generates the requested spectral indices.
        '''

        logger = self._logger

        cmd = self.spectral_indices_command_line()

        # Only if required
        if cmd is not None:

            logger.info(' '.join(['SPECTRAL INDICES COMMAND:', cmd]))

            output = ''
            try:
                output = utilities.execute_cmd(cmd)
            except Exception, e:
                raise ee.ESPAException(ee.ErrorCodes.spectral_indices,
                                       str(e)), None, sys.exc_info()[2]
            finally:
                if len(output) > 0:
                    logger.info(output)

    # -------------------------------------------
    def dswe_command_line(self):
        '''
        Description:
            Returns the command line required to generate Dynamic Surface
            Water Extent.  Evaluates the options requested by the user to
            define the command line string to use, or returns None indicating
            nothing todo.

        Note:
            Provides the L4, L5, L7, and L8(LC8) command line.
        '''

        options = self._parms['options']

        cmd = None
        if options['include_dswe']:

            cmd = ['do_dynamic_surface_water_extent.py',
                   '--xml', self._xml_filename,
                   '--dem', self._dem_filename,
                   '--verbose']

            cmd = ' '.join(cmd)

        return cmd

    # -------------------------------------------
    def generate_dswe(self):
        '''
        Description:
            Generates the Dynamic Surface Water Extent product.
        '''

        logger = self._logger

        cmd = self.dswe_command_line()

        # Only if required
        if cmd is not None:

            logger.info(' '.join(['DSWE COMMAND:', cmd]))

            output = ''
            try:
                output = utilities.execute_cmd(cmd)
            except Exception, e:
                raise ee.ESPAException(ee.ErrorCodes.dswe,
                                       str(e)), None, sys.exc_info()[2]
            finally:
                if len(output) > 0:
                    logger.info(output)

    # -------------------------------------------
    def build_science_products(self):
        '''
        Description:
            Build the science products requested by the user.
        '''

        # Nothing to do if the user did not specify anything to build
        if not self._build_products:
            return

        logger = self._logger

        logger.info("[LandsatProcessor] Building Science Products")

        # Change to the working directory
        current_directory = os.getcwd()
        os.chdir(self._work_dir)

        try:
            self.convert_to_raw_binary()

            self.generate_dem_product()

            self.generate_sr_products()

            self.generate_cfmask()

            # TODO - Today we do not do this anymore so code it back in
            #        if/when it is required
            # self.generate_sr_browse_data()

            self.generate_spectral_indices()

            self.generate_dswe()

        finally:
            # Change back to the previous directory
            os.chdir(current_directory)

    # -------------------------------------------
    def cleanup_work_dir(self):
        '''
        Description:
            Cleanup all the intermediate non-products and the science
            products not requested.
        '''

        logger = self._logger

        options = self._parms['options']

        # Define all of the non-product files that need to be removed before
        # product tarball generation
        non_product_files = [
            'lndsr.*.txt',
            'lndcal.*.txt',
            'LogReport*',
            '*_MTL.txt.old'
        ]

        # Define DEM files that may need to be removed before product tarball
        # generation
        dem_files = [
            '*_dem.*'
        ]

        # Define L1 source files that may need to be removed before product
        # tarball generation
        l1_source_files = [
            'L*.TIF',
            'README.GTF',
            '*gap_mask*'
        ]

        # Define L1 source metadata files that may need to be removed before
        # product tarball generation
        l1_source_metadata_files = [
            '*_MTL*',
            '*_VER*',
            '*_GCP*'
        ]

        # Change to the working directory
        current_directory = os.getcwd()
        os.chdir(self._work_dir)

        try:
            # Remove the intermediate non-product files
            non_products = []
            for item in non_product_files:
                non_products.extend(glob.glob(item))

            # Add DEM files if not requested
            if not options['include_dem']:
                for item in dem_files:
                    non_products.extend(glob.glob(item))

            # Add level 1 source files if not requested
            if not options['include_source_data']:
                for item in l1_source_files:
                    non_products.extend(glob.glob(item))

            # Add metadata files if not requested
            if (not options['include_source_metadata'] and
                    not options['include_source_data']):
                for item in l1_source_metadata_files:
                    non_products.extend(glob.glob(item))

            if len(non_products) > 0:
                cmd = ' '.join(['rm', '-rf'] + non_products)
                logger.info(' '.join(['REMOVING INTERMEDIATE DATA COMMAND:',
                                      cmd]))

                output = ''
                try:
                    output = utilities.execute_cmd(cmd)
                except Exception, e:
                    raise ee.ESPAException(ee.ErrorCodes.cleanup_work_dir,
                                           str(e)), None, sys.exc_info()[2]
                finally:
                    if len(output) > 0:
                        logger.info(output)

            try:
                self.remove_products_from_xml()
            except Exception, e:
                raise ee.ESPAException(ee.ErrorCodes.remove_products,
                                       str(e)), None, sys.exc_info()[2]

        finally:
            # Change back to the previous directory
            os.chdir(current_directory)

    # -------------------------------------------
    def generate_statistics(self):
        '''
        Description:
            Generates statistics if required for the processor.
        '''

        options = self._parms['options']

        # Nothing to do if the user did not specify anything to build
        if not self._build_products or not options['include_statistics']:
            return

        # Generate the stats for each stat'able' science product

        # Hold the wild card strings in a type based dictionary
        files_to_search_for = dict()

        # Landsat files (Includes L4-L8)
        # The types must match the types in settings.py
        files_to_search_for['SR'] = ['*_sr_band[0-9].img']
        files_to_search_for['TOA'] = ['*_toa_band[0-9].img',
                                      '*_toa_band1[0-1].img']
        files_to_search_for['INDEX'] = ['*_nbr.img', '*_nbr2.img',
                                        '*_ndmi.img', '*_ndvi.img',
                                        '*_evi.img', '*_savi.img',
                                        '*_msavi.img']

        # Generate the stats for each file
        statistics.generate_statistics(self._work_dir,
                                       files_to_search_for)

    # -------------------------------------------
    def get_product_name(self):
        '''
        Description:
            Build the product name from the product information and current
            time.
        '''

        if self._product_name is None:
            product_id = self._parms['product_id']

            # Get the current time information
            ts = datetime.datetime.today()

            # Extract stuff from the product information
            sensor_inst = sensor.instance(product_id)

            sensor_code = sensor_inst.sensor_code.upper()
            path = sensor_inst.path
            row = sensor_inst.row
            year = sensor_inst.year
            doy = sensor_inst.doy

            product_name = '%s%s%s%s%s-SC%s%s%s%s%s%s' \
                % (sensor_code, path.zfill(3), row.zfill(3), year.zfill(4),
                   doy.zfill(3), str(ts.year).zfill(4), str(ts.month).zfill(2),
                   str(ts.day).zfill(2), str(ts.hour).zfill(2),
                   str(ts.minute).zfill(2), str(ts.second).zfill(2))

            self._product_name = product_name

        return self._product_name


# ===========================================================================
class LandsatTMProcessor(LandsatProcessor):
    '''
    Description:
        Implements TM specific processing.

    Note:
        Today all processing is inherited from the LandsatProcessors because
        the TM and ETM processors are identical.
    '''

    # -------------------------------------------
    def __init__(self, parms):
        super(LandsatTMProcessor, self).__init__(parms)


# ===========================================================================
class LandsatETMProcessor(LandsatProcessor):
    '''
    Description:
        Implements ETM specific processing.

    Note:
        Today all processing is inherited from the LandsatProcessors because
        the TM and ETM processors are identical.
    '''

    # -------------------------------------------
    def __init__(self, parms):
        super(LandsatETMProcessor, self).__init__(parms)


# ===========================================================================
class LandsatOLITIRSProcessor(LandsatProcessor):
    '''
    Description:
        Implements OLITIRS (LC8) specific processing.
    '''

    # -------------------------------------------
    def __init__(self, parms):
        super(LandsatOLITIRSProcessor, self).__init__(parms)

    # -------------------------------------------
    def sr_command_line(self):
        '''
        Description:
            Returns the command line required to generate surface reflectance.
            Evaluates the options requested by the user to define the command
            line string to use, or returns None indicating nothing todo.
        '''

        options = self._parms['options']

        cmd = ['do_l8_sr.py', '--xml', self._xml_filename]

        execute_do_l8_sr = False

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
            execute_do_l8_sr = True
        else:
            # If we do not need the SR data, then don't waste the time
            # generating it
            cmd.extend(['--process_sr', 'False'])

        # Check to see if Thermal or TOA is required
        # include_sr is added here for business logic
        if (options['include_sr_toa']
                or options['include_sr_thermal']
                or options['include_sr']
                or options['include_cfmask']):

            cmd.append('--write_toa')
            execute_do_l8_sr = True

        # Only return a string if we will need to run SR processing
        if not execute_do_l8_sr:
            cmd = None
        else:
            cmd = ' '.join(cmd)

        return cmd

    # -------------------------------------------
    def cfmask_command_line(self):
        '''
        Description:
            Returns the command line required to generate cfmask.
            Evaluates the options requested by the user to define the command
            line string to use, or returns None indicating nothing todo.
        '''

        options = self._parms['options']

        cmd = None
        if options['include_cfmask'] or options['include_sr']:
            cmd = ' '.join(['l8cfmask', '--verbose', '--max_cloud_pixels',
                            settings.CFMASK_MAX_CLOUD_PIXELS,
                            '--xml', self._xml_filename])

        return cmd


# ===========================================================================
class LandsatOLIProcessor(LandsatOLITIRSProcessor):
    '''
    Description:
        Implements OLI only (LO8) specific processing.
    '''

    # -------------------------------------------
    def __init__(self, parms):
        super(LandsatOLIProcessor, self).__init__(parms)

    # -------------------------------------------
    def cfmask_command_line(self):
        '''
        Description:
            Returns the command line required to generate cfmask.
            Evaluates the options requested by the user to define the command
            line string to use, or returns None indicating nothing todo.

        Note: cfmask processing requires both OLI and TIRS bands so OLI only
              products can not execute l8cfmask.
        '''

        # Return None since we can not process this option.
        return None

    # -------------------------------------------
    def spectral_indices_command_line(self):
        '''
        Description:
            Returns the command line required to generate spectral indices.
            Evaluates the options requested by the user to define the command
            line string to use, or returns None indicating nothing todo.

        Note:
            SR indices can not be produced with OLI only products
        '''

        # Return None since we can not process this option.
        return None


# ===========================================================================
class ModisProcessor(CDRProcessor):

    _hdf_filename = None

    # -------------------------------------------
    def __init__(self, parms):
        super(ModisProcessor, self).__init__(parms)

    # -------------------------------------------
    def validate_parameters(self):
        '''
        Description:
            Validates the parameters required for the processor.
        '''

        logger = self._logger

        # Call the base class parameter validation
        super(ModisProcessor, self).validate_parameters()

        logger.info("Validating [ModisProcessor] parameters")

        options = self._parms['options']

        # Force these parameters to false if not provided
        # They are the required includes for product generation
        required_includes = ['include_customized_source_data',
                             'include_source_data',
                             'include_statistics']

        for parameter in required_includes:
            if not parameters.test_for_parameter(options, parameter):
                logger.warning("'%s' parameter missing defaulting to False"
                               % parameter)
                options[parameter] = False

        # Determine if we need to build products
        if (not options['include_customized_source_data']):

            logger.info("***NO CUSTOMIZED PRODUCTS CHOSEN***")
            self._build_products = False
        else:
            self._build_products = True

    # -------------------------------------------
    def stage_input_data(self):
        '''
        Description:
            Stages the input data required for the processor.
        '''

        product_id = self._parms['product_id']
        download_url = self._parms['download_url']

        file_name = ''.join([product_id,
                             settings.MODIS_INPUT_FILENAME_EXTENSION])
        destination_file = os.path.join(self._stage_dir, file_name)

        # Download the source data
        try:
            transfer.download_file_url(download_url, destination_file)
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.staging_data, str(e)), \
                None, sys.exc_info()[2]

        self._hdf_filename = os.path.basename(destination_file)

        # Copy the staged data to the work directory
        try:
            transfer.copy_file_to_file(destination_file, self._work_dir)
            os.unlink(destination_file)
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.unpacking, str(e)), \
                None, sys.exc_info()[2]

    # -------------------------------------------
    def convert_to_raw_binary(self):
        '''
        Description:
            Converts the Landsat(LPGS) input data to our internal raw binary
            format.
        '''

        logger = self._logger

        options = self._parms['options']

        # Build a command line arguments list
        cmd = ['convert_modis_to_espa',
               '--hdf', self._hdf_filename,
               '--xml', self._xml_filename]
        if not options['include_source_data']:
            cmd.append('--del_src_files')

        # Turn the list into a string
        cmd = ' '.join(cmd)
        logger.info(' '.join(['CONVERT MODIS TO ESPA COMMAND:', cmd]))

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
    def build_science_products(self):
        '''
        Description:
            Build the science products requested by the user.

        Note:
            We get science products as the input, so the only thing really
            happening here is generating a customized product for the
            statistics generation.
        '''

        # Nothing to do if the user did not specify anything to build
        if not self._build_products:
            return

        logger = self._logger

        logger.info("[ModisProcessor] Building Science Products")

        # Change to the working directory
        current_directory = os.getcwd()
        os.chdir(self._work_dir)

        try:
            self.convert_to_raw_binary()

        finally:
            # Change back to the previous directory
            os.chdir(current_directory)

    # -------------------------------------------
    def cleanup_work_dir(self):
        '''
        Description:
            Cleanup all the intermediate non-products and the science
            products not requested.
        '''

        # Nothing to do for Modis products
        return

    # -------------------------------------------
    def generate_statistics(self):
        '''
        Description:
            Generates statistics if required for the processor.
        '''

        options = self._parms['options']

        # Nothing to do if the user did not specify anything to build
        if not self._build_products or not options['include_statistics']:
            return

        # Generate the stats for each stat'able' science product

        # Hold the wild card strings in a type based dictionary
        files_to_search_for = dict()

        # MODIS files
        # The types must match the types in settings.py
        files_to_search_for['SR'] = ['*sur_refl_b*.img']
        files_to_search_for['INDEX'] = ['*NDVI.img', '*EVI.img']
        files_to_search_for['LST'] = ['*LST_Day_1km.img',
                                      '*LST_Night_1km.img',
                                      '*LST_Day_6km.img',
                                      '*LST_Night_6km.img']
        files_to_search_for['EMIS'] = ['*Emis_*.img']

        # Generate the stats for each file
        statistics.generate_statistics(self._work_dir,
                                       files_to_search_for)

    # -------------------------------------------
    def get_product_name(self):
        '''
        Description:
            Build the product name from the product information and current
            time.
        '''

        if self._product_name is None:
            product_id = self._parms['product_id']

            # Get the current time information
            ts = datetime.datetime.today()

            # Extract stuff from the product information
            sensor_inst = sensor.instance(product_id)

            short_name = sensor_inst.short_name.upper()
            horizontal = sensor_inst.horizontal
            vertical = sensor_inst.vertical
            year = sensor_inst.year
            doy = sensor_inst.doy

            product_name = '%sh%sv%s%s%s-SC%s%s%s%s%s%s' \
                % (short_name, horizontal.zfill(2), vertical.zfill(2),
                   year.zfill(4), doy.zfill(3), str(ts.year).zfill(4),
                   str(ts.month).zfill(2), str(ts.day).zfill(2),
                   str(ts.hour).zfill(2), str(ts.minute).zfill(2),
                   str(ts.second).zfill(2))

            self._product_name = product_name

        return self._product_name


# ===========================================================================
class ModisAQUAProcessor(ModisProcessor):
    '''
    Description:
        Implements AQUA specific processing.
    '''

    # -------------------------------------------
    def __init__(self, parms):
        super(ModisAQUAProcessor, self).__init__(parms)


# ===========================================================================
class ModisTERRAProcessor(ModisProcessor):
    '''
    Description:
        Implements TERRA specific processing.
    '''

    # -------------------------------------------
    def __init__(self, parms):
        super(ModisTERRAProcessor, self).__init__(parms)


# ===========================================================================
class PlotProcessor(ProductProcessor):
    '''
    Description:
        Implements Plot processing.
    '''

    _sensor_colors = None
    _bg_color = None
    _marker = None
    _marker_size = None
    _marker_edge_width = None

    _time_delta_5_days = None

    _band_type_data_ranges = None

    _sr_swir_modis_b5_sensor_info = None
    _sr_swir1_sensor_info = None
    _sr_swir2_sensor_info = None
    _sr_coastal_sensor_info = None
    _sr_blue_sensor_info = None
    _sr_green_sensor_info = None
    _sr_red_sensor_info = None
    _sr_nir_sensor_info = None
    _sr_cirrus_sensor_info = None

    _toa_thermal_sensor_info = None
    _toa_swir1_sensor_info = None
    _toa_swir2_sensor_info = None
    _toa_coastal_sensor_info = None
    _toa_blue_sensor_info = None
    _toa_green_sensor_info = None
    _toa_red_sensor_info = None
    _toa_nir_sensor_info = None
    _toa_cirrus_sensor_info = None

    _emis_20_sensor_info = None
    _emis_22_sensor_info = None
    _emis_23_sensor_info = None
    _emis_29_sensor_info = None
    _emis_31_sensor_info = None
    _emis_32_sensor_info = None

    _lst_day_sensor_info = None
    _lst_night_sensor_info = None

    _ndvi_sensor_info = None
    _evi_sensor_info = None
    _savi_sensor_info = None
    _msavi_sensor_info = None
    _nbr_sensor_info = None
    _nbr2_sensor_info = None
    _ndmi_sensor_info = None

    def __init__(self, parms):

        # Setup the default colors
        self._sensor_colors = dict()
        self._sensor_colors['Terra'] = '#664400'   # Some Brown kinda like dirt
        self._sensor_colors['Aqua'] = '#00cccc'    # Some cyan like blue color
        self._sensor_colors['L4'] = '#cc3333'     # A nice Red
        self._sensor_colors['L5'] = '#0066cc'     # A nice Blue
        self._sensor_colors['L7'] = '#00cc33'     # An ok Green
        self._sensor_colors['L8'] = '#ffbb00'      # An ok Yellow
        self._sensor_colors['L8-TIRS1'] = '#ffbb00'  # An ok Yellow
        self._sensor_colors['L8-TIRS2'] = '#664400'  # Some Brown like dirt
        self._bg_color = settings.PLOT_BG_COLOR

        # Setup the default marker
        self._marker = settings.PLOT_MARKER
        self._marker_size = float(settings.PLOT_MARKER_SIZE)
        self._marker_edge_width = float(settings.PLOT_MARKER_EDGE_WIDTH)

        # Specify a base number of days to expand the plot date range. This
        # helps keep data points from being placed on the plot border lines
        self._time_delta_5_days = datetime.timedelta(days=5)

        # --------------------------------------------------------------------
        # Define the data ranges and output ranges for the plotting
        # DATA_(MAX/MIN) must match the (UPPER/LOWER)_BOUND in settings.py
        # The toplevel keys are used as search strings into the band_type
        # displayed names, so they need to match unique(enough) portions of
        # those strings
        # --------------------------------------------------------------------
        #          DATA_MAX: The maximum value represented in the data.
        #          DATA_MIN: The minimum value represented in the data.
        #         SCALE_MAX: The DATA_MAX is scaled to this value.
        #         SCALE_MIN: The DATA_MIN is scaled to this value.
        #       DISPLAY_MAX: The maximum value to display on the plot.
        #       DISPLAY_MIN: The minimum value to display on the plot.
        #    MAX_N_LOCATORS: The maximum number of spaces between Y-axis tick
        #                    marks.  This should be adjusted so that the tick
        #                    marks fall on values that display nicely.  Due to
        #                    having some buffer added to the display minimum
        #                    and maximum values, the value chosen for this
        #                    parameter should include the space between the
        #                    top and the top tick mark as well as the bottom
        #                    and bottom tick mark. (i.e. Add two)
        # --------------------------------------------------------------------
        br_sr = settings.BAND_TYPE_STAT_RANGES['SR']
        br_toa = settings.BAND_TYPE_STAT_RANGES['TOA']
        br_index = settings.BAND_TYPE_STAT_RANGES['INDEX']
        br_lst = settings.BAND_TYPE_STAT_RANGES['LST']
        br_emis = settings.BAND_TYPE_STAT_RANGES['EMIS']
        self._band_type_data_ranges = {
            'SR': {
                'DATA_MAX': float(br_sr['UPPER_BOUND']),
                'DATA_MIN': float(br_sr['LOWER_BOUND']),
                'SCALE_MAX': 1.0,
                'SCALE_MIN': 0.0,
                'DISPLAY_MAX': 1.0,
                'DISPLAY_MIN': 0.0,
                'MAX_N_LOCATORS': 12
            },
            'TOA': {
                'DATA_MAX': float(br_toa['UPPER_BOUND']),
                'DATA_MIN': float(br_toa['LOWER_BOUND']),
                'SCALE_MAX': 1.0,
                'SCALE_MIN': 0.0,
                'DISPLAY_MAX': 1.0,
                'DISPLAY_MIN': 0.0,
                'MAX_N_LOCATORS': 12
            },
            'NDVI': {
                'DATA_MAX': float(br_index['UPPER_BOUND']),
                'DATA_MIN': float(br_index['LOWER_BOUND']),
                'SCALE_MAX': 1.0,
                'SCALE_MIN': -0.1,
                'DISPLAY_MAX': 1.0,
                'DISPLAY_MIN': -0.1,
                'MAX_N_LOCATORS': 13
            },
            'EVI': {
                'DATA_MAX': float(br_index['UPPER_BOUND']),
                'DATA_MIN': float(br_index['LOWER_BOUND']),
                'SCALE_MAX': 1.0,
                'SCALE_MIN': -0.1,
                'DISPLAY_MAX': 1.0,
                'DISPLAY_MIN': -0.1,
                'MAX_N_LOCATORS': 13
            },
            'SAVI': {
                'DATA_MAX': float(br_index['UPPER_BOUND']),
                'DATA_MIN': float(br_index['LOWER_BOUND']),
                'SCALE_MAX': 1.0,
                'SCALE_MIN': -0.1,
                'DISPLAY_MAX': 1.0,
                'DISPLAY_MIN': -0.1,
                'MAX_N_LOCATORS': 13
            },
            'MSAVI': {
                'DATA_MAX': float(br_index['UPPER_BOUND']),
                'DATA_MIN': float(br_index['LOWER_BOUND']),
                'SCALE_MAX': 1.0,
                'SCALE_MIN': -0.1,
                'DISPLAY_MAX': 1.0,
                'DISPLAY_MIN': -0.1,
                'MAX_N_LOCATORS': 13
            },
            'NBR': {
                'DATA_MAX': float(br_index['UPPER_BOUND']),
                'DATA_MIN': float(br_index['LOWER_BOUND']),
                'SCALE_MAX': 1.0,
                'SCALE_MIN': -0.1,
                'DISPLAY_MAX': 1.0,
                'DISPLAY_MIN': -0.1,
                'MAX_N_LOCATORS': 13
            },
            'NBR2': {
                'DATA_MAX': float(br_index['UPPER_BOUND']),
                'DATA_MIN': float(br_index['LOWER_BOUND']),
                'SCALE_MAX': 1.0,
                'SCALE_MIN': -0.1,
                'DISPLAY_MAX': 1.0,
                'DISPLAY_MIN': -0.1,
                'MAX_N_LOCATORS': 13
            },
            'NDMI': {
                'DATA_MAX': float(br_index['UPPER_BOUND']),
                'DATA_MIN': float(br_index['LOWER_BOUND']),
                'SCALE_MAX': 1.0,
                'SCALE_MIN': -0.1,
                'DISPLAY_MAX': 1.0,
                'DISPLAY_MIN': -0.1,
                'MAX_N_LOCATORS': 13
            },
            'LST': {
                'DATA_MAX': float(br_lst['UPPER_BOUND']),
                'DATA_MIN': float(br_lst['LOWER_BOUND']),
                'SCALE_MAX': 1.0,
                'SCALE_MIN': 0.0,
                'DISPLAY_MAX': 1.0,
                'DISPLAY_MIN': 0.0,
                'MAX_N_LOCATORS': 12
            },
            'Emis': {
                'DATA_MAX': float(br_emis['UPPER_BOUND']),
                'DATA_MIN': float(br_emis['LOWER_BOUND']),
                'SCALE_MAX': 1.0,
                'SCALE_MIN': 0.0,
                'DISPLAY_MAX': 1.0,
                'DISPLAY_MIN': 0.0,
                'MAX_N_LOCATORS': 12
            }
        }

        # --------------------------------------------------------------------
        # Define the configuration for searching for files and some of the
        # text for the plots and filenames.
        # Doing this greatly simplified the code. :)
        # Should be real easy to add others. :)
        # --------------------------------------------------------------------

        L4_NAME = 'Landsat 4'
        L5_NAME = 'Landsat 5'
        L7_NAME = 'Landsat 7'
        L8_NAME = 'Landsat 8'
        L8_TIRS1_NAME = 'Landsat 8 TIRS1'
        L8_TIRS2_NAME = 'Landsat 8 TIRS2'
        TERRA_NAME = 'Terra'
        AQUA_NAME = 'Aqua'

        # --------------------------------------------------------------------
        # Only MODIS SR band 5 files
        self._sr_swir_modis_b5_sensor_info = \
            [('MOD*sur_refl*b05.stats', TERRA_NAME),
             ('MYD*sur_refl*b05.stats', AQUA_NAME)]

        # --------------------------------------------------------------------
        # SR (L4-L7 B5) (L8 B6) (MODIS B6)
        self._sr_swir1_sensor_info = [('LT4*_sr_band5.stats', L4_NAME),
                                      ('LT5*_sr_band5.stats', L5_NAME),
                                      ('LE7*_sr_band5.stats', L7_NAME),
                                      ('LC8*_sr_band6.stats', L8_NAME),
                                      ('MOD*sur_refl_b06*.stats', TERRA_NAME),
                                      ('MYD*sur_refl_b06*.stats', AQUA_NAME)]

        # SR (L4-L8 B7) (MODIS B7)
        self._sr_swir2_sensor_info = [('LT4*_sr_band7.stats', L4_NAME),
                                      ('LT5*_sr_band7.stats', L5_NAME),
                                      ('LE7*_sr_band7.stats', L7_NAME),
                                      ('LC8*_sr_band7.stats', L8_NAME),
                                      ('MOD*sur_refl_b07*.stats', TERRA_NAME),
                                      ('MYD*sur_refl_b07*.stats', AQUA_NAME)]

        # SR (L8 B1)  coastal aerosol
        self._sr_coastal_sensor_info = [('LC8*_sr_band1.stats', L8_NAME)]

        # SR (L4-L7 B1) (L8 B2) (MODIS B3)
        self._sr_blue_sensor_info = [('LT4*_sr_band1.stats', L4_NAME),
                                     ('LT5*_sr_band1.stats', L5_NAME),
                                     ('LE7*_sr_band1.stats', L7_NAME),
                                     ('LC8*_sr_band2.stats', L8_NAME),
                                     ('MOD*sur_refl_b03*.stats', TERRA_NAME),
                                     ('MYD*sur_refl_b03*.stats', AQUA_NAME)]

        # SR (L4-L7 B2) (L8 B3) (MODIS B4)
        self._sr_green_sensor_info = [('LT4*_sr_band2.stats', L4_NAME),
                                      ('LT5*_sr_band2.stats', L5_NAME),
                                      ('LE7*_sr_band2.stats', L7_NAME),
                                      ('LC8*_sr_band3.stats', L8_NAME),
                                      ('MOD*sur_refl_b04*.stats', TERRA_NAME),
                                      ('MYD*sur_refl_b04*.stats', AQUA_NAME)]

        # SR (L4-L7 B3) (L8 B4) (MODIS B1)
        self._sr_red_sensor_info = [('LT4*_sr_band3.stats', L4_NAME),
                                    ('LT5*_sr_band3.stats', L5_NAME),
                                    ('LE7*_sr_band3.stats', L7_NAME),
                                    ('LC8*_sr_band4.stats', L8_NAME),
                                    ('MOD*sur_refl_b01*.stats', TERRA_NAME),
                                    ('MYD*sur_refl_b01*.stats', AQUA_NAME)]

        # SR (L4-L7 B4) (L8 B5) (MODIS B2)
        self._sr_nir_sensor_info = [('LT4*_sr_band4.stats', L4_NAME),
                                    ('LT5*_sr_band4.stats', L5_NAME),
                                    ('LE7*_sr_band4.stats', L7_NAME),
                                    ('LC8*_sr_band5.stats', L8_NAME),
                                    ('MOD*sur_refl_b02*.stats', TERRA_NAME),
                                    ('MYD*sur_refl_b02*.stats', AQUA_NAME)]

        # SR (L8 B9)
        self._sr_cirrus_sensor_info = [('LC8*_sr_band9.stats', L8_NAME)]

        # --------------------------------------------------------------------
        # Only Landsat TOA band 6(L4-7) band 10(L8) band 11(L8)
        self._toa_thermal_sensor_info = \
            [('LT4*_toa_band6.stats', L4_NAME),
             ('LT5*_toa_band6.stats', L5_NAME),
             ('LE7*_toa_band6.stats', L7_NAME),
             ('LC8*_toa_band10.stats', L8_TIRS1_NAME),
             ('LC8*_toa_band11.stats', L8_TIRS2_NAME)]

        # --------------------------------------------------------------------
        # Only Landsat TOA (L4-L7 B5) (L8 B6)
        self._toa_swir1_sensor_info = [('LT4*_toa_band5.stats', L4_NAME),
                                       ('LT5*_toa_band5.stats', L5_NAME),
                                       ('LE7*_toa_band5.stats', L7_NAME),
                                       ('L[C,O]8*_toa_band6.stats', L8_NAME)]

        # Only Landsat TOA (L4-L8 B7)
        self._toa_swir2_sensor_info = [('LT4*_toa_band7.stats', L4_NAME),
                                       ('LT5*_toa_band7.stats', L5_NAME),
                                       ('LE7*_toa_band7.stats', L7_NAME),
                                       ('L[C,O]8*_toa_band7.stats', L8_NAME)]

        # Only Landsat TOA (L8 B1)
        self._toa_coastal_sensor_info = [('L[C,O]8*_toa_band1.stats', L8_NAME)]

        # Only Landsat TOA (L4-L7 B1) (L8 B2)
        self._toa_blue_sensor_info = [('LT4*_toa_band1.stats', L4_NAME),
                                      ('LT5*_toa_band1.stats', L5_NAME),
                                      ('LE7*_toa_band1.stats', L7_NAME),
                                      ('L[C,O]8*_toa_band2.stats', L8_NAME)]

        # Only Landsat TOA (L4-L7 B2) (L8 B3)
        self._toa_green_sensor_info = [('LT4*_toa_band2.stats', L4_NAME),
                                       ('LT5*_toa_band2.stats', L5_NAME),
                                       ('LE7*_toa_band2.stats', L7_NAME),
                                       ('L[C,O]8*_toa_band3.stats', L8_NAME)]

        # Only Landsat TOA (L4-L7 B3) (L8 B4)
        self._toa_red_sensor_info = [('LT4*_toa_band3.stats', L4_NAME),
                                     ('LT5*_toa_band3.stats', L5_NAME),
                                     ('LE7*_toa_band3.stats', L7_NAME),
                                     ('L[C,O]8*_toa_band4.stats', L8_NAME)]

        # Only Landsat TOA (L4-L7 B4) (L8 B5)
        self._toa_nir_sensor_info = [('LT4*_toa_band4.stats', L4_NAME),
                                     ('LT5*_toa_band4.stats', L5_NAME),
                                     ('LE7*_toa_band4.stats', L7_NAME),
                                     ('L[C,O]8*_toa_band5.stats', L8_NAME)]

        # Only Landsat TOA (L8 B9)
        self._toa_cirrus_sensor_info = [('L[C,O]8*_toa_band9.stats', L8_NAME)]

        # --------------------------------------------------------------------
        # Only MODIS band 20 files
        self._emis_20_sensor_info = [('MOD*Emis_20.stats', TERRA_NAME),
                                     ('MYD*Emis_20.stats', AQUA_NAME)]

        # Only MODIS band 22 files
        self._emis_22_sensor_info = [('MOD*Emis_22.stats', TERRA_NAME),
                                     ('MYD*Emis_22.stats', AQUA_NAME)]

        # Only MODIS band 23 files
        self._emis_23_sensor_info = [('MOD*Emis_23.stats', TERRA_NAME),
                                     ('MYD*Emis_23.stats', AQUA_NAME)]

        # Only MODIS band 29 files
        self._emis_29_sensor_info = [('MOD*Emis_29.stats', TERRA_NAME),
                                     ('MYD*Emis_29.stats', AQUA_NAME)]

        # Only MODIS band 31 files
        self._emis_31_sensor_info = [('MOD*Emis_31.stats', TERRA_NAME),
                                     ('MYD*Emis_31.stats', AQUA_NAME)]

        # Only MODIS band 32 files
        self._emis_32_sensor_info = [('MOD*Emis_32.stats', TERRA_NAME),
                                     ('MYD*Emis_32.stats', AQUA_NAME)]

        # --------------------------------------------------------------------
        # Only MODIS Day files
        self._lst_day_sensor_info = [('MOD*LST_Day_*.stats', TERRA_NAME),
                                     ('MYD*LST_Day_*.stats', AQUA_NAME)]

        # Only MODIS Night files
        self._lst_night_sensor_info = [('MOD*LST_Night_*.stats', TERRA_NAME),
                                       ('MYD*LST_Night_*.stats', AQUA_NAME)]

        # --------------------------------------------------------------------
        # MODIS and Landsat files
        self._ndvi_sensor_info = [('LT4*_sr_ndvi.stats', L4_NAME),
                                  ('LT5*_sr_ndvi.stats', L5_NAME),
                                  ('LE7*_sr_ndvi.stats', L7_NAME),
                                  ('LC8*_sr_ndvi.stats', L8_NAME),
                                  ('MOD*_NDVI.stats', TERRA_NAME),
                                  ('MYD*_NDVI.stats', AQUA_NAME)]

        # --------------------------------------------------------------------
        # MODIS and Landsat files
        self._evi_sensor_info = [('LT4*_sr_evi.stats', L4_NAME),
                                 ('LT5*_sr_evi.stats', L5_NAME),
                                 ('LE7*_sr_evi.stats', L7_NAME),
                                 ('LC8*_sr_evi.stats', L8_NAME),
                                 ('MOD*_EVI.stats', TERRA_NAME),
                                 ('MYD*_EVI.stats', AQUA_NAME)]

        # --------------------------------------------------------------------
        # Only Landsat SAVI files
        self._savi_sensor_info = [('LT4*_sr_savi.stats', L4_NAME),
                                  ('LT5*_sr_savi.stats', L5_NAME),
                                  ('LE7*_sr_savi.stats', L7_NAME),
                                  ('LC8*_sr_savi.stats', L8_NAME)]

        # --------------------------------------------------------------------
        # Only Landsat MSAVI files
        self._msavi_sensor_info = [('LT4*_sr_msavi.stats', L4_NAME),
                                   ('LT5*_sr_msavi.stats', L5_NAME),
                                   ('LE7*_sr_msavi.stats', L7_NAME),
                                   ('LC8*_sr_msavi.stats', L8_NAME)]

        # --------------------------------------------------------------------
        # Only Landsat NBR files
        self._nbr_sensor_info = [('LT4*_sr_nbr.stats', L4_NAME),
                                 ('LT5*_sr_nbr.stats', L5_NAME),
                                 ('LE7*_sr_nbr.stats', L7_NAME),
                                 ('LC8*_sr_nbr.stats', L8_NAME)]

        # --------------------------------------------------------------------
        # Only Landsat NBR2 files
        self._nbr2_sensor_info = [('LT4*_sr_nbr2.stats', L4_NAME),
                                  ('LT5*_sr_nbr2.stats', L5_NAME),
                                  ('LE7*_sr_nbr2.stats', L7_NAME),
                                  ('LC8*_sr_nbr2.stats', L8_NAME)]

        # --------------------------------------------------------------------
        # Only Landsat NDMI files
        self._ndmi_sensor_info = [('LT4*_sr_ndmi.stats', L4_NAME),
                                  ('LT5*_sr_ndmi.stats', L5_NAME),
                                  ('LE7*_sr_ndmi.stats', L7_NAME),
                                  ('LC8*_sr_ndmi.stats', L8_NAME)]

        super(PlotProcessor, self).__init__(parms)

    # -------------------------------------------
    def get_statistics_hostname(self):
        '''
        Description:
            Returns the hostname to use for retrieving the input data.
        '''

        return utilities.get_cache_hostname()

    # -------------------------------------------
    def get_statistics_directory(self):
        '''
        Description:
            Returns the source directory to use for retrieving the input data.
        '''

        order_id = self._parms['orderid']

        return os.path.join(settings.ESPA_CACHE_DIRECTORY, order_id)

    # -------------------------------------------
    def validate_parameters(self):
        '''
        Description:
            Validates the parameters required for the processor.
        '''

        logger = self._logger

        # Call the base class parameter validation
        super(PlotProcessor, self).validate_parameters()

        logger.info("Validating [PlotProcessor] parameters")

        options = self._parms['options']

        # Statistics input location information
        if not parameters.test_for_parameter(options, 'statistics_host'):
            options['statistics_host'] = self.get_statistics_hostname()

        if not parameters.test_for_parameter(options, 'statistics_directory'):
            options['statistics_directory'] = self.get_statistics_directory()

        # Override the colors if they were specified
        if parameters.test_for_parameter(options, 'terra_color'):
            self._sensor_colors['Terra'] = options['terra_color']
        else:
            options['terra_color'] = self._sensor_colors['Terra']
        if parameters.test_for_parameter(options, 'aqua_color'):
            self._sensor_colors['Aqua'] = options['aqua_color']
        else:
            options['aqua_color'] = self._sensor_colors['Aqua']
        if parameters.test_for_parameter(options, 'l4_color'):
            self._sensor_colors['L4'] = options['l4_color']
        else:
            options['l4_color'] = self._sensor_colors['L4']
        if parameters.test_for_parameter(options, 'l5_color'):
            self._sensor_colors['L5'] = options['l5_color']
        else:
            options['l5_color'] = self._sensor_colors['L5']
        if parameters.test_for_parameter(options, 'l7_color'):
            self._sensor_colors['L7'] = options['l7_color']
        else:
            options['l7_color'] = self._sensor_colors['L7']
        if parameters.test_for_parameter(options, 'l8_color'):
            self._sensor_colors['L8'] = options['l8_color']
        else:
            options['l8_color'] = self._sensor_colors['L8']
        if parameters.test_for_parameter(options, 'l8_tirs1_color'):
            self._sensor_colors['L8-TIRS1'] = options['l8_tirs1_color']
        else:
            options['l8_tirs1_color'] = self._sensor_colors['L8-TIRS1']
        if parameters.test_for_parameter(options, 'l8_tirs2_color'):
            self._sensor_colors['L8-TIRS2'] = options['l8_tirs2_color']
        else:
            options['l8_tirs2_color'] = self._sensor_colors['L8-TIRS2']
        if parameters.test_for_parameter(options, 'bg_color'):
            self._bg_color = options['bg_color']
        else:
            options['bg_color'] = self._bg_color

        # Override the marker if it was specified
        if parameters.test_for_parameter(options, 'marker'):
            self._marker = options['marker']
        else:
            options['marker'] = self._marker
        if parameters.test_for_parameter(options, 'marker_size'):
            self._marker_size = options['marker_size']
        else:
            options['marker_size'] = self._marker_size
        if parameters.test_for_parameter(options, 'marker_edge_width'):
            self._marker_edge_width = options['marker_edge_width']
        else:
            options['marker_edge_width'] = self._marker_edge_width

    # -------------------------------------------
    def read_statistics(self, statistics_file):
        '''
        Description:
          Read the file contents and return as a list of key values.
        '''

        found_valid = False
        with open(statistics_file, 'r') as statistics_fd:
            for line in statistics_fd:
                line_lower = line.strip().lower()
                parts = line_lower.split('=')
                # Some files may not contain the field so detect that
                # TODO - This can be removed after version 2.6.1
                if parts[0] == 'valid':
                    found_valid = True
                yield(parts)

        # Some files may not contain the field so report valid for them
        # TODO - This can be removed after version 2.6.1
        if not found_valid:
            yield(['valid', 'yes'])

    # -------------------------------------------
    def get_ymds_from_filename(self, filename):
        '''
        Description:
          Determine the year, month, day_of_month, and sensor from the
          scene name.
        '''

        year = 0
        sensor = 'unk'

        if filename.startswith('MOD'):
            date_element = filename.split('.')[1]
            year = int(date_element[1:5])
            day_of_year = int(date_element[5:8])
            sensor = 'Terra'

        elif filename.startswith('MYD'):
            date_element = filename.split('.')[1]
            year = int(date_element[1:5])
            day_of_year = int(date_element[5:8])
            sensor = 'Aqua'

        elif filename.startswith('LT4'):
            year = int(filename[9:13])
            day_of_year = int(filename[13:16])
            sensor = 'L4'

        elif filename.startswith('LT5'):
            year = int(filename[9:13])
            day_of_year = int(filename[13:16])
            sensor = 'L5'

        elif filename.startswith('LE7'):
            year = int(filename[9:13])
            day_of_year = int(filename[13:16])
            sensor = 'L7'

        elif filename.startswith('LC8') or filename.startswith('LO8'):
            year = int(filename[9:13])
            day_of_year = int(filename[13:16])
            # We plot both TIRS bands in the thermal plot so they need to
            # be separatly identified
            if 'toa_band10' in filename:
                sensor = 'L8-TIRS1'
            elif 'toa_band11' in filename:
                sensor = 'L8-TIRS2'
            else:
                sensor = 'L8'

        # Now that we have the year and doy we can get the month and day of
        # month
        date = utilities.date_from_doy(year, day_of_year)

        return (year, date.month, date.day, day_of_year, sensor)

    # -------------------------------------------
    def combine_sensor_stats(self, stats_name, stats_files):
        '''
        Description:
          Combines all the stat files for one sensor into one csv file.
        '''

        logger = self._logger

        stats = dict()

        # Fix the output filename
        out_filename = stats_name.replace(' ', '_').lower()
        out_filename = ''.join([out_filename, '_stats.csv'])

        # Read each file into a dictionary
        for stats_file in stats_files:
            stats[stats_file] = \
                dict((key, value) for (key, value)
                     in self.read_statistics(stats_file))

        stat_data = list()
        # Process through and create records
        for filename, obj in stats.items():
            logger.debug(filename)
            # Figure out the date for stats record
            (year, month, day_of_month, day_of_year, sensor) = \
                self.get_ymds_from_filename(filename)
            date = ('%04d-%02d-%02d'
                    % (int(year), int(month), int(day_of_month)))
            logger.debug(date)

            line = ','.join([date, '%03d' % day_of_year,
                             obj['minimum'], obj['maximum'],
                             obj['mean'], obj['stddev'], obj['valid']])
            logger.debug(line)

            stat_data.append(line)

        # Create an empty string buffer to hold the output
        temp_buffer = StringIO()

        # Write the file header
        temp_buffer.write('DATE,DOY,MINIMUM,MAXIMUM,MEAN,STDDEV,VALID')

        # Sort the stats into the buffer
        for line in sorted(stat_data):
            temp_buffer.write('\n')
            temp_buffer.write(line)

        # Flush and save the buffer as a string
        temp_buffer.flush()
        data = temp_buffer.getvalue()
        temp_buffer.close()

        # Create the output file
        with open(out_filename, 'w') as output_fd:
            output_fd.write(data)

    # -------------------------------------------
    def scale_data_to_range(self, in_high, in_low, out_high, out_low, data):
        '''
        Description:
          Scale the values in the data array to the specified output range.
        '''

        # Figure out the ranges
        in_range = in_high - in_low
        out_range = out_high - out_low

        return (out_high - ((out_range * (in_high - data)) / in_range))

    # -------------------------------------------
    def generate_plot(self, plot_name, subjects, band_type, stats,
                      plot_type="Value"):
        '''
        Description:
          Builds a plot and then generates a png formatted image of the plot.
        '''

        logger = self._logger

        # Test for a valid plot_type parameter
        # For us 'Range' mean min, max, and mean
        if plot_type not in ('Range', 'Value'):
            error = ("Error plot_type='%s' must be one of ('Range', 'Value')"
                     % plot_type)
            raise ValueError(error)

        # Create the subplot objects
        fig = mpl_plot.figure()

        # Adjust the figure size
        fig.set_size_inches(11, 8.5)

        min_plot = mpl_plot.subplot(111, axisbg=self._bg_color)

        # Determine which ranges to use for scaling the data before plotting
        use_data_range = ''
        for range_type in self._band_type_data_ranges:
            if band_type.startswith(range_type):
                use_data_range = range_type
                break
        logger.info("Using use_data_range [%s] for band_type [%s]"
                    % (use_data_range, band_type))

        # Make sure the band_type has been coded (help the developer)
        if use_data_range == '':
            raise ValueError("Error unable to determine 'use_data_range'")

        data_max = self._band_type_data_ranges[use_data_range]['DATA_MAX']
        data_min = self._band_type_data_ranges[use_data_range]['DATA_MIN']
        scale_max = self._band_type_data_ranges[use_data_range]['SCALE_MAX']
        scale_min = self._band_type_data_ranges[use_data_range]['SCALE_MIN']
        display_max = \
            self._band_type_data_ranges[use_data_range]['DISPLAY_MAX']
        display_min = \
            self._band_type_data_ranges[use_data_range]['DISPLAY_MIN']
        max_n_locators = \
            self._band_type_data_ranges[use_data_range]['MAX_N_LOCATORS']

        # --------------------------------------------------------------------
        # Build a dictionary of sensors which contains lists of the values,
        # while determining the minimum and maximum values to be displayed

        # I won't be here to resolve this
        plot_date_min = datetime.date(9998, 12, 31)
        # Doubt if we have any this old
        plot_date_max = datetime.date(1900, 01, 01)

        sensor_dict = defaultdict(list)

        if plot_type == "Range":
            lower_subject = 'mean'  # Since Range force to the mean
        else:
            lower_subject = subjects[0].lower()

        # Convert the list of stats read from the file into a list of stats
        # organized by the sensor and contains a python date element
        for filename, obj in stats.items():
            logger.debug(filename)
            # Figure out the date for plotting
            (year, month, day_of_month, day_of_year, sensor) = \
                self.get_ymds_from_filename(filename)
            # day_of_year isn't used, but need a var because it is returned

            date = datetime.date(year, month, day_of_month)
            min_value = float(obj['minimum'])
            max_value = float(obj['maximum'])
            mean = float(obj['mean'])
            stddev = float(obj['stddev'])

            # Date must be first in the list for later sorting to work
            sensor_dict[sensor].append((date, min_value, max_value, mean,
                                        stddev))

            # While we are here figure out...
            # The min and max range for the X-Axis value
            if date < plot_date_min:
                plot_date_min = date
            if date > plot_date_max:
                plot_date_max = date
        # END - for filename

        # Process through the sensor organized dictionary in sorted order
        sorted_sensors = sorted(sensor_dict.keys())
        for sensor in sorted_sensors:
            dates = list()
            min_values = np.empty(0, dtype=np.float)
            max_values = np.empty(0, dtype=np.float)
            mean_values = np.empty(0, dtype=np.float)
            stddev_values = np.empty(0, dtype=np.float)

            # Collect all for a specific sensor
            # Sorted only works because we have date first in the list
            for (date, min_value, max_value, mean,
                 stddev) in sorted(sensor_dict[sensor]):
                dates.append(date)
                min_values = np.append(min_values, min_value)
                max_values = np.append(max_values, max_value)
                mean_values = np.append(mean_values, mean)
                stddev_values = np.append(stddev_values, stddev)

            # These operate on and come out as numpy arrays
            min_values = self.scale_data_to_range(data_max, data_min,
                                                  scale_max, scale_min,
                                                  min_values)
            max_values = self.scale_data_to_range(data_max, data_min,
                                                  scale_max, scale_min,
                                                  max_values)
            mean_values = self.scale_data_to_range(data_max, data_min,
                                                   scale_max, scale_min,
                                                   mean_values)
            stddev_values = self.scale_data_to_range(data_max, data_min,
                                                     scale_max, scale_min,
                                                     stddev_values)

            # Draw the min to max line for these dates
            if plot_type == "Range":
                min_plot.vlines(dates, min_values, max_values,
                                colors=self._sensor_colors[sensor],
                                linestyles='solid', linewidths=1)

            # Plot the lists of dates and values for the subject
            values = list()
            if lower_subject == 'minimum':
                values = min_values
            if lower_subject == 'maximum':
                values = max_values
            if lower_subject == 'mean':
                values = mean_values
            if lower_subject == 'stddev':
                values = stddev_values

            # Process through the data and plot segments of the data
            # (i.e. skip drawing lines between same date items)
            data_count = len(dates)
            x_data = list()
            y_data = list()
            for index in range(data_count):
                x_data.append(dates[index])
                y_data.append(values[index])

                if index < (data_count - 1):
                    if dates[index] == dates[index+1]:
                        # Draw the markers for this segment of the dates
                        min_plot.plot(x_data, y_data, label=sensor,
                                      marker=self._marker,
                                      color=self._sensor_colors[sensor],
                                      linestyle='-',
                                      markersize=self._marker_size,
                                      markeredgewidth=self._marker_edge_width)
                        x_data = list()
                        y_data = list()

            if len(x_data) > 0:
                # Draw the markers for the final segment of the dates
                min_plot.plot(x_data, y_data, label=sensor,
                              marker=self._marker,
                              color=self._sensor_colors[sensor],
                              linestyle='-',
                              markersize=self._marker_size,
                              markeredgewidth=self._marker_edge_width)

            # Cleanup the x and y data memory
            del x_data
            del y_data
        # END - for sensor

        # --------------------------------------------------------------------
        # Adjust the y range to help move them from the edge of the plot
        plot_y_min = display_min - 0.025
        plot_y_max = display_max + 0.025

        # Adjust the day range to help move them from the edge of the plot
        date_diff = plot_date_max - plot_date_min
        logger.debug(date_diff.days)
        for increment in range(0, int(date_diff.days/365) + 1):
            # Add 5 days to each end of the range for each year
            # With a minimum of 5 days added to each end of the range
            plot_date_min -= self._time_delta_5_days
            plot_date_max += self._time_delta_5_days
        logger.debug(plot_date_min)
        logger.debug(plot_date_max)
        logger.debug((plot_date_max - plot_date_min).days)

        # Configuration for the dates
        auto_date_locator = mpl_dates.AutoDateLocator()

        days_spanned = (plot_date_max - plot_date_min).days
        if days_spanned > 10 and days_spanned < 30:
            # I don't know why, but setting them to 9 works for us
            # Some other values also work, but as far as I am concerned the
            # AutoDateLocator is BROKEN!!!!!
            auto_date_locator = mpl_dates.AutoDateLocator(minticks=9,
                                                          maxticks=9)
        auto_date_formatter = mpl_dates.AutoDateFormatter(auto_date_locator)

        # X Axis details
        min_plot.xaxis.set_major_locator(auto_date_locator)
        min_plot.xaxis.set_major_formatter(auto_date_formatter)

        # X Axis - Limits - Determine the date range of the to-be-displayed
        #                   data
        min_plot.set_xlim(plot_date_min, plot_date_max)

        # X Axis - Label - Will always be 'Date'
        mpl_plot.xlabel('Date')

        # Y Axis details
        major_locator = MaxNLocator(max_n_locators)
        min_plot.yaxis.set_major_locator(major_locator)

        # Y Axis - Limits
        min_plot.set_ylim(plot_y_min, plot_y_max)

        # Y Axis - Label
        # We are going to make the Y Axis Label the title for now (See Title)
        # mpl_plot.ylabel(' '.join(subjects))

        # Plot - Title
        plot_name = ' '.join([plot_name, '-'] + subjects)
        # mpl_plot.title(plot_name)
        # The Title gets covered up by the legend so use the Y Axis Label
        mpl_plot.ylabel(plot_name)

        # Configure the legend
        legend = mpl_plot.legend(sorted_sensors,
                                 bbox_to_anchor=(0.0, 1.01, 1.0, 0.5),
                                 loc=3, ncol=6, mode="expand",
                                 borderaxespad=0.0, numpoints=1,
                                 prop={'size': 12})

        # Change the legend background color to match the plot background color
        frame = legend.get_frame()
        frame.set_facecolor(self._bg_color)

        # Fix the filename and save the plot
        filename = plot_name.replace('- ', '').lower()
        filename = filename.replace(' ', '_')
        filename = ''.join([filename, '_plot'])

        # Adjust the margins to be a little better
        mpl_plot.subplots_adjust(left=0.1, right=0.92, top=0.9, bottom=0.1)

        mpl_plot.grid(which='both', axis='y', linestyle='-')

        # Save the plot to a file
        mpl_plot.savefig('%s.png' % filename, dpi=100)

        # Close the plot so we can open another one
        mpl_plot.close()

    # -------------------------------------------
    def generate_plots(self, plot_name, stats_files, band_type):
        '''
        Description:
          Gather all the information needed for plotting from the files and
          generate a plot for each statistic
        '''

        logger = self._logger

        stats = dict()

        # Read each file into a dictionary
        for stats_file in stats_files:
            logger.debug(stats_file)
            stats[stats_file] = \
                dict((key, value) for(key, value)
                     in self.read_statistics(stats_file))
            if stats[stats_file]['valid'] == 'no':
                # Remove it so we do not have it in the plot
                logger.warning("[%s] Data is not valid:"
                               " Will not be used for plot generation"
                               % stats_file)
                del stats[stats_file]

        # Check if we have enough stuff to plot or not
        if len(stats) < 2:
            logger.warning("Not enough points to plot [%s] skipping plotting"
                           % plot_name)
            return

        plot_subjects = ['Minimum', 'Maximum', 'Mean']
        self.generate_plot(plot_name, plot_subjects, band_type, stats, "Range")

        plot_subjects = ['Minimum']
        self.generate_plot(plot_name, plot_subjects, band_type, stats)

        plot_subjects = ['Maximum']
        self.generate_plot(plot_name, plot_subjects, band_type, stats)

        plot_subjects = ['Mean']
        self.generate_plot(plot_name, plot_subjects, band_type, stats)

        plot_subjects = ['StdDev']
        self.generate_plot(plot_name, plot_subjects, band_type, stats)

    # -------------------------------------------
    def process_band_type(self, sensor_info, band_type):
        '''
        Description:
          A generic processing routine which finds the files to process based
          on the provided search criteria.  Utilizes the provided band type as
          part of the plot names and filenames.  If no files are found, no
          plots or combined statistics will be generated.
        '''

        single_sensor_files = list()
        multi_sensor_files = list()
        single_sensor_name = ''
        sensor_count = 0  # How many sensors were found....
        for (search_string, sensor_name) in sensor_info:
            single_sensor_files = glob.glob(search_string)
            if single_sensor_files and single_sensor_files is not None:
                if len(single_sensor_files) > 0:
                    sensor_count += 1  # We found another sensor
                    single_sensor_name = sensor_name
                    self.combine_sensor_stats(' '.join([sensor_name,
                                                        band_type]),
                                              single_sensor_files)
                    multi_sensor_files.extend(single_sensor_files)

        # Cleanup the memory for this while we process the multi-sensor list
        del single_sensor_files

        # We always use the multi sensor variable here because it will only
        # have the single sensor in it, if that is the case
        if sensor_count > 1:
            self.generate_plots("Multi Sensor %s" % band_type,
                                multi_sensor_files, band_type)
        elif sensor_count == 1 and len(multi_sensor_files) > 1:
            self.generate_plots(' '.join([single_sensor_name, band_type]),
                                multi_sensor_files, band_type)
        # Else do not plot

        # Remove the processed files
        if sensor_count > 0:
            for filename in multi_sensor_files:
                if os.path.exists(filename):
                    os.unlink(filename)

        del multi_sensor_files

    # -------------------------------------------
    def process_stats(self):
        '''
        Description:
          Process the stat results to plots.  If any bands/files do not exist,
          plots will not be generated for them.
        '''

        # Change to the working directory
        current_directory = os.getcwd()
        os.chdir(self._work_dir)

        try:
            # ----------------------------------------------------------------
            self.process_band_type(self._sr_coastal_sensor_info,
                                   "SR COASTAL AEROSOL")
            self.process_band_type(self._sr_blue_sensor_info, "SR Blue")
            self.process_band_type(self._sr_green_sensor_info, "SR Green")
            self.process_band_type(self._sr_red_sensor_info, "SR Red")
            self.process_band_type(self._sr_nir_sensor_info, "SR NIR")
            self.process_band_type(self._sr_swir1_sensor_info, "SR SWIR1")
            self.process_band_type(self._sr_swir2_sensor_info, "SR SWIR2")
            self.process_band_type(self._sr_cirrus_sensor_info, "SR CIRRUS")

            # ----------------------------------------------------------------
            self.process_band_type(self._sr_swir_modis_b5_sensor_info,
                                   "SR SWIR B5")

            # ----------------------------------------------------------------
            self.process_band_type(self._toa_thermal_sensor_info, "SR Thermal")

            # ----------------------------------------------------------------
            self.process_band_type(self._toa_coastal_sensor_info,
                                   "TOA COASTAL AEROSOL")
            self.process_band_type(self._toa_blue_sensor_info, "TOA Blue")
            self.process_band_type(self._toa_green_sensor_info, "TOA Green")
            self.process_band_type(self._toa_red_sensor_info, "TOA Red")
            self.process_band_type(self._toa_nir_sensor_info, "TOA NIR")
            self.process_band_type(self._toa_swir1_sensor_info, "TOA SWIR1")
            self.process_band_type(self._toa_swir2_sensor_info, "TOA SWIR2")
            self.process_band_type(self._toa_cirrus_sensor_info, "TOA CIRRUS")

            # ----------------------------------------------------------------
            self.process_band_type(self._emis_20_sensor_info, "Emis Band 20")
            self.process_band_type(self._emis_22_sensor_info, "Emis Band 22")
            self.process_band_type(self._emis_23_sensor_info, "Emis Band 23")
            self.process_band_type(self._emis_29_sensor_info, "Emis Band 29")
            self.process_band_type(self._emis_31_sensor_info, "Emis Band 31")
            self.process_band_type(self._emis_32_sensor_info, "Emis Band 32")

            # ----------------------------------------------------------------
            self.process_band_type(self._lst_day_sensor_info, "LST Day")
            self.process_band_type(self._lst_night_sensor_info, "LST Night")

            # ----------------------------------------------------------------
            self.process_band_type(self._ndvi_sensor_info, "NDVI")

            # ----------------------------------------------------------------
            self.process_band_type(self._evi_sensor_info, "EVI")

            # ----------------------------------------------------------------
            self.process_band_type(self._savi_sensor_info, "SAVI")

            # ----------------------------------------------------------------
            self.process_band_type(self._msavi_sensor_info, "MSAVI")

            # ----------------------------------------------------------------
            self.process_band_type(self._nbr_sensor_info, "NBR")

            # ----------------------------------------------------------------
            self.process_band_type(self._nbr2_sensor_info, "NBR2")

            # ----------------------------------------------------------------
            self.process_band_type(self._ndmi_sensor_info, "NDMI")

        finally:
            # Change back to the previous directory
            os.chdir(current_directory)

    # -------------------------------------------
    def stage_input_data(self):
        '''
        Description:
            Stages the input data required for the processor.
        '''

        options = self._parms['options']

        source_stats_files = os.path.join(options['statistics_directory'],
                                          'stats/*')

        # Transfer the files using scp
        # (don't provide any usernames and passwords)
        try:
            transfer.transfer_file(options['statistics_host'],
                                   source_stats_files,
                                   'localhost', self._work_dir)
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.staging_data, str(e)), \
                None, sys.exc_info()[2]

    # -------------------------------------------
    def get_product_name(self):
        '''
        Description:
            Return the product name for that statistics and plot product from
            the product request information.
        '''

        if self._product_name is None:
            self._product_name = '-'.join([self._parms['orderid'],
                                           'statistics'])

        return self._product_name

    # -------------------------------------------
    def process_product(self):
        '''
        Description:
            Perform the processor specific processing to generate the request
            product.
        '''

        # Stage the required input data
        self.stage_input_data()

        # Create the combinded stats and plots
        self.process_stats()

        # Package and deliver product
        (destination_product_file, destination_cksum_file) = \
            self.distribute_product()

        return (destination_product_file, destination_cksum_file)


# ===========================================================================
# ===========================================================================
def get_instance(parms):
    '''
    Description:
        Provides a method to retrieve the proper processor for the specified
        product.
    '''

    product_id = parms['product_id']

    if product_id == 'plot':
        return PlotProcessor(parms)

    sensor_code = sensor.instance(product_id).sensor_code.lower()

    if sensor_code == 'lt4':
        return LandsatTMProcessor(parms)
    elif sensor_code == 'lt5':
        return LandsatTMProcessor(parms)
    elif sensor_code == 'le7':
        return LandsatETMProcessor(parms)
    elif sensor_code == 'lo8':
        return LandsatOLIProcessor(parms)
    elif sensor_code == 'lt8':
        msg = "A processor for [%s] has not been implemented" % product_id
        raise NotImplementedError(msg)
    elif sensor_code == 'lc8':
        return LandsatOLITIRSProcessor(parms)
    elif sensor_code == 'mod':
        return ModisTERRAProcessor(parms)
    elif sensor_code == 'myd':
        return ModisAQUAProcessor(parms)
    else:
        msg = "A processor for [%s] has not been implemented" % product_id
        raise NotImplementedError(msg)
