
# imports from espa/espa_common
try:
    from logger_factory import EspaLogging
except:
    from espa_common.logger_factory import EspaLogging


class ProductProcessor(object):
    '''
    Description:
        Provides the super class implementation for processing products.
    '''

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

        msg = "[%s] needs to be implemented in the child class" % __name__
        raise NotImplementedError(msg)

    # -------------------------------------------
    def stage_input_data(self, parms):
        '''
        Description:
            Stages the input data required for the processor.
        '''

        msg = "[%s] needs to be implemented in the child class" % __name__
        raise NotImplementedError(msg)

    # -------------------------------------------
    def process(self, parms):
        '''
        Description:
            Generates a product through a defined process.
        '''

        # Validate the parameters
        self.validate_parameters(parms)

        # Build the command line
        # TODO TODO TODO

        # Initialize the processing directory.
        # TODO TODO TODO

        # Stage the required input data
        self.stage_input_data()

        # Transfer the staged data to the work directory
        # TODO TODO TODO

        # Build science products
        # TODO TODO TODO

        # Deliver product
        # TODO TODO TODO

        # Cleanup the processing directory to free disk space for other
        # products to process.
        # TODO TODO TODO
        #self.cleanup_processing_directory()
