
import os
import logging
import logging.config

import settings


class EspaLoggerException(Exception):
    '''
    Description:
        An exception just for the EspaLogging class.
    '''
    pass


class EspaLogging(object):
    my_config = None
    basic_logger_configured = False

    @classmethod
    def check_logger_configured(cls, logger_name):
        '''
        Description:
          Checks to see if a logger has been configured.

        On error raises:
          EspaLoggerException
        '''

        logger_name = logger_name.lower()

        if logger_name not in cls.my_config['loggers']:
            msg = ("Reporter [%s] is not configured" % logger_name)
            raise EspaLoggerException(msg)

    @classmethod
    def configure_base_logger(cls,
                              filename='/tmp/espa-base-logger.log',
                              format=('%(asctime)s.%(msecs)03d %(process)d'
                                      ' %(levelname)-8s'
                                      ' %(filename)s:%(lineno)d:%(funcName)s'
                                      ' -- %(message)s'),
                              datefmt='%Y-%m-%d %H:%M:%S',
                              level=logging.DEBUG):

        if not cls.basic_logger_configured:
            # Setup a base logger so that we can use it for errors
            logging.basicConfig(filename=filename, format=format,
                                datefmt=datefmt, level=level)

            cls.basic_logger_configured = True

    @classmethod
    def configure(cls, logger_name, order=None, product=None, debug=False):
        '''
        Description:
          Adds a configured logger from settings to the logging configured
          for this python execution instance.

        On error raises:
          EspaLoggerException
        '''

        logger_name = logger_name.lower()

        if logger_name not in settings.LOGGER_CONFIG['loggers']:
            msg = ("Reporter [%s] is not a configured logger in settings.py"
                   % logger_name)
            raise EspaLoggerException(msg)

        if (logger_name == 'espa.processing'
                and (order is None or product is None)):
            msg = ("Reporter [espa.processing] is required to have an order"
                   " and product for proper configuration of the loggers"
                   " filename")
            raise EspaLoggerException(msg)

        # Basic initialization for the configuration
        if cls.my_config is None:
            cls.my_config = dict()
            cls.my_config['version'] = settings.LOGGER_CONFIG['version']
            cls.my_config['disable_existing_loggers'] = \
                settings.LOGGER_CONFIG['disable_existing_loggers']
            cls.my_config['loggers'] = dict()
            cls.my_config['handlers'] = dict()
            cls.my_config['formatters'] = dict()

            # Setup a basic logger so that we can use it for errors
            cls.configure_base_logger()

        # Configure the logger
        if logger_name not in cls.my_config['loggers']:

            # For shorter access to them
            loggers = settings.LOGGER_CONFIG['loggers']
            handlers = settings.LOGGER_CONFIG['handlers']
            formatters = settings.LOGGER_CONFIG['formatters']

            # Copy the loggers dict for the logger we want
            cls.my_config['loggers'][logger_name] = \
                loggers[logger_name].copy()

            # Turn on debug level logging if requested
            # TODO - May only want to do this for file loggers
            if debug:
                cls.my_config['loggers'][logger_name]['level'] = 'DEBUG'

            # Copy the handlers dict for the handlers we want
            for handler in loggers[logger_name]['handlers']:
                cls.my_config['handlers'][handler] = handlers[handler].copy()

                # Copy the formatter dict for the formatters we want
                formatter_name = handlers[handler]['formatter']
                cls.my_config['formatters'][formatter_name] = \
                    formatters[formatter_name].copy()

            if (logger_name == 'espa.processing'
                    and order is not None and product is not None):
                # Get the name of the handler to be modified
                handler_name = logger_name

                # Get the handler
                config_handler = cls.my_config['handlers'][handler_name]

                # Override the logger path and name
                filename = '/tmp/espa-job-%s-%s.log' % (order, product)
                config_handler['filename'] = filename

            # Now configure the python logging module
            logging.config.dictConfig(cls.my_config)

    @classmethod
    def get_filename(cls, logger_name):
        '''
        Description:
          Returns the full path and name of the file used for the specified
          logger.

        On error raises:
          EspaLoggerException
        '''

        logger_name = logger_name.lower()
        cls.check_logger_configured(logger_name)

        handler = cls.my_config['handlers'][logger_name]

        if handler['class'] != 'logging.FileHandler':
            msg = ("Reporter [%s] is not a file logger" % logger_name)
            raise EspaLoggerException(msg)

        return handler['filename']

    @classmethod
    def delete_logger_file(cls, logger_name):
        '''
        Description:
          Deletes the file associated with the specified logger.

        On error raises:
          EspaLoggerException
        '''

        logger_name = logger_name.lower()
        cls.check_logger_configured(logger_name)

        handler = cls.my_config['handlers'][logger_name]

        if handler['class'] != 'logging.FileHandler':
            msg = ("Reporter [%s] is not a file logger" % logger_name)
            raise EspaLoggerException(msg)

        filename = handler['filename']

        if os.path.exists(filename):
            try:
                os.unlink(filename)
            except Exception, e:
                raise EspaLoggerException(str(e))

    @classmethod
    def read_logger_file(cls, logger_name):
        '''
        Description:
          Reads and returns the contents of the file associated with the
          specified logger.

        On error raises:
          EspaLoggerException
        '''

        logger_name = logger_name.lower()
        cls.check_logger_configured(logger_name)

        handler = cls.my_config['handlers'][logger_name]

        if handler['class'] != 'logging.FileHandler':
            msg = ("Reporter [%s] is not a file logger" % logger_name)
            raise EspaLoggerException(msg)

        filename = handler['filename']

        file_data = ''
        if os.path.exists(filename):
            with open(filename, "r") as file_fd:
                file_data = file_fd.read()

        return file_data

    @classmethod
    def get_logger(cls, logger_name):
        '''
        Description:
          Checks to see if a logger has been configured and returns the
          logger or generates an exception.

        On error raises:
          EspaLoggerException
        '''

        logger_name = logger_name.lower()
        if logger_name != 'base':
            cls.check_logger_configured(logger_name)

        return logging.getLogger(logger_name)
