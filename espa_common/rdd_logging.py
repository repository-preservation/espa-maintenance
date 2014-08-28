
import logging
import logging.config

import rdd_settings


class EspaLoggingException(Exception):
    '''
    Description:
        An exception just for the EspaLogging
    '''
    pass


class EspaLogger(object):
    my_config = None

    @classmethod
    def check_logger_configured(cls, logger_name):
        '''
        Desctription:
          Checks to see if a logger has been configured.

        On error raises:
          EspaLoggingException
        '''

        logger_name = logger_name.lower()

        if logger_name not in cls.my_config['loggers']:
            msg = ("Logger [%s] is not configured" % logger_name)
            raise EspaLoggingException(msg)

    @classmethod
    def configure(cls, logger_name, order=None, scene=None):
        '''
        Desctription:
          Adds a configured logger from settings to the loggers configured for
          this python execution instance.

        On error raises:
          EspaLoggingException
        '''

        logger_name = logger_name.lower()

        if logger_name not in rdd_settings.LOG_CONFIG['loggers']:
            msg = ("Logger [%s] is not a configured logger in settings.py"
                   % logger_name)
            raise EspaLoggingException(msg)

        if (logger_name == 'espa.processing'
                and (order is None or scene is None)):
            msg = ("Logger [espa.processing] is required to have an order and"
                   " scene for proper configuration of the log filename")
            raise EspaLoggingException(msg)

        # Basic initialization for the configuration
        if cls.my_config is None:
            cls.my_config = dict()
            cls.my_config['version'] = rdd_settings.LOG_CONFIG['version']
            cls.my_config['disable_existing_loggers'] = \
                rdd_settings.LOG_CONFIG['disable_existing_loggers']
            cls.my_config['loggers'] = dict()
            cls.my_config['handlers'] = dict()
            cls.my_config['formatters'] = dict()

            # Setup a basic logger so that we can use it for errors
            logging.basicConfig(level=logging.DEBUG)

        # Configure the logging
        if logger_name not in cls.my_config['loggers']:

            # For shorter access to them
            loggers = rdd_settings.LOG_CONFIG['loggers']
            handlers = rdd_settings.LOG_CONFIG['handlers']
            formatters = rdd_settings.LOG_CONFIG['formatters']

            # Copy the loggers dict for the logger we want
            cls.my_config['loggers'][logger_name] = \
                loggers[logger_name].copy()

            # Copy the handlers dict for the handlers we want
            for handler in loggers[logger_name]['handlers']:
                cls.my_config['handlers'][handler] = handlers[handler].copy()

                # Copy the formatter dict for the formatters we want
                # Copy the formatter dict for the formatters we want
                formatter_name = handlers[handler]['formatter']
                cls.my_config['formatters'][formatter_name] = \
                    formatters[formatter_name].copy()

            if (logger_name == 'espa.processing'
                    and order is not None and scene is not None):
                # Get the name of the handler to be modified
                handler_name = logger_name

                # Get the handler
                config_handler = cls.my_config['handlers'][handler_name]

                # Figure out the log path and name
                filename = '/tmp/espa-%s-%s-jobdebug.log' % (order, scene)
                config_handler['filename'] = filename

            # Now configure the python logging module
            logging.config.dictConfig(cls.my_config)

    @classmethod
    def get_logfilename(cls, logger_name):
        pass

    @classmethod
    def delete_logfile(cls, logger_name):
        pass

    @classmethod
    def read_logfile(cls, logger_name):
        pass

    @classmethod
    def get_logger(cls, logger_name):
        '''
        Desctription:
          Checks to see if a logger has been configured and returns the logger
          or generates an exception.

        On error raises:
          EspaLoggingException
        '''

        logger_name = logger_name.lower()

        try:
            cls.check_logger_configured(logger_name)
        except Exception, e:
            raise

        return logging.getLogger(logger_name)
