
import logging
import logging.config

import rdd_settings


class EspaLogger(object):
    configured = False

    @classmethod
    def configure(cls, order, scene):

        if not cls.configured:
            # Setup a basic logger so that we can use it for errors
            logging.basicConfig(level=logging.DEBUG)

            # Figure out the log path and name
            filename = '/tmp/espa-%s-%s-jobdebug.log' % (order, scene)

            # Get the name of the handler to be modified
            handler_name = rdd_settings.LOGGER_ALIAS['PROCESSING']

            # Get the handler
            config_handler = rdd_settings.LOG_CONFIG['handlers'][handler_name]

            # Set the filename
            config_handler['filename'] = filename

            # Now configure the python logging module
            logging.config.dictConfig(rdd_settings.LOG_CONFIG)

            # Let the class know we are configured
            cls.configured = True

        return logging.getLogger(rdd_settings.LOGGER_ALIAS['PROCESSING'])

    @classmethod
    def get_logfile(cls, logger_name):
        pass

    @classmethod
    def delete_logfile(cls, logger_name):
        pass

    @classmethod
    def read_logfile(cls, logger_name):
        pass

    @classmethod
    def getLogger(cls, logger_name):
        if not cls.configured:
            raise Exception("EspaLogger not configured you dummy")

        return logging.getLogger(logger_name)
