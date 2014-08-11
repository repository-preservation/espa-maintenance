
import logging
import logging.config


LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'espa.standard': {
            'format': ('%(asctime)s.%(msecs)03d %(process)d'
                       ' %(levelname)-8s'
                       ' %(filename)s:%(lineno)d:%(funcName)s'
                       ' -- %(message)s'),
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'espa.thread': {
            'format': ('%(asctime)s.%(msecs)03d %(process)d'
                       ' %(levelname)-8s'
                       ' %(filename)s:%(lineno)d:%(funcName)s'
                       ' %(thread)d'
                       ' -- %(message)s'),
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'processing': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard',
            'filename': '/tmp/espa_processing.log',
            'mode': 'a'
        },
        'web': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.thread',
            'filename': '/tmp/espa_web.log',
            'mode': 'a'
        }
    },
    'loggers': {
        'processing': {
            'level': 'INFO',
            'propagate': False,
            'handlers': ['processing']
        },
        'web': {
            'level': 'INFO',
            'propagate': False,
            'handlers': ['web']
        },
        'django.request': {
            'level': 'ERROR',
            'propagate': False,
            'handlers': ['web'],
        }
    }
}


def configure_log_handler(handler_name=None, filename=None):

    if not handler_name:
        raise Exception("You must specify a handler_name")

    if not filename:
        raise Exception("You must specify a filename for the log")

    LOG_CONFIG['handlers'][handler_name]['filename'] = ('/tmp/espa_%s.log'
                                                       % filename)

def configure_logger(logger_name=None, level='INFO'):

    if not logger_name:
        raise Exception("You must specify a logger_name")

    LOG_CONFIG['loggers'][logger_name]['level'] = level


def configure_loggers():
    logging.config.dictConfig(LOG_CONFIG)
