
'''
    Description:
      This defines the names to be used for the handlers and the loggers that
      are used by the ESPA system.
      Each of these must be defined in the following LOG_CONFIG object.
'''
LOGGER_ALIAS = {
    'CRON': 'espa.cron',
    'PROCESSING': 'espa.processing',
    'WEB': 'espa.web'
}


'''
    Description:
      This defines the logging configuration to be provided to the python
      "logging" module.  It must conform to the defined LOGGER_ALIAS's above.
'''
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
        'espa.cron': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard',
            'filename': '/tmp/espa_cron.log',
            'mode': 'a'
        },
        'espa.processing': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard',
            'filename': '/tmp/espa_processing.log',
            'mode': 'a'
        },
        'espa.web': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard',
            'filename': '/tmp/espa_web.log',
            'mode': 'a'
        }
    },
    'loggers': {
        'espa.cron': {
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.cron']
        },
        'espa.processing': {
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.processing']
        },
        'espa.web': {
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.web']
        },
        'django.request': {
            'level': 'ERROR',
            'propagate': False,
            'handlers': ['espa.web'],
        }
    }
}
