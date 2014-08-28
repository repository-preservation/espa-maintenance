
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
        'espa.standard.low': {
            'format': ('%(asctime)s.%(msecs)03d %(process)d'
                       ' %(levelname)-8s    low  '
                       ' %(filename)s:%(lineno)d:%(funcName)s'
                       ' -- %(message)s'),
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'espa.standard.normal': {
            'format': ('%(asctime)s.%(msecs)03d %(process)d'
                       ' %(levelname)-8s normal  '
                       ' %(filename)s:%(lineno)d:%(funcName)s'
                       ' -- %(message)s'),
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'espa.standard.high': {
            'format': ('%(asctime)s.%(msecs)03d %(process)d'
                       ' %(levelname)-8s   high  '
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
        # All espa handler names need to match the espa logger names
        'espa.cron.all': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard',
            'filename': '/tmp/espa_cron.log',
            'mode': 'a'
        },
        'espa.cron.low': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard.low',
            'filename': '/tmp/espa_cron.log',
            'mode': 'a'
        },
        'espa.cron.normal': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard.normal',
            'filename': '/tmp/espa_cron.log',
            'mode': 'a'
        },
        'espa.cron.high': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard.high',
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
        'espa.cron.all': {
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.cron.all']
        },
        'espa.cron.low': {
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.cron.low']
        },
        'espa.cron.normal': {
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.cron.normal']
        },
        'espa.cron.high': {
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.cron.high']
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
