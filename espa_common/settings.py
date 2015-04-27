
# Specifies the buffer length for an order line in the order file
# The Hadoop File System block size should be a multiple of this value
ORDER_BUFFER_LENGTH = 2048

# Set the hadoop timeouts to a ridiculous number so jobs don't get killed
# before they are done
HADOOP_TIMEOUT = 172800000  # which is 2 days

# Specifies the hadoop queue to use based on priority
# 'all' must be present as it is used in the cron code to pass 'None' instead
HADOOP_QUEUE_MAPPING = {
    'all': 'ondemand',
    'low': 'ondemand-low',
    'normal': 'ondemand',
    'high': 'ondemand-high'
}

# filename extension for landsat input products
LANDSAT_INPUT_FILENAME_EXTENSION = '.tar.gz'

# Path to the MODIS Terra source data location
TERRA_BASE_SOURCE_PATH = '/MOLT'
# Path to the MODIS Aqua source data location
AQUA_BASE_SOURCE_PATH = '/MOLA'

# file extension for modis input products
MODIS_INPUT_FILENAME_EXTENSION = '.hdf'

# host for modis input checks
MODIS_INPUT_CHECK_HOST = 'e4ftl01.cr.usgs.gov'

# port for modis input checks
MODIS_INPUT_CHECK_PORT = 80

# Path to the completed orders
ESPA_CACHE_DIRECTORY = '/data2/LSRD/orders'

ESPA_EMAIL_ADDRESS = 'espa@usgs.gov'

ESPA_EMAIL_SERVER = 'gssdsflh01.cr.usgs.gov'

# Default resolution for browse generation
DEFAULT_BROWSE_RESOLUTION = 50

# Default name for the solr collection
DEFAULT_SOLR_COLLECTION_NAME = 'DEFAULT_COLLECTION'

# The limit value for when to start splitting clouds
# It is used as a string in the code since it is passed to the cfmask
# executable
CFMASK_MAX_CLOUD_PIXELS = '5000000'

# Number of seconds to sleep when errors are encountered before attempting the
# task again
DEFAULT_SLEEP_SECONDS = 2

# Maximum number of times to attempt packaging, delivery, and distribution
MAX_PACKAGING_ATTEMPTS = 3
MAX_DELIVERY_ATTEMPTS = 3
MAX_DISTRIBUTION_ATTEMPTS = 5

# Maximum number of times to attempt setting the scene error
MAX_SET_SCENE_ERROR_ATTEMPTS = 5

# List of hostnames to choose from for the access to the online cache
# Runs over 10Gb line
ESPA_CACHE_HOST_LIST = ['edclxs67p', 'edclxs140p']

# The external name for the online cache.  Runs over 1Gb line.
EXTERNAL_CACHE_HOST = 'edclpdsftp.cr.usgs.gov'

# Where to place the temporary scene processing log files
LOGFILE_PATH = '/tmp'

PLOT_BG_COLOR = '#f3f3f3'  # A light gray
PLOT_MARKER = (1, 3, 0)    # Better circle than 'o'
PLOT_MARKER_SIZE = 5.0     # A good size for the circle or diamond
PLOT_MARKER_EDGE_WIDTH = 0.9  # The width of the black marker border

# We are only supporting one radius when warping to sinusoidal
SINUSOIDAL_SPHERE_RADIUS = 6371007.181

# Some defines for common pixels sizes in decimal degrees
DEG_FOR_30_METERS = 0.0002695
DEG_FOR_15_METERS = (DEG_FOR_30_METERS / 2.0)
DEG_FOR_1_METER = (DEG_FOR_30_METERS / 30.0)

# Supported datums - the strings for them
WGS84 = 'WGS84'
NAD27 = 'NAD27'
NAD83 = 'NAD83'

TRANSFER_BLOCK_SIZE = 10485760

# We do not allow any user selectable choices for this projection
GEOGRAPHIC_PROJ4_STRING = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"

# Band type data ranges.  They are intended to be used for removing outliers
# from the data before statistics generation
# Must match DATA_MAX_Y and DATA_MIN_Y values in plotting.py
# The types must match the types in cdr_ecv.py and modis.py
# Note: These are also defined in such away that the fill values are also
#       excluded.
BAND_TYPE_STAT_RANGES = {
    'SR': {
        'UPPER_BOUND': 10000,
        'LOWER_BOUND': 0
    },
    'TOA': {
        'UPPER_BOUND': 10000,
        'LOWER_BOUND': 0
    },
    'INDEX': {
        'UPPER_BOUND': 10000,
        'LOWER_BOUND': -1000
    },
    'LST': {
        'UPPER_BOUND': 65535,
        'LOWER_BOUND': 7500
    },
    'EMIS': {
        'UPPER_BOUND': 255,
        'LOWER_BOUND': 1
    }
}

'''Resolves system-wide identification of sensor name based on three letter
   prefix
'''

SENSOR_INFO = {
    'LO8': {'name': 'oli', 'lta_name': 'LANDSAT_8'},
    'LC8': {'name': 'olitirs', 'lta_name': 'LANDSAT_8'},
    'LE7': {'name': 'etm', 'lta_name': 'LANDSAT_ETM_PLUS'},
    'LT4': {'name': 'tm', 'lta_name': 'LANDSAT_TM'},
    'LT5': {'name': 'tm', 'lta_name': 'LANDSAT_TM'},
    'MYD': {'name': 'aqua'},
    'MOD': {'name': 'terra'}
}

'''Default pixel sizes based on the input products'''
DEFAULT_PIXEL_SIZE = {
    'meters': {
        '09A1': 500,
        '09GA': 500,
        '09GQ': 250,
        '09Q1': 250,
        '13Q1': 250,
        '13A3': 1000,
        '13A2': 1000,
        '13A1': 500,
        'LC8': 30,
        'LO8': 30,
        'LE7': 30,
        'LT4': 30,
        'LT5': 30
    },
    'dd': {
        '09A1': 0.00449155,
        '09GA': 0.00449155,
        '09GQ': 0.002245775,
        '09Q1': 0.002245775,
        '13Q1': 0.002245775,
        '13A3': 0.0089831,
        '13A2': 0.0089831,
        '13A1': 0.00449155,
        'LC8': 0.0002695,
        'LO8': 0.0002695,
        'LE7': 0.0002695,
        'LT4': 0.0002695,
        'LT5': 0.0002695
        }
}

''' Constant dictionary to hold the cache keys used in Django
 caching/memcached'''
CACHE_KEYS = {


}

''' SOAP client configuration parameters '''
# timeout is in seconds
SOAP_CLIENT_TIMEOUT = 60 * 30

# location where the WSDLS should be cached
SOAP_CACHE_LOCATION = '/tmp/suds'


''' Dictionary containing retry timeouts in seconds'''
RETRY = {
    'http_errors': {'timeout': 60 * 15, 'retry_limit': 10},
    'ftp_errors': {'timeout': 60 * 15, 'retry_limit': 10},
    'gzip_errors': {'timeout': 60 * 60 * 6, 'retry_limit': 10},
    'network_errors': {'timeout': 60 * 2, 'retry_limit': 5},
    'db_lock_timeout': {'timeout': 60 * 5, 'retry_limit': 10},
    'lta_soap_errors': {'timeout': 60 * 60, 'retry_limit': 12},
    'missing_aux_data': {'timeout': 60 * 60 * 24, 'retry_limit': 5},
    'retry_missing_l1': {'timeout': 60 * 60, 'retry_limit': 8},
    'ssh_errors': {'timeout': 60 * 5, 'retry_limit': 3}
}

'''
LOGGING DEFINITIONS
'''
PROCESSING_LOGGER = 'espa.processing'

LOGGER_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'espa.standard': {
            # Used by the processing and web systems
            'format': ('%(asctime)s.%(msecs)03d %(process)d'
                       ' %(levelname)-8s'
                       ' %(filename)s:%(lineno)d:%(funcName)s'
                       ' -- %(message)s'),
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'espa.standard.low': {
            # Provided so 'low' is added to the log message
            'format': ('%(asctime)s.%(msecs)03d %(process)d'
                       ' %(levelname)-8s    low '
                       ' %(filename)s:%(lineno)d:%(funcName)s'
                       ' -- %(message)s'),
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'espa.standard.normal': {
            # Provided so 'normal' is added to the log message
            'format': ('%(asctime)s.%(msecs)03d %(process)d'
                       ' %(levelname)-8s normal '
                       ' %(filename)s:%(lineno)d:%(funcName)s'
                       ' -- %(message)s'),
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'espa.standard.high': {
            # Provided so 'high' is added to the log message
            'format': ('%(asctime)s.%(msecs)03d %(process)d'
                       ' %(levelname)-8s   high '
                       ' %(filename)s:%(lineno)d:%(funcName)s'
                       ' -- %(message)s'),
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'espa.thread': {
            # An example for threading, not currently used
            'format': ('%(asctime)s.%(msecs)03d %(process)d'
                       ' %(levelname)-8s'
                       ' %(filename)s:%(lineno)d:%(funcName)s'
                       ' %(thread)d'
                       ' -- %(message)s'),
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        # All espa.* handler names need to match the espa.* logger names
        'espa.cron.all': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard',
            'filename': '/tmp/espa-cron.log',
            'mode': 'a'
        },
        'espa.cron.low': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard.low',
            'filename': '/tmp/espa-cron.log',
            'mode': 'a'
        },
        'espa.cron.normal': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard.normal',
            'filename': '/tmp/espa-cron.log',
            'mode': 'a'
        },
        'espa.cron.high': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard.high',
            'filename': '/tmp/espa-cron.log',
            'mode': 'a'
        },
        'espa.cron.plot': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard',
            'filename': '/tmp/espa-plot-cron.log',
            'mode': 'a'
        },
        'espa.cron.orderdisp': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard',
            'filename': '/tmp/espa-order-disposition-cron.log',
            'mode': 'a'
        },
        'espa.processing': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard',
            'filename': '/tmp/espa-processing.log',
            'mode': 'a'
        },
        'espa.web': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'espa.standard',
            'filename': '/tmp/espa-web.log',
            'mode': 'a'
        }
    },
    'loggers': {
        # All espa.* logger names need to match the espa.* handler names
        # All espa.cron.<priority> must match the priority levels defined in
        # settings.HADOOP_QUEUE_MAPPING above
        'espa.cron.all': {
            # To be used by the 'all' cron
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.cron.all']
        },
        'espa.cron.low': {
            # To be used by the 'low' cron
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.cron.low']
        },
        'espa.cron.normal': {
            # To be used by the 'normal' cron
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.cron.normal']
        },
        'espa.cron.high': {
            # To be used by the 'high' cron
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.cron.high']
        },
        'espa.cron.plot': {
            # To be used by the 'lpcs' cron
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.cron.plot']
        },
        'espa.cron.orderdisp': {
            # To be used by the order disposition cron
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.cron.orderdisp']
        },
        'espa.processing': {
            # To be used by the processing system
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.processing']
        },
        'espa.web': {
            # To be used by the web system
            'level': 'INFO',
            'propagate': False,
            'handlers': ['espa.web']
        },
        'django.request': {
            # To be used by django
            'level': 'ERROR',
            'propagate': False,
            'handlers': ['espa.web'],
        }
    }
}
