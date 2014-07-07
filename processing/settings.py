

##############################################################################
# Used in cdr_ecv_cron.py

# Specifies the buffer length for an order line in the order file
# The Hadoop File System block size should be a multiple of this value
ORDER_BUFFER_LENGTH = 2048

# Set the hadoop timeouts to a ridiculous number so jobs don't get killed
# before they are done
HADOOP_TIMEOUT = 172800000  # which is 2 days


##############################################################################
# Used in cdr_ecv.py

# Path to the Landsat L1T source data location
LANDSAT_BASE_SOURCE_PATH = '/data/standard_l1t'


##############################################################################
# Used in modis.py

# Path to the MODIS Terra source data location
TERRA_BASE_SOURCE_PATH = '/MOLT'
# Path to the MODIS Aqua source data location
AQUA_BASE_SOURCE_PATH = '/MOLA'


##############################################################################
# Used in cdr_ecv.py and modis.py

# Path to place the completed orders
ESPA_BASE_OUTPUT_PATH = '/data2/LSRD'


##############################################################################
# Used in lpcs_cron.py

# Path to the completed orders
ESPA_CACHE_DIRECTORY = '/data2/LSRD'
# Can override this by setting the environment variable DEV_CACHE_DIRECTORY


##############################################################################
# Used by browse.py and science.py

# Default resolution for browse generation
DEFAULT_BROWSE_RESOLUTION = 50


##############################################################################
# Used by science.py

# Default name for the solr collection
DEFAULT_SOLR_COLLECTION_NAME = 'DEFAULT_COLLECTION'

# The limit value for when to start splitting clouds
# It is used as a string in the code since it is passed to the cfmask
# executable
CFMASK_MAX_CLOUD_PIXELS = '5000000'


##############################################################################
# Used by distribution.py

# Number of seconds to sleep when errors are encountered before attempting the
# task again
DEFAULT_SLEEP_SECONDS = 2

# Maximum number of times to attempt packaging, delivery, and distribution
MAX_PACKAGING_ATTEMPTS = 3
MAX_DELIVERY_ATTEMPTS = 3
MAX_DISTRIBUTION_ATTEMPTS = 5


##############################################################################
# Used by util.py

# List of hostnames to choose from for the access to the online cache
# 140 is here twice so the load is 2/3 + 1/3.  machines are mismatched
ESPA_CACHE_HOST_LIST = ['edclxs67p', 'edclxs140p', 'edclxs140p']
# Can override this by setting the environment variable DEV_CACHE_HOSTNAME

# Where to place the temporary scene processing log files
LOGFILE_PATH = '/tmp'


##############################################################################
# Used by statistics.py

# Band type data ranges.  They are intended to be used for removing outliers
# from the data before statistics generation
# Must match DATA_MAX_Y and DATA_MIN_Y values in plot.py
# The types must match the types in cdr_ecv.py and modis.py
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
