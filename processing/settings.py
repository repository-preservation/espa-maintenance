

##############################################################################
# Used in cdr_ecv_cron.py

# Specifies the buffer length for an order line in the order file
# The Hadoop File System block size should be a multiple of this value
order_buffer_length = 2048

# Set the hadoop timeouts to a ridiculous number so jobs don't get killed
# before they are done
hadoop_timeout = 172800000  # which is 2 days


##############################################################################
# Used in cdr_ecv.py

# Path to the Landsat L1T source data location
landsat_base_source_path = '/data/standard_l1t'


##############################################################################
# Used in modis.py

# Path to the MODIS Terra source data location
terra_base_source_path = '/MOLT'
# Path to the MODIS Aqua source data location
aqua_base_source_path = '/MOLA'


##############################################################################
# Used in cdr_ecv.py and modis.py

# Path to place the completed orders
espa_base_output_path = '/data2/LSRD'


##############################################################################
# Used in lpvs_cron.py

# Path to the completed orders
espa_cache_directory = '/data2/LSRD'
# Can override this by setting the environment variable DEV_CACHE_DIRECTORY


##############################################################################
# Used by browse.py and science.py

# Default resolution for browse generation
default_browse_resolution = 50


##############################################################################
# Used by science.py

# Default name for the solr collection
default_solr_collection_name = 'DEFAULT_COLLECTION'

# The limit value for when to start splitting clouds
# It is used as a string in the code since it is passed to the cfmask
# executable
cfmask_max_cloud_pixels = '5000000'


##############################################################################
# Used by distribution.py

# Number of seconds to sleep when errors are encountered before attempting the
# task again
default_sleep_seconds = 2

# Maximum number of times to attempt packaging, delivery, and distribution
max_packaging_attempts = 3
max_delivery_attempts = 3
max_distribution_attempts = 5


##############################################################################
# Used by util.py

# List of hostnames to choose from for the access to the online cache
# 140 is here twice so the load is 2/3 + 1/3.  machines are mismatched
espa_cache_host_list = ['edclxs67p', 'edclxs140p', 'edclxs140p']
# Can override this by setting the environment variable DEV_CACHE_HOSTNAME
