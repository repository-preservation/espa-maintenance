"""
Django settings for espa_web project.

For more information on this file, see
https://docs.djangoproject.com/en/1.6/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.6/ref/settings/
"""

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
import os
import ConfigParser

#this is the location of the main project directory
#NOT the directory this file lives in!!!
BASE_DIR = os.path.dirname(os.path.dirname(__file__))

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.6/howto/deployment/checklist/

#load up the site specific config file.
#If one is not specified default to the user home directory, looking for .cfgno
ESPA_CONFIG_FILE = os.environ.get('ESPA_CONFIG_FILE',
                                  os.path.join(os.path.expanduser('~'),
                                               '.cfgnfo'))

#stop everything if we don't have the config file
if not os.path.exists(ESPA_CONFIG_FILE):
    raise Exception("Espa config file not found at %s... exiting"
                    % ESPA_CONFIG_FILE)

config = ConfigParser.RawConfigParser()

with open(ESPA_CONFIG_FILE) as file_handle:
    config.readfp(file_handle)


# set the ESPA_ENV variable correctly
ESPA_ENV = 'dev'

# ************t*************
# NEVER CHANGE THIS TO ops IN dev OR tst UNLESS THE dev AND tst CRONS ARE OFF
# *************************
if "ESPA_ENV" in os.environ:
    if os.environ['ESPA_ENV'].lower() == 'ops':
        ESPA_ENV = 'ops'
    elif os.environ['ESPA_ENV'].lower() == 'tst':
        ESPA_ENV = 'tst'
    elif os.environ['ESPA_ENV'].lower() == 'dev':
        ESPA_ENV = 'dev'
    else:
        raise Exception("ESPA_ENV set to unknown value:%s... \
            must be one of 'dev', 'tst' or 'ops'... \
            cannot continue" % os.environ['ESPA_ENV'])


# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = config.get('config', 'key')

# SECURITY WARNING: don't run with debug turned on in production!
#allow us to override this with env var
DEBUG = False
TEMPLATE_DEBUG = False

#make sure its set to a proper value
if os.environ.get('ESPA_DEBUG', '').lower() == 'true':
    DEBUG = True
    TEMPLATE_DEBUG = True


ALLOWED_HOSTS = ['*']


# Application definition

INSTALLED_APPS = (
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'ordering',
    'console',
)

MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
)

#where do we find the initial set of urls?
ROOT_URLCONF = 'espa_web.urls'

WSGI_APPLICATION = 'espa_web.wsgi.application'


# Database
# https://docs.djangoproject.com/en/1.6/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',       # Add 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
        'NAME': config.get('config', 'db'),         # Or path to database file if using sqlite3.
        'USER': config.get('config', 'dbuser'),     # Not used with sqlite3.
        'PASSWORD': config.get('config', 'dbpass'), # Not used with sqlite3.
        'HOST': config.get('config', 'dbhost'),     # Set to empty string for localhost. Not used with sqlite3.
        'PORT': config.get('config', 'dbport'),     # Set to empty string for default. Not used with sqlite3.
    },
    #'postgres': {
    #    'ENGINE': 'django.db.backends.postgresql_psycopg2',
    #    'NAME': config.get('config', 'post-db'),
    #    'USER': config.get('config', 'post-user'),
    #    'PASSWORD': config.get('config', 'post-pass'),
    #    'HOST': config.get('config', 'post-host'),
    #    'PORT': config.get('config', 'post-port')
    #}
}

# Internationalization
# https://docs.djangoproject.com/en/1.6/topics/i18n/

LANGUAGE_CODE = 'en-us'

#TIME_ZONE = 'UTC'
TIME_ZONE = 'America/Chicago'

USE_I18N = True

USE_L10N = True

USE_TZ = False


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.6/howto/static-files/
STATIC_ROOT = os.path.join(BASE_DIR, 'espa_web', 'static/')

STATIC_URL = '/static/'

# Templates

TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates"
    #or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    os.path.join(BASE_DIR, "espa_web/templates"),
)

TEMPLATE_CONTEXT_PROCESSORS = (
    'django.core.context_processors.debug',
    'django.core.context_processors.i18n',
    'django.core.context_processors.media',
    'django.core.context_processors.static',
    'django.contrib.auth.context_processors.auth',
    'django.contrib.messages.context_processors.messages',
    'espa_web.context_processors.include_external_urls',
)

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.Loader',
    'django.template.loaders.app_directories.Loader',
)

#ESPA Service URLS
SERVICE_LOCATOR = {
    "sys": {
        "orderservice": "http://eedev.cr.usgs.gov/OrderWrapperServicedevsys/resources",
        "orderdelivery": "http://eedev.cr.usgs.gov/OrderDeliverydevsys/OrderDeliveryService?WSDL",
        "orderupdate": "http://eedev.cr.usgs.gov/OrderStatusServicedevsys/OrderStatusService?wsdl",
        "massloader": "http://eedev.cr.usgs.gov/MassLoaderdevsys/MassLoader?wsdl",
        "registration": "http://eedev.cr.usgs.gov/RegistrationServicedevsys/RegistrationService?wsdl",
        "register_user": "https://eedev.cr.usgs.gov/devsys/register/",
        "earthexplorer": "https://eedev.cr.usgs.gov/devsys",
        "forgot_login": "https://eedev.cr.usgs.gov/devsys/login/username"
    },
    "dev": {
        "orderservice": "http://eedevmast.cr.usgs.gov/OrderWrapperServicedevmast/resources",
        "orderdelivery": "http://eedevmast.cr.usgs.gov/OrderDeliverydevmast/OrderDeliveryService?WSDL",
        "orderupdate": "http://eedevmast.cr.usgs.gov/OrderStatusServicedevmast/OrderStatusService?wsdl",
        "massloader": "http://eedevmast.cr.usgs.gov/MassLoaderdevmast/MassLoader?wsdl",
        "registration": "http://eedevmast.cr.usgs.gov/RegistrationServicedevmast/RegistrationService?wsdl",
        "register_user": "https://eedevmast.cr.usgs.gov/register",
        "earthexplorer": "https://eedevmast.cr.usgs.gov",
        "forgot_login": "https://eedevmast.cr.usgs.gov/login/username"
    },
    "tst": {
        "orderservice": "http://edclxs152.cr.usgs.gov/OrderWrapperService/resources",
        "orderdelivery": "http://edclxs152.cr.usgs.gov/OrderDeliveryService/OrderDeliveryService?WSDL",
        "orderupdate": "http://edclxs152.cr.usgs.gov/OrderStatusService/OrderStatusService?wsdl",
        "massloader": "http://edclxs152.cr.usgs.gov/MassLoader/MassLoader?wsdl",
        "registration": "http://edclxs152.cr.usgs.gov/RegistrationService/RegistrationService?wsdl",
        "register_user": "https://earthexplorer.usgs.gov/register",
        "earthexplorer": "https://earthexplorer.usgs.gov",
        "forgot_login": "https://earthexplorer.usgs.gov/login/username"
    },
    "ops": {
        "orderservice": "http://edclxs152.cr.usgs.gov/OrderWrapperService/resources",
        "orderdelivery": "http://edclxs152.cr.usgs.gov/OrderDeliveryService/OrderDeliveryService?WSDL",
        "orderupdate": "http://edclxs152.cr.usgs.gov/OrderStatusService/OrderStatusService?wsdl",
        "massloader": "http://edclxs152.cr.usgs.gov/MassLoader/MassLoader?wsdl",
        "registration": "http://edclxs152.cr.usgs.gov/RegistrationService/RegistrationService?wsdl",
        "register_user": "https://earthexplorer.usgs.gov/register",
        "earthexplorer": "https://earthexplorer.usgs.gov",
        "forgot_login": "https://earthexplorer.usgs.gov/login/username"
    }
}

# add the EE Authentication Backend in addition to the ModelBackend
# authentication stops at the first success... so this order does matter
#leave the standard ModelBackend in first so the builtin admin account
#never hits EE
AUTHENTICATION_BACKENDS = ('django.contrib.auth.backends.ModelBackend',
                           'espa_web.auth_backends.EEAuthBackend',)

# sets the login_url to the named url action ('login') contained in urls.py
LOGIN_URL = 'login'

# if the user didn't select a ?next parameter (will happen if they are trying
# to access /) then send them to the homepage
LOGIN_REDIRECT_URL = 'index'

# This is polluting the settings.py I know, but at the moment this is the
# best place for this since it is needed in lta.py and in context_processors.py
# ************t*************
# NEVER CHANGE THIS TO ops IN dev OR tst UNLESS THE dev AND tst CRONS ARE OFF
# *************************
URL_FOR = lambda service_name: SERVICE_LOCATOR[ESPA_ENV][service_name]

# Set up caching for Django.  Everything is pointed to our single memcache 
# cluster but each environment is going to separated out with the environment
# value as a key prefix.
if ESPA_ENV is 'dev':
    CACHES = {
    'default': {
        'KEY_PREFIX' : ESPA_ENV,
        'BACKEND': 'django.core.cache.backends.memcached.MemcachedCache',
        'LOCATION': [
            'localhost:11211',
        ]
    }
}
else:    
    CACHES = {
        'default': {
            'KEY_PREFIX' : ESPA_ENV,
            'BACKEND': 'django.core.cache.backends.memcached.MemcachedCache',
            'LOCATION': [
                'l8srlscp20.cr.usgs.gov:11211',
                'l8srlscp21.cr.usgs.gov:11211',
                'l8srlscp22.cr.usgs.gov:11211',
            ]
        }
    }

# cache timeouts by usage (in seconds)
SYSTEM_MESSAGE_CACHE_TIMEOUT = 60


