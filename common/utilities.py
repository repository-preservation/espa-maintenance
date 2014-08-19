
'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Utility module for ESPA project.
  This is a shared module to hold simple utility functions.

History:
  Original implementation by David V. Hill, USGS/EROS
  Updated Jan/2014 by Ron Dilley, USGS/EROS
'''

import os
import datetime
import commands
import random
import urllib2
import xmlrpclib
import re
from email.mime.text import MIMEText
from smtplib import SMTP

# local objects and methods
import settings


def scenecache_is_alive(url='http://edclpdsftp.cr.usgs.gov:50000/RPC2'):
    """Determine if the specified url has an http server
    that accepts POST calls

    Keyword args:
    url -- The url of the server to check

    Return:
    True -- If the contacted server is alive and accepts POST calls
    False -- If the server does not accept POST calls or the
             server could not be contacted
    """

    try:
        return urllib2.urlopen(url, data="").getcode() == 200
    except Exception, e:
        if settings.DEBUG:
            print("Scene cache could not be contacted")
            print(e)
        return False


def scenecache_client():
    """Return an xmlrpc proxy to the caller for the scene cache

    Returns -- An xmlrpclib ServerProxy object
    """
    url = 'http://edclpdsftp.cr.usgs.gov:50000/RPC2'
    # url = os.environ['ESPA_SCENECACHE_URL']
    if scenecache_is_alive(url):
        return xmlrpclib.ServerProxy(url)
    else:
        msg = "Could not contact scene_cache at %s" % url
        raise RuntimeError(msg)


def date_from_doy(year, doy):
    '''Returns a python date object given a year and day of year'''

    d = datetime.datetime(int(year), 1, 1) + datetime.timedelta(int(doy) - 1)

    if int(d.year) != int(year):
        raise Exception("doy [%s] must fall within the specified year [%s]" %
                        (doy, year))
    else:
        return d


def is_number(s):
    '''Determines if a string value is a float or int.

    Keyword args:
    s -- A string possibly containing a float or int

    Return:
    True if s is a float or int
    False if s is not a float or int
    '''
    try:
        float(s)
        return True
    except ValueError:
        return False


# ============================================================================
def get_logfile(orderid, sceneid):
    '''
    Description:
      Returns the full path and name of the log file to use
    '''

    return '%s/%s-%s-jobdebug.txt' % (settings.LOGFILE_PATH, orderid, sceneid)


# ============================================================================
def execute_cmd(cmd):
    '''
    Description:
      Execute a command line and return the terminal output or raise an
      exception

    Returns:
        output - The stdout and/or stderr from the executed command.
    '''

    output = ''

    (status, output) = commands.getstatusoutput(cmd)

    if status < 0:
        message = "Application terminated by signal [%s]" % cmd
        if len(output) > 0:
            message = ' Stdout/Stderr is: '.join([message, output])
        raise Exception(message)

    if status != 0:
        message = "Application failed to execute [%s]" % cmd
        if len(output) > 0:
            message = ' Stdout/Stderr is: '.join([message, output])
        raise Exception(message)

    if os.WEXITSTATUS(status) != 0:
        message = "Application [%s] returned error code [%d]" \
                  % (cmd, os.WEXITSTATUS(status))
        if len(output) > 0:
            message = ' Stdout/Stderr is: '.join([message, output])
        raise Exception(message)

    return output


# ============================================================================
def strip_zeros(value):
    '''
    Description:
      Removes all leading zeros from a string
    '''

    while value.startswith('0'):
        value = value[1:len(value)]
    return value


# ============================================================================
def get_cache_hostname():
    '''
    Description:
      Poor mans load balancer for accessing the online cache over the private
      network
    '''

    # 140 is here twice so the load is 2/3 + 1/3.  machines are mismatched
    host_list = settings.ESPA_CACHE_HOST_LIST

    def check_host_status(hostname):
        cmd = "ping -q -c 1 %s" % hostname
        output = ''
        try:
            output = execute_cmd(cmd)
        except Exception, e:
            return -1
        return 0

    def get_hostname():
        hostname = random.choice(host_list)
        if check_host_status(hostname) == 0:
            return hostname
        else:
            for x in host_list:
                if x == hostname:
                    host_list.remove(x)
            if len(host_list) > 0:
                return get_hostname()
            else:
                raise Exception("No online cache hosts available...")

    return get_hostname()


def send_email(recipient, subject, body):
    '''Sends an email to a receipient on the behalf of espa'''

    if not validate_email(recipient):
        raise TypeError("Invalid email address provided:%s" % recipient)

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['To'] = recipient
    msg['From'] = settings.ESPA_EMAIL_ADDRESS
    s = SMTP(host=settings.ESPA_EMAIL_SERVER)
    s.sendmail(msg['From'], msg['To'], msg.as_string())
    s.quit()


def validate_email(email):
    '''Compares incoming email address against regular expression to make sure
    its at least formatted like an email

    Keyword args:
    email -- String to validate as an email address

    Return:
    True if the string is a properly formatted email address
    False if not
    '''
    pattern = '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}$'
    return re.match(pattern, email.strip())
