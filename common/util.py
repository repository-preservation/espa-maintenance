
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

import datetime
import subprocess
import random

# local objects and methods
import settings



def date_from_doy(year, doy):
    '''Returns a python date object given a year and day of year'''
    
    date = datetime.datetime(int(year), 1, 1) + datetime.timedelta(int(doy) - 1)
    
    if int(date.year) != int(year):
        raise Exception("doy [%s] must fall within the specified year [%s]" %
        (doy, year))
    else:
        return date
    
    
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
      Execute a command line and return SUCCESS or ERROR

    Returns:
        output - The stdout and/or stderr from the executed command.
    '''

    output = ''
    proc = None
    try:
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, shell=True)
        output = proc.communicate()[0]

        if proc.returncode < 0:
            message = "Application terminated by signal [%s]" % cmd
            raise Exception(message)

        if proc.returncode != 0:
            message = "Application failed to execute [%s]" % cmd
            raise Exception(message)

        application_exitcode = proc.returncode >> 8
        if application_exitcode != 0:
            message = "Application [%s] returned error code [%d]" \
                      % (cmd, application_exitcode)
            raise Exception(message)

    finally:
        del proc

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
