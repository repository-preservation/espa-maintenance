
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

# local objects and methods
import settings


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


def execute_cmd(cmd):
    '''
    Description:
      Execute a command line and return the terminal output or raise an
      exception

    Returnsdsflh01.cr.usgs.gov
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


def strip_zeros(value):
    '''
    Description:
      Removes all leading zeros from a string
    '''

    while value.startswith('0'):
        value = value[1:len(value)]
    return value


def get_cache_hostname():
    '''
    Description:
      Poor mans load balancer for accessing the online cache over the private
      network
    '''

    host_list = settings.ESPA_CACHE_HOST_LIST

    def check_host_status(hostname):
        cmd = "ping -q -c 1 %s" % hostname

        try:
            execute_cmd(cmd)
        except Exception:
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


def tar_files(tarred_full_path, file_list, gzip=False):
    '''
    Description:
      Create a tar ball (*.tar) of the specified file(s).
      OR
      Create a tar.gz ball (*.tar.gz) of the specified file(s).
    '''

    flags = '-cf'
    target = '%s.tar' % tarred_full_path

    # If zipping was chosen, change the flags and the target name
    if gzip:
        flags = '-czf'
        target = '%s.tar.gz' % tarred_full_path

    cmd = ['tar', flags, target]
    cmd.extend(file_list)
    cmd = ' '.join(cmd)

    output = ''
    try:
        output = execute_cmd(cmd)
    except Exception:
        msg = "Error encountered tar'ing file(s): Stdout/Stderr:"
        if len(output) > 0:
            msg = ' '.join([msg, output])
        else:
            msg = ' '.join([msg, "NO STDOUT/STDERR"])
        # Raise and retain the callstack
        raise Exception(msg)

    return target


def gzip_files(file_list):
    '''
    Description:
      Create a gzip for each of the specified file(s).
    '''

    # Force the gzip file to overwrite any previously existing attempt
    cmd = ['gzip', '--force']
    cmd.extend(file_list)
    cmd = ' '.join(cmd)

    output = ''
    try:
        output = execute_cmd(cmd)
    except Exception:
        msg = "Error encountered compressing file(s): Stdout/Stderr:"
        if len(output) > 0:
            msg = ' '.join([msg, output])
        else:
            msg = ' '.join([msg, "NO STDOUT/STDERR"])
        # Raise and retain the callstack
        raise Exception(msg)


def checksum_local_file(filename):
    '''
    Description:
      Create a checksum for the specified file.
    '''

    cmd = ' '.join(['cksum', filename])

    cksum_result = ''
    try:
        cksum_result = execute_cmd(cmd)
    except Exception:
        msg = "Error encountered generating checksum: Stdout/Stderr:"
        if len(cksum_result) > 0:
            msg = ' '.join([msg, cksum_result])
        else:
            msg = ' '.join([msg, "NO STDOUT/STDERR"])
        # Raise and retain the callstack
        raise Exception(msg)

    return cksum_result
