#!/usr/bin/env python
'''****************************************************************************
FILE: apache_log_helper.py

PURPOSE: Used to extract data from logfile. 

PROJECT: Land Satellites Data System Science Research and Development (LSRD)
    at the USGS EROS

LICENSE TYPE: NASA Open Source Agreement Version 1.3

AUTHOR: ngenetzky@usgs.gov
****************************************************************************'''
import logging
import datetime
import urllib


def fail_to_parse(value, line):
    # Get the logger
    logger = logging.getLogger(__name__)
    logger.debug('Failed to parse for {0} in <\n{1}>'.format(value, line))
    return 'BAD_PARSE'


def substring_between(s, start, finish):
    '''Find string between two substrings'''
    end_of_start = s.index(start) + len(start)
    start_of_finish = s.index(finish, end_of_start)
    return s[end_of_start:start_of_finish]


def timefilter_decorator(mapper, start_date, end_date):
    '''Returns mapper than will first filter down to lines with dates in date range

    Precondition:
        start_date and end_date are datetime objects
        mapper is a function that accepts a single parameter, line.
    Postcondition:
        returns a mapper that will return None if date is not in range
    '''
    def new_mapper(line):
        dt = get_datetime(line)
        if(start_date <= dt and dt <= end_date):
            return mapper(line)
    return new_mapper


def get_datetime(line):
    try:
        time_local = substring_between(line, '[', '] "')
    except ValueError:
        return fail_to_parse('time_local', line)
    else:
        try:
            return datetime.datetime.strptime(time_local,
                                              '%d/%b/%Y:%H:%M:%S -0500')
        except ValueError:
            fail_to_parse('datetime', line)
            return datetime.datetime.max


def get_date(line):
    try:
        return get_datetime(line).date()
    except ValueError:
        fail_to_parse('date', line)


def get_bytes(line):
    '''Obtain number of downloaded bytes from a line of text

    Precondition: line is a ' ' separated list of data.
                Bytes downloaded is the second int in the items from 6 to 12.
    Postcondition: return bytes_downloaded
    '''
    data = line.split()
    if(data[9].isdigit()):
        return data[9]
    else:
        return fail_to_parse('downloaded_bytes', line)


def get_rtcode(line):
    '''Obtain a return_code from a line of text

    Precondition: line is a ' ' separated list of data.
                Return code is the first int in the items from 6 to 11.
    Postcondition: return return_code
    '''
    data = line.split()
    if(data[8].isdigit()):
        return data[8]
    else:
        return fail_to_parse('return_code', line)


def get_user_email(line):
    request = substring_between(line, '] "', '" ')
    request = urllib.unquote(request)
    try:
        return substring_between(request, 'orders/', '-')
    except ValueError:
        return fail_to_parse('user_email', line)


def get_email_category(line):
    email = get_user_email(line)
    if '.gov' in email:
        if('usgs.' in email):
            return 'usgs.gov'
        else:
            return 'not-usgs.gov'
    elif '.edu' in email:
        return '*.edu'
    else:
        return 'other'


def get_scene_id(line):
    try:
        response_after_orderid = substring_between(line, 'orders/', '" ')
        return substring_between(response_after_orderid, '/', '.tar.gz')
    except ValueError:
        try:
            response_after_orderid = substring_between(line, 'orders/', '" ')
            return substring_between(response_after_orderid, '/', '.cksum')
        except ValueError:
            return fail_to_parse('sceneid', line)


def get_order_id(line):
    request = substring_between(line, '] "', '" ')
    try:
        return substring_between(request, 'orders/', '/')
    except ValueError:
        return fail_to_parse('orderid', line)


def is_successful_request(line):
    '''Extracts return code and then returns true if code indicates success'''
    return (get_rtcode(line) in ['200', '206'])


def is_production_order(line):
    return (('GET /orders/' in line) and ('.tar.gz' in line))


def is_404_request(line):
    '''Extracts return code and returns true if indicates file-not-found'''
    return (get_rtcode(line) in ['404'])

