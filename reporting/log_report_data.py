#!/usr/bin/env python
'''****************************************************************************
FILE: log_report_data.py

PURPOSE: Extracts data from each line in the Apache logfile into comma
        separated lists.

PROJECT: Land Satellites Data System Science Research and Development (LSRD)
    at the USGS EROS

LICENSE TYPE: NASA Open Source Agreement Version 1.3

AUTHOR: ngenetzky@usgs.gov
****************************************************************************'''
import sys
import logging
import argparse
import datetime
import apache_log_helper as ApacheLog


def mapper_data(line):
    '''Extracts values from a line of text into tuple

    Precondition: line is a ' ' separated list of data.
        Preconditions for the following functions must also
            be satisfied: get_user_email, get_bytes, get_order_id, get_scene_id
    Postcondition: return tuple where len(tuple)==6
    '''
    # mapper is going to find all the lines we're
    # interested in and only return those in its output

    # Filter lines to lines of interest
    if not ApacheLog.is_successful_request(line):
        return
    if not ApacheLog.is_production_order(line):
        return
    # Extract data
    remote_addr = line.split(' - ', 1)[0]
    dt = ApacheLog.get_datetime(line).isoformat()
    user_email = ApacheLog.get_user_email(line)
    bytes_sent = ApacheLog.get_bytes(line)
    orderid = ApacheLog.get_order_id(line)
    sceneid = ApacheLog.get_scene_id(line)
    return (dt, remote_addr, user_email, orderid, sceneid, bytes_sent)


def reducer(dictionary, next_tuple):
    '''Adds tuples to dictionary with first element as key'''
    # reducer is going to perform aggregate calculation
    # on the output of the mapper.  It can do this
    # because it receives all the lines of the the list
    # as its input
    if next_tuple is None:
        return dictionary
    dictionary[next_tuple[0]] = next_tuple[1:]
    return dictionary


def report(lines, start_date, end_date):
    '''Returns a dictionary of data extracted from logfile

    Precondition: lines are from an Apache formated log file
    Postcondition: returns a dictionary
        key is the datetime in iso format
        value is a list of data extracted from line
    '''
    logging.getLogger(__name__).debug('Using dates: {0} to {1}'
                                      .format(start_date.isoformat(),
                                              end_date.isoformat()))
    mapper = ApacheLog.timefilter_decorator(mapper_data, start_date, end_date)
    map_out = map(mapper, lines)
    return reduce(reducer, map_out, {})


def layout(data):
    '''Reports data from logfile in the form of comma separated lists

    Precondition: lines are from an Apache formated log file
    Postcondition: returns a string
        String contains user reports separated by "\n"
        Each user report contains comma separated values in this form:
        datetime,remote_addr,user_email,orderid,sceneid,bytes
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    report = []
    for k, v in data.iteritems():
        report.append('{0},{1}'.format(k, ','.join(v)))
    return ('\n'.join(report))


def isoformat_datetime(datetime_string):
    '''

    Supports: ISO-format variations with any level of time specified
        ISO-format with only year, month, day
        YearMonthDay with no spaces
    '''
    dt = None
    dt_formats = []
    if '.' in datetime_string:
        dt_formats.insert(0, '%Y-%m-%dT%H:%M:%S.%fZ')
    elif ':' in datetime_string:
        dt_formats.insert(0, '%Y-%m-%dT%H:%M:%S')
        dt_formats.insert(0, '%Y-%m-%dT%H:%M')
    elif 'T' in datetime_string:
        dt_formats.insert(0, '%Y-%m-%dT%H')
    else:
        dt_formats.insert(0, '%Y-%m-%d')

    for fmt in dt_formats:
        try:
            dt = datetime.datetime.strptime(datetime_string, fmt)
            break  # Parsed a valid datetime
        except:
            pass  # Parse failed, try the next one.
    if dt is None:  # All parses failed
        raise ValueError
    else:
        return dt


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.epilog = ('Datetime must be provided for in a subset of isoformat'
                     'min format:"YYYY-MM-DD" (missing elements are zeroed)'
                     ', max format:"YYYY-NM-DDTHH:MM:SS.SSSS"')
    parser.formatter_class = argparse.ArgumentDefaultsHelpFormatter

    parser.add_argument('-s', '--start_date', dest='start_date',
                        help='The start date for the date range filter',
                        required=False, default=datetime.datetime.min,
                        type=isoformat_datetime)
    parser.add_argument('-e', '--end_date', dest='end_date',
                        help='The end date for the date range filter',
                        required=False, default=datetime.datetime.max,
                        type=isoformat_datetime)
    return parser.parse_args()


def main(iterable, start_date=datetime.datetime.min,
         end_date=datetime.datetime.max):
    # Setup the default logger format and level. log to STDOUT
    logging.basicConfig(format=('%(asctime)s.%(msecs)03d %(process)d'
                                ' %(levelname)-8s'
                                ' %(filename)s:%(lineno)d:'
                                '%(funcName)s -- %(message)s'),
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO)
    return layout(report(iterable, start_date, end_date))


if __name__ == '__main__':
    args = parse_arguments()
    print(main(sys.stdin.readlines(),
               args.start_date, args.end_date))

