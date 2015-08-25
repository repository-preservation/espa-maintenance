#!/usr/bin/env python
'''****************************************************************************
FILE: bytes_downloaded_report.py

PURPOSE: Outputs an integer that represents the number of bytes downloaded.

PROJECT: Land Satellites Data System Science Research and Development (LSRD)
    at the USGS EROS

LICENSE TYPE: NASA Open Source Agreement Version 1.3

AUTHOR: ngenetzky@usgs.gov
****************************************************************************'''
import sys
import math
import logging
import datetime
import argparse
import apache_log_helper as ApacheLog


def convert_bytes(size):
    '''Convert bytes to proper units

    Precondition: size is in int(bytes)
    Postcondition: Output is in form of 'INTEGER STRING" where
            string contains unit label and integer is the value for those units

    Source: http://stackoverflow.com/a/14822210
    '''
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    if (size > 0):
        i = int(math.floor(math.log(size, 1024)))
        p = math.pow(1024, i)
        s = round(size/p, 2)
        return '%s %s' % (s, size_name[i])
    else:
        return '0 B'


def mapper_bytes(line):
    '''Returns bytes downloaded if it was a successful production order.'''
    # mapper is going to find all the lines we're
    # interested in and only return those in its output
    if not ApacheLog.is_successful_request(line):
        return 0
    if not ApacheLog.is_production_order(line):
        return 0
    return int(ApacheLog.get_bytes(line))


def reducer(accum, map_out):
    '''Accumulates, via addition, the value produced by the mapper'''
    # reducer is going to perform aggregate calculation
    # on the output of the mapper.  It can do this
    # because it receives all the lines of the the list
    # as its input
    if map_out is None:
        return accum
    return accum + map_out


def report(lines, start_date, end_date):
    '''Returns the number of bytes downloaded'''
    mapper = ApacheLog.timefilter_decorator(mapper_bytes, start_date, end_date)
    map_out = map(mapper, lines)
    reduce_out = reduce(reducer, map_out, 0)
    return reduce_out


def layout(data):
    gb = round(int(data)/math.pow(1024, 3), 2)
    return ('Total volume of ordered scenes downloaded (GB): {0}\n'
            .format(gb))


def layout2(data):
    labeled_data = convert_bytes(int(data))
    return ('Total volume of ordered scenes downloaded: {0}\n'
            .format(labeled_data))


def isoformat_datetime(datetime_string):
    '''Converts string of ISO-format variations with datetime object

    Precondition:
        Datetime_string must be provided a subset of isoformat anything from:
        "YYYY-MM-DD" to "YYYY-NM-DDTHH:MM:SS.SSSS"
    Postcondition:
        returns datetime.datetime object(missing elements are zeroed)
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

