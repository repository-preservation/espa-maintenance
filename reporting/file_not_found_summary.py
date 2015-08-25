#!/usr/bin/env python
'''****************************************************************************
FILE: file_not_found_summary.py

PURPOSE: Outputs a list of each email that committed a 404 offense, and lists
        the number of times they committed the offense.

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


def mapper(line):
    '''Returns user_email if it was a successful production order.'''
    # mapper is going to find all the lines we're
    # interested in and only return those in its output
    if not ApacheLog.is_production_order(line):
        return
    if not ApacheLog.is_404_request(line):
        return
    return ApacheLog.get_user_email(line)


def reducer(accum, map_out):
    '''Accumulates the occurrences of a value produced by the mapper'''
    # reducer is going to perform aggregate calculation
    # on the output of the mapper.  It can do this
    # because it receives all the lines of the the list
    # as its input
    if map_out is None:
        return accum
    try:
        accum[map_out] += 1
    except KeyError:
        accum[map_out] = 1
    return accum


def report(lines, start_date, end_date):
    '''Will compile a report that provides total offenses per user

    Precondition: lines are from an Apache formated log file
    Postcondition: returns a dictionary
        keys are user_emails
        values are number of occurrences
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    occurrences_per_email = {}
    email = map(mapper, lines)
    return reduce(reducer, email, occurrences_per_email)


def layout(data):
    '''Will compile a report that provides total offenses per user

    Precondition: data is a dictionary
        keys are user_emails
        values are number of occurrences
    Postcondition: returns string of each user_report separated by '\n'
        Each user_report contains total_offenses and user_email
        Report is sorted from most offenses to least offenses
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    sorted_num_of_offenses = sorted(data.iteritems(),
                                    key=lambda (k, v): v,
                                    reverse=True)
    final_report = []
    for item in sorted_num_of_offenses:
        # item[0] = email, item[1] = number of occurrences
        final_report.append('{1} {0}'.format(item[0], item[1]))
    return '\n'.join(final_report)


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

