#!/usr/bin/env python
'''****************************************************************************
FILE: file_not_found_summary.py

PURPOSE: Outputs a list of each email that committed a 404 offense, and lists
        the total number of times they committed the offense and the number
        of times per day.

PROJECT: Land Satellites Data System Science Research and Development (LSRD)
    at the USGS EROS

LICENSE TYPE: NASA Open Source Agreement Version 1.3

AUTHOR: ngenetzky@usgs.gov
****************************************************************************'''
import sys
import argparse
import datetime
import collections
import logging
import apache_log_helper as ApacheLog


def mapper_email_date(line):
    '''Returns user_email and date if it was a successful production order.'''
    # mapper is going to find all the lines we're
    # interested in and only return those in its output
    if not ApacheLog.is_production_order(line):
        return
    if not ApacheLog.is_404_request(line):
        return
    return (ApacheLog.get_user_email(line), ApacheLog.get_date(line))


def reducer(ordered_accum, next_tuple):
    '''Accumulate count per day and overall total.

    Precondition:
        next_tuple  has attribute '__getitem__'
        ordered_accum is dictionary
    Note: consider that next_tuple[0] is A, next_tuple[1] is B
    Postcondition:
        ordered_accum[A] is an OrderedDict.
        ordered_accum[A]['Total'] is incremented
        ordered_accum[A][B] is also incremented
    '''
    # reducer is going to perform aggregate calculation
    # on the output of the mapper.  It can do this
    # because it receives all the lines of the the list
    # as its input
    if next_tuple is None:
        return ordered_accum
    try:
        ordered_accum[next_tuple[0]]['Total'] += 1
    except KeyError:
        ordered_accum[next_tuple[0]] = collections.OrderedDict()
        ordered_accum[next_tuple[0]]['Total'] = 1
    try:
        ordered_accum[next_tuple[0]][next_tuple[1]] += 1
    except KeyError:
        ordered_accum[next_tuple[0]][next_tuple[1]] = 1
    return ordered_accum


def report(lines, start_date, end_date):
    '''Will compile a report that provides total offenses per user

    Precondition: lines are from an Apache formated log file
    Postcondition: returns a dictionary
        keys are user_emails
        values are ordered dictionaries
            The first item is the total count
            keys are dates in order that they appear in logfile
            values are number of occurrences
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    mapper = ApacheLog.timefilter_decorator(mapper_email_date,
                                            start_date, end_date)
    occurrences_per_email = {}
    email_date = map(mapper, lines)
    return reduce(reducer, email_date, occurrences_per_email)


def layout(data):
    '''Will compile a report that provides total offenses per user

    Precondition: data is a dictionary
        keys are user_emails
        values are ordered dictionaries
            The first item value is the total count
            After the first the key is the date and the values are
                number of occurrences on that particular date.
    Postcondition: returns string of each user_report separated by '\n'
        Each user_report contains total_offenses and user_email
        Each user_report also contains a per day offenses
        Report is sorted from most offenses to least offenses
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    sorted_num_of_offenses = sorted(data.iteritems(),
                                    key=lambda t: t[1]['Total'], reverse=True)
    final_report = []
    for item in sorted_num_of_offenses:
        # item[0] = email, item[1] = number of offense
        user_report = []
        total = item[1].popitem(last=False)[1]
        user_report.append(' '.join([str(total), item[0]]))
        for date, count in item[1].iteritems():
            user_report.append("\t {0} {1}"
                               .format(date, count))

        final_report.append('\n'.join(user_report))

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

