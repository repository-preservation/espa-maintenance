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
import datetime
import urllib


def substring_between(s, start, finish):
    '''Find string between two substrings'''
    end_of_start = s.index(start) + len(start)
    start_of_finish = s.index(finish, end_of_start)
    return s[end_of_start:start_of_finish]


def get_rtcode(line):
    '''Obtain a return_code from a line of text

    Precondition: line is a ' ' separated list of data.
                Return code is the first int in the items from 6 to 11.
    Postcondition: return return_code
    '''
    data = line.split()
    if(data[8].isdigit()):
        return data[8]


def get_user_email(line):
    request = substring_between(line, '] "', '" ')
    request = urllib.unquote(request)
    try:
        return substring_between(request, 'orders/', '-')
    except ValueError:
        return 'BAD_PARSE'


def is_404_request(line):
    return (get_rtcode(line) in ['404'])


def is_production_order(line):
    return (('"GET /orders/' in line) and ('.tar.gz' in line))


def mapper(line):
    '''Returns 1 if it was a successful production order.'''
    # mapper is going to find all the lines we're
    # interested in and only return those in its output
    if not is_production_order(line):
        return
    if not is_404_request(line):
        return
    return get_user_email(line)


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


def report(lines):
    occurrences_per_email = {}
    email_date = map(mapper, lines)
    return reduce(reducer, email_date, occurrences_per_email)


def layout(data):
    sorted_num_of_offenses = sorted(data.iteritems(),
                                    key=lambda (k, v): v,
                                    reverse=True)
    final_report = []
    for item in sorted_num_of_offenses:
        # item[0] = email, item[1] = number of offenses
        final_report.append('{1} {0}'.format(item[0], item[1]))

    return '\n'.join(final_report)


def main(iterable):
    return layout(report(iterable))


if __name__ == '__main__':
    print(main(sys.stdin.readlines()))

