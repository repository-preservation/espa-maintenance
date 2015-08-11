#!/usr/bin/env python
'''****************************************************************************
FILE: download_count_report.py

PURPOSE: Outputs an integer that represents the number of bytes downloaded.

PROJECT: Land Satellites Data System Science Research and Development (LSRD)
    at the USGS EROS

LICENSE TYPE: NASA Open Source Agreement Version 1.3

AUTHOR: ngenetzky@usgs.gov
****************************************************************************'''
import sys


def get_rtcode(line):
    '''Obtain a return_code from a line of text

    Precondition: line is a ' ' separated list of data.
                Return code is the first int in the items from 6 to 11.
    Postcondition: return return_code
    '''
    data = line.split()
    if(data[8].isdigit()):
        return data[8]


def get_bytes(line):
    '''Obtain a return_code from a line of text

    Precondition: line is a ' ' separated list of data.
                Bytes downloaded is the second int in the items from 6 to 12.
    Postcondition: return bytes_downloaded
    '''
    data = line.split()
    if(data[9].isdigit()):
        return data[9]


def is_successful_request(line):
    return (get_rtcode(line) in ['200', '206'])


def is_production_order(line):
    return (('"GET /orders/' in line) and ('.tar.gz' in line))


def mapper(line):
    '''Returns 1 if it was a successful production order.'''
    # mapper is going to find all the lines we're
    # interested in and only return those in its output
    if not is_successful_request(line):
        return 0
    if not is_production_order(line):
        return 0
    return 1


def reducer(accum, map_out):
    '''Accumulates, via addition, the value produced by the mapper'''
    # reducer is going to perform aggregate calculation
    # on the output of the mapper.  It can do this
    # because it receives all the lines of the the list
    # as its input
    return accum + map_out


def report(lines):
    map_out = map(mapper, lines)
    reduce_out = reduce(reducer, map_out, 0)
    return reduce_out


def layout(data):
    return ('Total number of ordered scenes downloaded through ESPA order'
            ' interface order links: {0}\n'.format(data))


def main(iterable):
    return layout(report(iterable))


if __name__ == '__main__':
    print(main(sys.stdin.readlines()))

