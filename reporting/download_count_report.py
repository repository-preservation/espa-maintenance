#!/usr/bin/env python
'''****************************************************************************
FILE: bytes_downloaded_report.py

PURPOSE: Outputs an integer that represents the number of bytes downloaded.

PROJECT: Land Satellites Data System Science Research and Development (LSRD)
    at the USGS EROS

LICENSE TYPE: NASA Open Source Agreement Version 1.3

AUTHOR: ngenetzky@usgs.gov

NOTES:

****************************************************************************'''
import sys
import mapreduce_logfile as mapred
import argparse


def parse_arguments():
    '''Parse argument, filter, default to filter='orders' '''
    desc = ('Outputs the bytes downloaded by successful requests')
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--ordertype', action='store',
                        dest='ordertype',
                        choices=['orders', 'dswe', 'burned_area'],
                        required=False, default='orders',
                        help='Which order type should be analyzed?')
    args = parser.parse_args()
    return args


def main(iterable, ordertype='orders'):
    if(ordertype not in ['orders', 'dswe', 'burned_area']):
        return ("{0} not in ordertype choices({1})"
                .format(ordertype, ['orders', 'dswe', 'burned_area']))

    filters = [mapred.is_successful_request]

    if(ordertype == 'orders'):
        filters.append(mapred.is_production_order)
    elif(ordertype == 'burned_area'):
        filters.append(mapred.is_burned_area_order)
    elif(ordertype == 'dswe'):
        filters.append(mapred.is_dswe_order)

    return mapred.report_requests(iterable, filters=filters)


if __name__ == '__main__':
    args = parse_arguments()
    print(main(iterable=sys.stdin.readlines(),
               ordertype=args.ordertype))



