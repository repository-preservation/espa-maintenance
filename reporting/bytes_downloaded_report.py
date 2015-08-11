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
from datetime import datetime


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y_%m_%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def parse_arguments():
    '''Parse argument, filter, default to filter='orders' '''
    desc = ('Outputs the bytes downloaded by successful requests')

    today = datetime.now()
    today.strftime("%Y_%m_%d")

    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--ordertype', action='store',
                        dest='ordertype',
                        choices=['orders', 'dswe', 'burned_area'],
                        required=False, default='orders',
                        help='Which order type should be analyzed?')

    parser.add_argument('-d', "--date_range", dest='date_range',
                        help="The Date Range, Start_date end_date,"
                             " format YYYY_MM_DD YYYY_MM_DD",
                        required=False, nargs=2, type=valid_date)

    args = parser.parse_args()
    return args


def main(iterable, ordertype='orders', date_range=None):
    if(ordertype not in ['orders', 'dswe', 'burned_area']):
        return ("{0} not in ordertype choices({1})"
                .format(ordertype, ['orders', 'dswe', 'burned_area']))

    filters = [mapred.is_successful_request]

    if(date_range is None):
        # Default, could filter by this month
        pass
    else:
        filters.append(lambda line:
                       mapred.is_within_daterange(line, date_range[0],
                                                  date_range[1]))

    # Filter by ordertype
    if(ordertype == 'orders'):
        filters.append(mapred.is_production_order)
    elif(ordertype == 'burned_area'):
        filters.append(mapred.is_burned_area_order)
    elif(ordertype == 'dswe'):
        filters.append(mapred.is_dswe_order)

    return mapred.report_bytes(iterable, filters=filters)


if __name__ == '__main__':
    args = parse_arguments()
    print(main(iterable=sys.stdin.readlines(),
               ordertype=args.ordertype,
               date_range=args.date_range))

