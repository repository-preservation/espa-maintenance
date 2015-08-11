#!/usr/bin/env python
'''****************************************************************************
FILE: file_not_found_report.py

PURPOSE: Produce a report of requests with a 404 return code from an Apache log
file. Shows number of occurrences per email, and optionally per day as well.

PROJECT: Land Satellites Data System Science Research and Development (LSRD)
    at the USGS EROS

LICENSE TYPE: NASA Open Source Agreement Version 1.3

AUTHOR: ngenetzky@usgs.gov

****************************************************************************'''
import sys
import argparse
import mapreduce_logfile as mapred


def parse_arguments():
    desc = ('Produce a report of requests with a 404 return code from an'
            ' Apache log file. Shows number of occurrences per email, and'
            ' optionally per day as well.')
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--show_per_day', dest='show_per_day',
                        action='store_true', default=False,
                        help='List offenses per day?')
    args = parser.parse_args()
    return args


def main(iterable, show_per_day=False):
    if(show_per_day):
        return mapred.report_404_perdate_peremail_on_production_orders(iterable)
    else:
        return mapred.report_404_per_user_email_on_production_orders(iterable)


if __name__ == '__main__':
    args = parse_arguments()
    print(main(iterable=sys.stdin.readlines(),
               show_per_day=args.show_per_day))

