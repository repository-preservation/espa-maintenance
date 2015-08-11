#!/usr/bin/env python
'''****************************************************************************
FILE: csv_report.py

PURPOSE: Outputs a comma separated list that contains information extracted
    from an Apache log file in the format:
    datetime,remote_addr,user_email,orderid,sceneid,bytes_sent

PROJECT: Land Satellites Data System Science Research and Development (LSRD)
    at the USGS EROS

LICENSE TYPE: NASA Open Source Agreement Version 1.3

AUTHOR: ngenetzky@usgs.gov

****************************************************************************'''
import sys
import mapreduce_logfile as mapred


def main(iterable):
    '''Will read, extract data, and output line by line.'''
    return mapred.report_csv(iterable=iterable,
                             filters=[mapred.is_successful_request])


if __name__ == '__main__':
    print(main(iterable=sys.stdin.readlines()))

