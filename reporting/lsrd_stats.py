#!/usr/bin/env python
import re
import datetime
import calendar
import argparse
import os
import subprocess
import traceback

from dbconnect import DBConnect
from utils import get_cfg, send_email, backup_cron, get_email_addr


FILE_PATH = os.path.realpath(__file__)
LOG_FILE = '/data/logs/espa.cr.usgs.gov-access_log.1'

EMAIL_SUBJECT = 'LSRD ESPA Metrics for {0} to {1}'
ORDER_SOURCES = ('ee', 'espa')


def arg_parser():
    """
    Process the command line arguments
    """
    parser = argparse.ArgumentParser(description="LSRD ESPA Metrics")

    parser.add_argument('-c', '--cron', action='store_true',
                        help='Setup cron job to run the 1st of every month')
    parser.add_argument('-p', '--prev', action='store_true',
                        help='Run metrics for the previous month')

    # For future use
    # parser.add_argument('-d', '--daterange', action='store', dest='daterange', type=str,
    #                     help='Date range to process, begin - end: YYYY-MM-DD YYYY-MM-DD')

    args = parser.parse_args()

    return args


def setup_cron():
    """
    Setup cron job for the 1st of the month
    for the previous month's metrics
    """
    backup_cron()

    chron_file = 'tmp'

    cron_str = '00 06 1 * * /usr/local/bin/python {1} -p'.format(FILE_PATH)

    crons = subprocess.check_output(['crontab', '-l']).split('\n')

    for idx, line in enumerate(crons):
        if __file__ in line:
            print('Cron job already exists')
            return

    add = ['#-----------------------------',
           '# LSRD ESPA stats',
           '#-----------------------------',
           cron_str]

    crons.extend(add)

    with open(chron_file, 'w') as f:
        f.write('\n'.join(crons) + '\n')

    msg = subprocess.check_output(['crontab', chron_file, '&&', 'rm', chron_file])
    if 'errors' in msg:
        print('Error creating cron job')


def download_boiler(info):
    """
    Boiler plate text for On-Demand Info for downloads

    :param info: values to insert into the boiler plate
    :param info: dict
    :return: formatted string
    """
    boiler = ('\n==========================================\n'
              ' On-demand - Download Info\n'
              '==========================================\n'
              'Total number of ordered scenes downloaded through ESPA order interface order links: {tot_dl}\n'
              'Total volume of ordered scenes downloaded (GB): {tot_vol}\n')

    return boiler.format(**info)


def ondemand_boiler(info):
    """
    Boiler plate text for On-Demand Info for orders

    :param info: values to insert into the boiler plate
    :param info: dict
    :return: formatted string
    """
    boiler = ('\n==========================================\n'
              ' On-demand - {who}\n'
              '==========================================\n'
              ' Total scenes ordered in the month for {who} interface: {sc_month}\n'
              ' Number of scenes ordered in the month (USGS) for {who} interface: {sc_usgs}\n'
              ' Number of scenes ordered in the month (non-USGS) for {who} interface: {sc_non}\n'
              ' Total orders placed in the month for {who} interface: {or_month}\n'
              ' Number of total orders placed in the month (USGS) for {who} interface: {or_usgs}\n'
              ' Number of total orders placed in the month (non-USGS) for {who} interface: {or_non}\n'
              ' Total number of unique On-Demand users for {who} interface: {tot_unique}\n')

    return boiler.format(**info)


def prod_boiler(info):
    """
    Boiler plate text for On-Demand Info for products breakdown

    :param info: values to insert into the boiler plate
    :param info: dict
    :return: formatted string
    """
    boiler = ('\n==========================================\n'
              ' What Was Ordered\n'
              '==========================================\n'
              ' SR: {sr}\n'
              ' SR Thermal: {therm}\n'
              ' ToA: {toa}\n'
              ' Source: {source}\n'
              ' Source Metadata: {meta}\n'
              ' Customized Source: {custom}\n'
              ' SR EVI: {evi}\n'
              ' SR MSAVI: {msavi}\n'
              ' SR NBR: {nbr}\n'
              ' SR NBR2: {nbr2}\n'
              ' SR NDMI: {ndmi}\n'
              ' SR NDVI: {ndvi}\n'
              ' SR SAVI: {savi}\n'
              ' CFMASK: {cfmask}\n')

    return boiler.format(**info)


def db_prodinfo(dbinfo, begin_date, end_date):
    """
    Queries the database to build the ordered product counts
    dates are given as YYYY-MM-DD

    :param dbinfo: Database connection information
    :type dbinfo: dict
    :param begin_date: Date to start the counts on
    :type begin_date: str
    :param end_date: Date to end the counts on
    :type end_date: str
    :return: Dictionary of count values
    """

    # Alphabetical order according to dictionary key the value will go into
    sql = ('''SELECT COUNT(s.name),
              SUM(CASE WHEN o.product_options::json->>'include_cfmask' = 'true' THEN 1 ELSE 0 END),
              SUM(CASE WHEN o.product_options::json->>'include_customized_source_data' = 'true' THEN 1 ELSE 0 END),
              SUM(CASE WHEN o.product_options::json->>'include_sr_evi' = 'true' THEN 1 ELSE 0 END),
              SUM(CASE WHEN o.product_options::json->>'include_source_metadata' = 'true' THEN 1 ELSE 0 END),
              SUM(CASE WHEN o.product_options::json->>'include_sr_msavi' = 'true' THEN 1 ELSE 0 END),
              SUM(CASE WHEN o.product_options::json->>'include_sr_nbr' = 'true' THEN 1 ELSE 0 END),
              SUM(CASE WHEN o.product_options::json->>'include_sr_nbr2' = 'true' THEN 1 ELSE 0 END),
              SUM(CASE WHEN o.product_options::json->>'include_sr_ndmi' = 'true' THEN 1 ELSE 0 END),
              SUM(CASE WHEN o.product_options::json->>'include_sr_ndvi' = 'true' THEN 1 ELSE 0 END),
              SUM(CASE WHEN o.product_options::json->>'include_sr_savi' = 'true' THEN 1 ELSE 0 END),
              SUM(CASE WHEN o.product_options::json->>'include_source_data' = 'true' THEN 1 ELSE 0 END),
              SUM(CASE WHEN o.product_options::json->>'include_sr' = 'true' THEN 1 ELSE 0 END),
              SUM(CASE WHEN o.product_options::json->>'include_sr_thermal' = 'true' THEN 1 ELSE 0 END),
              SUM(CASE WHEN o.product_options::json->>'include_sr_toa' = 'true' THEN 1 ELSE 0 END)
              FROM ordering_order o
              JOIN ordering_scene s ON s.order_id = o.id
              WHERE LENGTH(o.product_options) > 0
              AND o.order_date >= %s
              AND o.order_date <= %s;''')

    infodict = {'sr': 0,
                'therm': 0,
                'toa': 0,
                'source': 0,
                'meta': 0,
                'custom': 0,
                'evi': 0,
                'msavi': 0,
                'nbr': 0,
                'nbr2': 0,
                'ndmi': 0,
                'ndvi': 0,
                'savi': 0,
                'cfmask': 0}

    with DBConnect(**dbinfo) as db:
        db.select(sql, (begin_date, end_date))
        for i, key in enumerate(sorted(infodict.keys())):
            infodict[key] = int(db[0][i])

    return infodict


def calc_dlinfo(log_file):
    """
    Count the total tarballs downloaded and their combined size

    :param log_file: Combined Log Format file path
    :type log_file: str
    :return: Dictionary of values
    """
    infodict = {'tot_dl': 0,
                'tot_vol': 0.0}

    # (ip, logname, user, datetime, method, resource, status, size, referrer, agent)
    regex = r'(.*?) (.*?) (.*?) \[(.*?)\] "(.*?) (.*?) (.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'

    with open(log_file, 'r') as log:
        for line in log:
            try:
                gr = re.match(regex, line).groups()
                # Kept simple for ease of reading and future use
                if gr[7] == '200' and gr[4] == 'GET' and '.tar.gz' in gr[5] and '/orders/' in gr[5]:
                    infodict['tot_vol'] += int(gr[8])
                    infodict['tot_dl'] += 1
            except:
                continue

    # Bytes to GB
    infodict['tot_vol'] /= 1073741824.0

    return infodict


def db_scenestats(source, begin_date, end_date, dbinfo):
    """
    Queries the database for the number of scenes ordered
    separated by USGS and non-USGS emails
    dates are given as YYYY-MM-DD

    :param source: EE or ESPA
    :type source: str
    :param begin_date: Date to start the count on
    :type begin_date: str
    :param end_date: Date to stop the count on
    :type end_date: str
    :param dbinfo: Database connection information
    :type dbinfo: dict
    :return: Dictionary of the counts
    """
    sql = ('''select COUNT(*)
              from ordering_scene
              inner join ordering_order on ordering_scene.order_id = ordering_order.id
              where ordering_order.order_date >= %s
              and ordering_order.order_date <= %s
              and ordering_order.orderid like '%%@usgs.gov-%%'
              and ordering_order.order_source = %s;''',
           '''select COUNT(*)
              from ordering_scene
              inner join ordering_order on ordering_scene.order_id = ordering_order.id
              where ordering_order.order_date >= %s
              and ordering_order.order_date <= %s
              and ordering_order.orderid not like '%%@usgs.gov-%%'
              and ordering_order.order_source = %s;''')

    results = {'sc_month': 0,
               'sc_usgs': 0,
               'sc_non': 0}

    with DBConnect(**dbinfo) as db:
        for q in sql:
            db.select(q, (begin_date, end_date, source))

            if 'not like' in q:
                results['sc_non'] += int(db[0][0])
            else:
                results['sc_usgs'] += int(db[0][0])

    results['sc_month'] = results['sc_usgs'] + results['sc_non']

    return results


def db_orderstats(source, begin_date, end_date, dbinfo):
    """
    Queries the database to get the total number of orders
    separated by USGS and non-USGS emails
    dates are given as YYYY-MM-DD

    :param source: EE or ESPA
    :type source: str
    :param begin_date: Date to start the count on
    :type begin_date: str
    :param end_date: Date to stop the count on
    :type end_date: str
    :param dbinfo: Database connection information
    :type dbinfo: dict
    :return: Dictionary of the counts
    """
    sql = ('''select COUNT(*)
              from ordering_order
              where order_date >= %s
              and order_date <= %s
              and orderid like '%%@usgs.gov-%%'
              and order_source = %s;''',
           '''select COUNT(*)
              from ordering_order
              where order_date >= %s
              and order_date <= %s
              and orderid not like '%%@usgs.gov-%%'
              and order_source = %s;''')

    results = {'or_month': 0,
               'or_usgs': 0,
               'or_non': 0}

    with DBConnect(**dbinfo) as db:
        for q in sql:
            db.select(q, (begin_date, end_date, source))

            if 'not like' in q:
                results['or_non'] += int(db[0][0])
            else:
                results['or_usgs'] += int(db[0][0])

    results['or_month'] = results['or_usgs'] + results['or_non']

    return results


def db_uniquestats(source, begin_date, end_date, dbinfo):
    """
    Queries the database to get the total number of unique users
    dates are given as YYYY-MM-DD

    :param source: EE or ESPA
    :type source: str
    :param begin_date: Date to start the count on
    :type begin_date: str
    :param end_date: Date to stop the count on
    :type end_date: str
    :param dbinfo: Database connection information
    :type dbinfo: dict
    :return: Dictionary of the count
    """
    sql = '''select count(distinct(split_part(orderid, '-', 1)))
             from ordering_order
             where order_date >= %s
             and order_date <= %s
             and order_source = %s;'''

    with DBConnect(**dbinfo) as db:
        db.select(sql, (begin_date, end_date, source))
        return db[0][0]

def date_range():
    """
    Builds two strings for the 1st and last day of
    the previous month, YYYY-MM-DD

    :return: 1st day, last day
    """
    first = datetime.datetime.today().replace(day=1)
    last = first - datetime.timedelta(days=2)

    num_days = calendar.monthrange(last.year, last.month)[1]

    begin_date = '{0}-{1}-1'.format(last.year, last.month)
    end_date = '{0}-{1}-{2}'.format(last.year, last.month, num_days)

    return begin_date, end_date


def get_addresses(dbinfo):
    """
    Retrieve the notification email address from the database

    :param dbinfo: connection information
    :type dbinfo: dict
    :return: list of recipients and the sender address
    """
    receive = get_email_addr(dbinfo, 'stats_notification')
    sender = get_email_addr(dbinfo, 'espa_address')
    debug = get_email_addr(dbinfo, 'stats_debug')

    return receive, sender, debug


# def proc_daterange(cfg, begin, end):
#     """
#     For future use when log filing better supports this
#     """
#     pass


def proc_prevmonth(cfg):
    """
    Put together metrics for the previous month then
    email the results out

    :param cfg: database connection info
    :type cfg: dict
    """
    msg = ''
    emails = get_addresses(cfg)
    rng = date_range()
    subject = EMAIL_SUBJECT.format(rng[0], rng[1])

    try:
        infodict = calc_dlinfo(LOG_FILE)
        msg = download_boiler(infodict)

        for source in ORDER_SOURCES:
            infodict = db_orderstats(source, rng[0], rng[1], cfg)
            infodict.update(db_scenestats(source, rng[0], rng[1], cfg))
            infodict['tot_unique'] = db_uniquestats(source, rng[0], rng[1], cfg)
            infodict['who'] = source.upper()
            msg += ondemand_boiler(infodict)

        infodict = db_prodinfo(cfg, rng[0], rng[1])
        msg += prod_boiler(infodict)

    except Exception:
        exc_msg = str(traceback.format_exc()) + '\n\n' + msg
        send_email(emails[1], emails[2], subject, exc_msg)
        msg = 'There was an error with statistics processing.\n' \
              'The following has been notified of the error: {0}.'.format(', '.join(emails[2]))
    finally:
        send_email(emails[1], emails[0], subject, msg)


def run():
    opts = arg_parser()
    cfg = get_cfg()['config']

    if opts.cron:
        setup_cron()

    if opts.prev:
        proc_prevmonth(cfg)


if __name__ == '__main__':
    run()
