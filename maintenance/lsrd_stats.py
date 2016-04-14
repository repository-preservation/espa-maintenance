#!/usr/bin/env python
import re
import datetime
import calendar
import argparse
import subprocess
import traceback
import os

from dbconnect import DBConnect
from utils import get_cfg, send_email, backup_cron, get_email_addr
import psycopg2.extras

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

    cron_file = 'tmp'
    file_path = os.path.join(os.path.expanduser('~'), 'espa-site', 'maintenance', 'lsrd_stats.py')

    cron_str = '00 06 1 * * /usr/local/bin/python {0} -p'.format(file_path)

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

    with open(cron_file, 'w') as f:
        f.write('\n'.join(crons) + '\n')

    msg = subprocess.check_output(['crontab', cron_file])
    if 'errors' in msg:
        print('Error creating cron job')
    else:
        subprocess.call(['rm', cron_file])


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


def download_byproduct_boiler(info):
    boiler = ('\n==========================================\n'
              ' On-demand - Download Info by Product\n'
              '==========================================\n'
              ' Total Scenes: {total}\n'
              ' SR: {sr}\n'
              ' SR Thermal: {brightness temperature}\n'
              ' ToA: {toa}\n'
              ' Source: {original source data}\n'
              ' Source Metadata: {metadata}\n'
              ' Customized Source: {level 1}\n'
              ' SR EVI: {evi}\n'
              ' SR MSAVI: {msavi}\n'
              ' SR NBR: {nbr}\n'
              ' SR NBR2: {nbr2}\n'
              ' SR NDMI: {ndmi}\n'
              ' SR NDVI: {ndvi}\n'
              ' SR SAVI: {savi}\n'
              ' CFMASK: {cfmask}\n')

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
              ' Total scenes ordered in the month for {who} interface: {scenes_month}\n'
              ' Number of scenes ordered in the month (USGS) for {who} interface: {scenes_usgs}\n'
              ' Number of scenes ordered in the month (non-USGS) for {who} interface: {scenes_non}\n'
              ' Total orders placed in the month for {who} interface: {orders_month}\n'
              ' Number of total orders placed in the month (USGS) for {who} interface: {orders_usgs}\n'
              ' Number of total orders placed in the month (non-USGS) for {who} interface: {orders_non}\n'
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
              ' Total Scenes: {total}\n'
              ' SR: {sr}\n'
              ' SR Thermal: {brightness temperature}\n'
              ' ToA: {toa}\n'
              ' Source: {original source data}\n'
              ' Source Metadata: {metadata}\n'
              ' Customized Source: {level 1}\n'
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
    dates are given as ISO 8601 'YYYY-MM-DD'

    :param dbinfo: Database connection information
    :type dbinfo: dict
    :param begin_date: Date to start the counts on
    :type begin_date: str
    :param end_date: Date to end the counts on
    :type end_date: str
    :return: Dictionary of count values
    """
    sql = ('''SELECT COUNT(s.name) "total",
              SUM(CASE WHEN o.product_options::json->>'include_cfmask' = 'true' THEN 1 ELSE 0 END) "cfmask",
              SUM(CASE WHEN o.product_options::json->>'include_customized_source_data' = 'true' THEN 1 ELSE 0 END) "level 1",
              SUM(CASE WHEN o.product_options::json->>'include_sr_evi' = 'true' THEN 1 ELSE 0 END) "evi",
              SUM(CASE WHEN o.product_options::json->>'include_source_metadata' = 'true' THEN 1 ELSE 0 END) "metadata",
              SUM(CASE WHEN o.product_options::json->>'include_sr_msavi' = 'true' THEN 1 ELSE 0 END) "msavi",
              SUM(CASE WHEN o.product_options::json->>'include_sr_nbr' = 'true' THEN 1 ELSE 0 END) "nbr",
              SUM(CASE WHEN o.product_options::json->>'include_sr_nbr2' = 'true' THEN 1 ELSE 0 END) "nbr2",
              SUM(CASE WHEN o.product_options::json->>'include_sr_ndmi' = 'true' THEN 1 ELSE 0 END) "ndmi",
              SUM(CASE WHEN o.product_options::json->>'include_sr_ndvi' = 'true' THEN 1 ELSE 0 END) "ndvi",
              SUM(CASE WHEN o.product_options::json->>'include_sr_savi' = 'true' THEN 1 ELSE 0 END) "savi",
              SUM(CASE WHEN o.product_options::json->>'include_source_data' = 'true' THEN 1 ELSE 0 END) "original source data",
              SUM(CASE WHEN o.product_options::json->>'include_sr' = 'true' THEN 1 ELSE 0 END) "sr",
              SUM(CASE WHEN o.product_options::json->>'include_sr_thermal' = 'true' THEN 1 ELSE 0 END) "brightness temperature",
              SUM(CASE WHEN o.product_options::json->>'include_sr_toa' = 'true' THEN 1 ELSE 0 END) "toa"
              FROM ordering_order o
              JOIN ordering_scene s ON s.order_id = o.id
              WHERE LENGTH(o.product_options) > 0
              AND o.order_date::date >= %s
              AND o.order_date::date <= %s;''')

    with DBConnect(cursor_factory=psycopg2.extras.DictCursor, **dbinfo) as db:
        db.select(sql, (begin_date, end_date))
        results = db[0]

    return results


def db_dl_prodinfo(dbinfo, orderinfo, begin_date, end_date):
    """
    Queries the database to build the product counts that were downloaded
    dates are given as ISO 8601 'YYYY-MM-DD'

    :param dbinfo: Database connection information
    :type dbinfo: dict
    :param begin_date: Date to start the counts on
    :type begin_date: str
    :param end_date: Date to end the counts on
    :type end_date: str
    :return: Dictionary of count values
    """
    ids, scenes = zip(*orderinfo)

    ids = remove_duplicates(ids)
    scenes = remove_duplicates(scenes)
    # scenes = add_wildcard(scenes)

    sql = ('''SELECT COUNT(s.name) "total",
              SUM(CASE WHEN o.product_options::json->>'include_cfmask' = 'true' THEN 1 ELSE 0 END) "cfmask",
              SUM(CASE WHEN o.product_options::json->>'include_customized_source_data' = 'true' THEN 1 ELSE 0 END) "level 1",
              SUM(CASE WHEN o.product_options::json->>'include_sr_evi' = 'true' THEN 1 ELSE 0 END) "evi",
              SUM(CASE WHEN o.product_options::json->>'include_source_metadata' = 'true' THEN 1 ELSE 0 END) "metadata",
              SUM(CASE WHEN o.product_options::json->>'include_sr_msavi' = 'true' THEN 1 ELSE 0 END) "msavi",
              SUM(CASE WHEN o.product_options::json->>'include_sr_nbr' = 'true' THEN 1 ELSE 0 END) "nbr",
              SUM(CASE WHEN o.product_options::json->>'include_sr_nbr2' = 'true' THEN 1 ELSE 0 END) "nbr2",
              SUM(CASE WHEN o.product_options::json->>'include_sr_ndmi' = 'true' THEN 1 ELSE 0 END) "ndmi",
              SUM(CASE WHEN o.product_options::json->>'include_sr_ndvi' = 'true' THEN 1 ELSE 0 END) "ndvi",
              SUM(CASE WHEN o.product_options::json->>'include_sr_savi' = 'true' THEN 1 ELSE 0 END) "savi",
              SUM(CASE WHEN o.product_options::json->>'include_source_data' = 'true' THEN 1 ELSE 0 END) "original source data",
              SUM(CASE WHEN o.product_options::json->>'include_sr' = 'true' THEN 1 ELSE 0 END) "sr",
              SUM(CASE WHEN o.product_options::json->>'include_sr_thermal' = 'true' THEN 1 ELSE 0 END) "brightness temperature",
              SUM(CASE WHEN o.product_options::json->>'include_sr_toa' = 'true' THEN 1 ELSE 0 END) "toa"
              FROM ordering_order o
              JOIN ordering_scene s ON s.order_id = o.id
              WHERE LENGTH(o.product_options) > 0
              AND o.orderid = ANY (%s)
              AND s.name ~* %s
              AND o.order_date::date >= %s
              AND o.order_date::date <= %s;''')

    with DBConnect(cursor_factory=psycopg2.extras.DictCursor, **dbinfo) as db:
        db.select(sql, (ids, '|'.join(scenes), begin_date, end_date))
        results = db[0]

    return results


def remove_duplicates(arr_obj):
    return list(set(arr_obj))


def add_wildcard(ids):
    return [s + '%' for s in ids]


def calc_dlinfo(log_file):
    """
    Count the total tarballs downloaded from /orders/ and their combined size

    :param log_file: Combined Log Format file path
    :type log_file: str
    :return: Dictionary of values
    """
    infodict = {'tot_dl': 0,
                'tot_vol': 0.0}

    orders = []

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
                    orders.append(gr[5])
            except:
                continue

    # Bytes to GB
    infodict['tot_vol'] /= 1073741824.0

    return infodict, orders


def db_scenestats(source, begin_date, end_date, dbinfo):
    """
    Queries the database for the number of scenes ordered
    separated by USGS and non-USGS emails
    dates are given as ISO 8601 'YYYY-MM-DD'

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
              where ordering_order.order_date::date >= %s
              and ordering_order.order_date::date <= %s
              and ordering_order.orderid like '%%@usgs.gov-%%'
              and ordering_order.order_source = %s;''',
           '''select COUNT(*)
              from ordering_scene
              inner join ordering_order on ordering_scene.order_id = ordering_order.id
              where ordering_order.order_date::date >= %s
              and ordering_order.order_date::date <= %s
              and ordering_order.orderid not like '%%@usgs.gov-%%'
              and ordering_order.order_source = %s;''')

    counts = {'scenes_month': 0,
              'scenes_usgs': 0,
              'scenes_non': 0}

    with DBConnect(**dbinfo) as db:
        for q in sql:
            db.select(q, (begin_date, end_date, source))

            if 'not like' in q:
                counts['scenes_non'] += int(db[0][0])
            else:
                counts['scenes_usgs'] += int(db[0][0])

    counts['scenes_month'] = counts['scenes_usgs'] + counts['scenes_non']

    return counts


def db_orderstats(source, begin_date, end_date, dbinfo):
    """
    Queries the database to get the total number of orders
    separated by USGS and non-USGS emails
    dates are given as ISO 8601 'YYYY-MM-DD'

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
              where order_date::date >= %s
              and order_date::date <= %s
              and orderid like '%%@usgs.gov-%%'
              and order_source = %s;''',
           '''select COUNT(*)
              from ordering_order
              where order_date::date >= %s
              and order_date::date <= %s
              and orderid not like '%%@usgs.gov-%%'
              and order_source = %s;''')

    counts = {'orders_month': 0,
              'orders_usgs': 0,
              'orders_non': 0}

    with DBConnect(**dbinfo) as db:
        for q in sql:
            db.select(q, (begin_date, end_date, source))

            if 'not like' in q:
                counts['orders_non'] += int(db[0][0])
            else:
                counts['orders_usgs'] += int(db[0][0])

    counts['orders_month'] = counts['orders_usgs'] + counts['orders_non']

    return counts


def db_uniquestats(source, begin_date, end_date, dbinfo):
    """
    Queries the database to get the total number of unique users
    dates are given as ISO 8601 'YYYY-MM-DD'

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
             where order_date::date >= %s
             and order_date::date <= %s
             and order_source = %s;'''

    with DBConnect(**dbinfo) as db:
        db.select(sql, (begin_date, end_date, source))
        return db[0][0]

def date_range():
    """
    Builds two strings for the 1st and last day of
    the previous month, ISO 8601 'YYYY-MM-DD'

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


def extract_orderid(order_paths):
    return tuple((x[2], x[3].split('-')[0]) for
                 x in [i.split('/') for i in order_paths])


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
        infodict, order_paths = calc_dlinfo(LOG_FILE)
        msg = download_boiler(infodict)

        orderids = extract_orderid(order_paths)
        infodict = db_dl_prodinfo(cfg, orderids, rng[0], rng[1])
        msg += download_byproduct_boiler(infodict)

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
