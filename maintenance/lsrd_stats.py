#!/usr/bin/env python
import re
import datetime
import calendar
import argparse
import subprocess
import traceback
import os
import json
from collections import Counter, defaultdict
import gzip
import sys

from dbconnect import DBConnect
import utils
import psycopg2.extras

REMOTE_LOG = ('/opt/cots/nginx/logs/archive/access.log-{}.gz'
              .format(datetime.datetime
                      .today()
                      .replace(day=1)
                      .strftime('%Y%m01')))

LOCAL_LOG = os.path.join(os.path.expanduser('~'), 'espa-site', 'logs',
                         '{}').format(REMOTE_LOG.split('/')[-1])

EMAIL_SUBJECT = 'LSRD ESPA Metrics for {0} to {1}'
ORDER_SOURCES = ('ee', 'espa')

SENSOR_KEYS = ('tm4', 'tm5', 'etm7', 'olitirs8', 'oli8',
               'mod09a1', 'mod09ga', 'mod09gq', 'mod09q1',
               'mod13a1', 'mod13a2', 'mod13a3', 'mod13q1',
               'myd09a1', 'myd09ga', 'myd09gq', 'myd09q1',
               'myd13a1', 'myd13a2', 'myd13a3', 'myd13q1', 'invalid')


def arg_parser():
    """
    Process the command line arguments
    """
    parser = argparse.ArgumentParser(description="LSRD ESPA Metrics")

    parser.add_argument('-c', '--cron', action='store_true',
                        help='Setup cron job to run the 1st of every month')
    parser.add_argument('-p', '--prev', action='store_true',
                        help='Run metrics for the previous month')
    parser.add_argument('-e', '--environment', dest='environment',
                        help='environment to run for: dev/tst/ops')

    args = parser.parse_args()

    return args


def setup_cron(env):
    """
    Setup cron job for the 1st of the month
    for the previous month's metrics

    :param env: dev/tst/ops
    """
    utils.backup_cron()

    cron_file = 'tmp'
    file_path = os.path.join(os.path.expanduser('~'), 'espa-site',
                             'maintenance', 'lsrd_stats.py')

    cron_str = ('00 06 1 * * /usr/local/bin/python {0} -p -e {1}'
                .format(file_path, env))

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
              ' {title}\n'
              '==========================================\n'
              ' Total Scenes: {total}\n'
              ' SR: {sr}\n'
              ' SR Thermal: {bt}\n'
              ' ToA: {toa}\n'
              ' Source: {customized_source_data}\n'
              ' Source Metadata: {source_metadata}\n'
              ' Customized Source: {l1}\n'
              ' SR EVI: {sr_evi}\n'
              ' SR MSAVI: {sr_msavi}\n'
              ' SR NBR: {sr_nbr}\n'
              ' SR NBR2: {sr_nbr2}\n'
              ' SR NDMI: {sr_ndmi}\n'
              ' SR NDVI: {sr_ndvi}\n'
              ' SR SAVI: {sr_savi}\n'
              ' CFMASK: {cloud}\n')

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
    sql = ('SELECT product_opts '
           'FROM ordering_order '
           'WHERE order_date::date >= %s '
           'AND order_date::date <= %s')

    init = {'total': 0}

    with DBConnect(**dbinfo) as db:
        db.select(sql, (begin_date, end_date))
        results = reduce(counts_prodopts, map(process_db_prodopts, db.fetcharr), init)

    results['title'] = 'What was Ordered'
    return results


def process_db_prodopts(row):
    ret = {'total': 0}
    opts = row[0]

    for key in SENSOR_KEYS:
        if key in opts:
            num = len(opts[key]['inputs'])
            ret['total'] += num

            prods = opts[key]['products']
            for prod in prods:
                if prod in ret:
                    ret[prod] += num
                else:
                    ret[prod] = num

    return ret


def counts_prodopts(*dicts):
    ret = defaultdict(int)
    for d in dicts:
        for k, v in d.items():
            ret[k] += v

    return dict(ret)


def db_dl_prodinfo(dbinfo, orders_scenes):
    """
    Queries the database to get the associated product options

    This query is meant to go with downloads by product

    :param dbinfo: Database connection information
    :type dbinfo: dict
    :param orders_scenes: Order id's that have been downloaded from
     based on web logs and scene names
    :type orders_scenes: tuple
    :return: Dictionary of count values
    """
    ids = zip(*orders_scenes)[0]
    ids = remove_duplicates(ids)

    sql = ('SELECT o.orderid, o.product_opts '
           'FROM ordering_order o '
           'WHERE o.orderid = ANY (%s)')

    with DBConnect(**dbinfo) as db:
        db.select(sql, (ids, ))
        results = {k: val for k, val in db.fetcharr}

    return results


def remove_duplicates(arr_obj):
    return list(set(arr_obj))


def tally_product_dls(orders_scenes, prod_options):
    """
    Counts the number of times a product has been downloaded

    :param orders_scenes: Order id's and scenes that have been
     downloaded from based on web logs, paired tuple
    :type orders_scenes: tuple
    :param prod_options: Unique order id's and their associated product
        options, dict keyed on order id
    :type prod_options: list
    :return: dictionary count
    """
    results = defaultdict(int)

    for orderid, scene in orders_scenes:
        opts = prod_options[orderid]
        for opt_key in opts:
            if opt_key in SENSOR_KEYS:
                inputs = opts[opt_key]['inputs']
                if [x for x in inputs if scene in x]:
                    for prod in opts[opt_key]['products']:
                        results[prod] += 1

                    results['total'] += 1

    return results


def calc_dlinfo(log_file, start_date, end_date):
    """
    Count the total tarballs downloaded from /orders/ and their combined size

    :param log_file: Combined Log Format file path
    :type log_file: str
    :return: Dictionary of values
    """
    infodict = {'tot_dl': 0,
                'tot_vol': 0.0}

    orders = []

    sd = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    ed = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

    with gzip.open(log_file) as log:
        for line in log:
            gr = filter_log_line(line, sd, ed)
            if gr:
                infodict['tot_vol'] += int(gr[8])
                infodict['tot_dl'] += 1
                orders.append(gr[5])

    # Bytes to GB
    infodict['tot_vol'] /= 1073741824.0

    return infodict, orders


def filter_log_line(line, start_date, end_date):
    """
    Used to determine if a line in the log should be used for metrics
    counting

    Filters to make sure the line follows a regex
    HTTP response is a 200
    HTTP method is a GET
    location is from /orders/
    falls within the date range

    :param line: incoming line from the log
    :param start_date: inclusive start date
    :param end_date: inclusive end date
    :return: regex groups returned from re.match
    """
    # (ip, logname, user, datetime, method, resource, status, size, referrer, agent)
    regex = r'(.*?) (.*?) (.*?) \[(.*?)\] "(.*?) (.*?) (.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'

    try:
        gr = re.match(regex, line).groups()
        ts, tz = gr[3].split()
        dt = datetime.datetime.strptime(ts, r'%d/%b/%Y:%H:%M:%S').date()

        if (gr[7] == '200' and
                gr[4] == 'GET' and
                '.tar.gz' in gr[5] and
                '/orders/' in gr[5] and
                start_date <= dt <= end_date):

            return gr
    except:
        return False


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
    receive = utils.get_config_value(dbinfo, 'email.stats_notification').split(',')
    sender = utils.get_config_value(dbinfo, 'email.espa_address').split(',')
    debug = utils.get_config_value(dbinfo, 'email.stats_debug').split(',')

    return receive, sender, debug


def extract_orderid(order_paths):
    '/orders/earthengine-landsat@google.com-11022015-210201/LT50310341990240-SC20151130234238.tar.gz'
    return tuple((x[2], x[3].split('-')[0])
                 for x in
                 [i.split('/') for i in order_paths])


def proc_prevmonth(cfg, env):
    """
    Put together metrics for the previous month then
    email the results out

    :param cfg: database connection info
    :type cfg: dict
    :param env: dev/tst/ops
    :type env: str
    """
    msg = ''
    receive, sender, debug = get_addresses(cfg)
    rng = date_range()
    subject = EMAIL_SUBJECT.format(rng[0], rng[1])

    try:
        # Fetch the web log
        if not os.path.exists(os.path.dirname(LOCAL_LOG)):
            os.makedirs(os.path.dirname(LOCAL_LOG))

        utils.fetch_web_log(cfg, REMOTE_LOG, LOCAL_LOG, env)

        # Process the web log file
        infodict, order_paths = calc_dlinfo(LOCAL_LOG, rng[0], rng[1])
        msg = download_boiler(infodict)

        # Downloads by Product
        orders_scenes = extract_orderid(order_paths)

        if not orders_scenes:
            raise ValueError

        prod_opts = db_dl_prodinfo(cfg, orders_scenes)
        infodict = tally_product_dls(orders_scenes, prod_opts)
        msg += prod_boiler(infodict)

        # On-Demand users and orders placed information
        for source in ORDER_SOURCES:
            infodict = db_orderstats(source, rng[0], rng[1], cfg)
            infodict.update(db_scenestats(source, rng[0], rng[1], cfg))
            infodict['tot_unique'] = db_uniquestats(source, rng[0], rng[1], cfg)
            infodict['who'] = source.upper()
            msg += ondemand_boiler(infodict)

        # Orders by Product
        infodict = db_prodinfo(cfg, rng[0], rng[1])
        msg += prod_boiler(infodict)

    except Exception:
        exc_msg = str(traceback.format_exc()) + '\n\n' + msg
        utils.send_email(sender, debug, subject, exc_msg)
        msg = ('There was an error with statistics processing.\n'
               'The following have been notified of the error: {0}.'
               .format(', '.join(debug)))
        raise
    finally:
        utils.send_email(sender, receive, subject, msg)

        if os.path.exists(LOCAL_LOG):
            os.remove(LOCAL_LOG)


def run():
    opts = arg_parser()
    cfg = utils.get_cfg()

    if not opts.environment:
        raise ValueError('You must set the -e variable')

    env = opts.environment

    if opts.cron:
        setup_cron(env)

    if opts.prev:
        proc_prevmonth(cfg['db'], env)


if __name__ == '__main__':
    run()
