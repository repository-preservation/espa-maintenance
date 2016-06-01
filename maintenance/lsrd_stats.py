#!/usr/bin/env python
import re
import datetime
import calendar
import argparse
import subprocess
import traceback
import os
import json
from collections import Counter
import gzip

from dbconnect import DBConnect
import utils
import psycopg2.extras

REMOTE_LOG = '/opt/cots/nginx/logs/archive/access.log-{}.gz'
LOCAL_LOG = os.path.join(os.path.expanduser('~'), 'espa-site', 'logs',
                         '{}')

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
              ' SR: {include_sr}\n'
              ' SR Thermal: {include_sr_thermal}\n'
              ' ToA: {include_sr_toa}\n'
              ' Source: {include_source_data}\n'
              ' Source Metadata: {include_source_metadata}\n'
              ' Customized Source: {include_customized_source_data}\n'
              ' SR EVI: {include_sr_evi}\n'
              ' SR MSAVI: {include_sr_msavi}\n'
              ' SR NBR: {include_sr_nbr}\n'
              ' SR NBR2: {include_sr_nbr2}\n'
              ' SR NDMI: {include_sr_ndmi}\n'
              ' SR NDVI: {include_sr_ndvi}\n'
              ' SR SAVI: {include_sr_savi}\n'
              ' CFMASK: {include_cfmask}\n')

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
              SUM(CASE WHEN o.product_options::json->>'include_cfmask' = 'true' THEN 1 ELSE 0 END) "include_cfmask",
              SUM(CASE WHEN o.product_options::json->>'include_customized_source_data' = 'true' THEN 1 ELSE 0 END) "include_customized_source_data",
              SUM(CASE WHEN o.product_options::json->>'include_sr_evi' = 'true' THEN 1 ELSE 0 END) "include_sr_evi",
              SUM(CASE WHEN o.product_options::json->>'include_source_metadata' = 'true' THEN 1 ELSE 0 END) "include_source_metadata",
              SUM(CASE WHEN o.product_options::json->>'include_sr_msavi' = 'true' THEN 1 ELSE 0 END) "include_sr_msavi",
              SUM(CASE WHEN o.product_options::json->>'include_sr_nbr' = 'true' THEN 1 ELSE 0 END) "include_sr_nbr",
              SUM(CASE WHEN o.product_options::json->>'include_sr_nbr2' = 'true' THEN 1 ELSE 0 END) "include_sr_nbr2",
              SUM(CASE WHEN o.product_options::json->>'include_sr_ndmi' = 'true' THEN 1 ELSE 0 END) "include_sr_ndmi",
              SUM(CASE WHEN o.product_options::json->>'include_sr_ndvi' = 'true' THEN 1 ELSE 0 END) "include_sr_ndvi",
              SUM(CASE WHEN o.product_options::json->>'include_sr_savi' = 'true' THEN 1 ELSE 0 END) "include_sr_savi",
              SUM(CASE WHEN o.product_options::json->>'include_source_data' = 'true' THEN 1 ELSE 0 END) "include_source_data",
              SUM(CASE WHEN o.product_options::json->>'include_sr' = 'true' THEN 1 ELSE 0 END) "include_sr",
              SUM(CASE WHEN o.product_options::json->>'include_sr_thermal' = 'true' THEN 1 ELSE 0 END) "include_sr_thermal",
              SUM(CASE WHEN o.product_options::json->>'include_sr_toa' = 'true' THEN 1 ELSE 0 END) "include_sr_toa"
              FROM ordering_order o
              JOIN ordering_scene s ON s.order_id = o.id
              WHERE LENGTH(o.product_options) > 0
              AND o.order_date::date >= %s
              AND o.order_date::date <= %s;''')

    with DBConnect(cursor_factory=psycopg2.extras.DictCursor, **dbinfo) as db:
        db.select(sql, (begin_date, end_date))
        results = dict(db[0])

    results['title'] = 'What was Ordered'
    return results


def db_dl_prodinfo(dbinfo, ids):
    """
    Queries the database to get the associated product options

    :param dbinfo: Database connection information
    :type dbinfo: dict
    :param ids: Order id's that have been downloaded from based on web logs
    :type ids: tuple
    :return: Dictionary of count values
    """
    ids = remove_duplicates(ids)

    sql = ('SELECT o.orderid, o.product_options '
           'FROM ordering_order o '
           'WHERE o.orderid = ANY (%s)')

    with DBConnect(**dbinfo) as db:
        db.select(sql, (ids, ))
        results = [x for x in db]

    return results


def remove_duplicates(arr_obj):
    return list(set(arr_obj))


def tally_product_dls(ids, prod_options):
    """
    Counts the number of times a product has been downloaded

    :param ids: Order id's that have been downloaded from based on web logs
    :type ids: tuple
    :param prod_options: Unique order id's and their associated product
        options in paired tuples
    :type prod_options: list
    :return: dictionary count
    """
    infodict = {'total': 0,
                'include_cfmask': 0,
                'include_customized_source_data': 0,
                'include_sr_evi': 0,
                'include_source_metadata': 0,
                'include_sr_msavi': 0,
                'include_sr_nbr': 0,
                'include_sr_nbr2': 0,
                'include_sr_ndmi': 0,
                'include_sr_ndvi': 0,
                'include_sr_savi': 0,
                'include_source_data': 0,
                'include_sr': 0,
                'include_sr_thermal': 0,
                'include_sr_toa': 0,
                'title': 'What was Downloaded'}

    counts = Counter(ids)

    # if len(counts) != len(prod_options):
    #     raise Exception('Length of unique order ids from the web log'
    #                     'does not match what was received from the'
    #                     'database')

    infodict['total'] = len(ids)

    for order_key, val in prod_options:
        opts = json.loads(val)
        for opt_key in infodict:
            if opts.get(opt_key):
                infodict[opt_key] += counts[order_key]

    return infodict


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
    return tuple(x[2] for x in [i.split('/') for i in order_paths])


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

    rlog = REMOTE_LOG.format(datetime.datetime
                             .today()
                             .replace(day=1)
                             .strftime('%Y%m01'))

    llog = LOCAL_LOG.format(rlog.split('/')[-1])

    try:
        # Fetch the web log
        if not os.path.exists(os.path.dirname(llog)):
            os.makedirs(os.path.dirname(llog))

        utils.fetch_web_log(cfg, rlog, llog, env)

        # Process the web log file
        infodict, order_paths = calc_dlinfo(llog, rng[0], rng[1])
        msg = download_boiler(infodict)

        # Downloads by Product
        orderids = extract_orderid(order_paths)
        prod_opts = db_dl_prodinfo(cfg, orderids)
        infodict = tally_product_dls(orderids, prod_opts)
        msg += prod_boiler(infodict)

        # On-Demand users and order placed information
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

        if os.path.exists(llog):
            os.remove(llog)


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
