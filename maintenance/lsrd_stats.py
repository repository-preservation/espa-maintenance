#!/usr/bin/env python
import re
import datetime
import calendar
import argparse
import subprocess
import traceback
import os
from collections import defaultdict
import gzip
import urllib2

from dbconnect import DBConnect
import utils

DATE_FMT = '%Y-%m-%d'
LOG_FILENAME = 'edclpdsftp.cr.usgs.gov-access_log-'
LOG_FILE_TIMESTAMP = LOG_FILENAME + '%Y%m%d' + '.gz'

EMAIL_SUBJECT = 'LSRD ESPA Metrics for {begin} to {stop}'
ORDER_SOURCES = ('ee', 'espa')

SENSOR_KEYS = ('tm4', 'tm5', 'etm7', 'olitirs8', 'oli8',
               'mod09a1', 'mod09ga', 'mod09gq', 'mod09q1',
               'mod13a1', 'mod13a2', 'mod13a3', 'mod13q1',
               'myd09a1', 'myd09ga', 'myd09gq', 'myd09q1',
               'myd13a1', 'myd13a2', 'myd13a3', 'myd13q1', 'invalid')


def arg_parser(defaults):
    """
    Process the command line arguments
    """
    parser = argparse.ArgumentParser(description="LSRD ESPA Metrics")

    parser.add_argument('-e', '--environment', dest='environment',
                        choices=['dev', 'tst', 'ops'], required=True,
                        help='environment to run for: dev/tst/ops')
    parser.add_argument('-b', '--begin', dest='begin',
                        default=defaults['begin'],
                        help='Start date to search (%s)' %
                        defaults['begin'].strftime(DATE_FMT))
    parser.add_argument('-s', '--stop', dest='stop',
                        default=defaults['stop'],
                        help='End date to search (%s)' %
                        defaults['stop'].strftime(DATE_FMT))
    parser.add_argument('-c', '--conf_file', dest='conf_file',
                        default=defaults['conf_file'],
                        help='Configuration file [%s]' % defaults['conf_file'])
    parser.add_argument('-r', '--remote', dest='remote',
                        default=defaults['remote'],
                        help='Directory structure of remote log location (%s)'
                        % defaults['remote'])
    parser.add_argument('-d', '--dir', dest='dir',
                        default=defaults['dir'],
                        help='Directory to temporarily store logs')
    parser.add_argument('-l', '--local', dest='local', action='store_true',
                        help='[DEBUG] For using a local file')  # FIXME remove

    args = parser.parse_args()
    defaults.update(args.__dict__)

    for _ in ['begin', 'stop']:
        if type(defaults[_]) is str:
            defaults[_] = datetime.datetime.strptime(defaults[_], DATE_FMT).date()

    return defaults


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
              ' Source: {l1}\n'
              ' Source Metadata: {source_metadata}\n'
              ' Customized Source: {customized_source_data}\n'
              ' SR EVI: {sr_evi}\n'
              ' SR MSAVI: {sr_msavi}\n'
              ' SR NBR: {sr_nbr}\n'
              ' SR NBR2: {sr_nbr2}\n'
              ' SR NDMI: {sr_ndmi}\n'
              ' SR NDVI: {sr_ndvi}\n'
              ' SR SAVI: {sr_savi}\n'
              ' CFMASK: {cloud}\n'
              ' Plot: {plot}\n')

    return boiler.format(title=info.get('title'),
                         total=info.get('total', 0),
                         sr=info.get('sr', 0),
                         bt=info.get('bt', 0),
                         toa=info.get('toa', 0),
                         customized_source_data=info.get('customized_source_data', 0),
                         source_metadata=info.get('source_metadata', 0),
                         l1=info.get('l1', 0),
                         sr_evi=info.get('sr_evi', 0),
                         sr_msavi=info.get('sr_msavi', 0),
                         sr_nbr=info.get('sr_nbr', 0),
                         sr_nbr2=info.get('sr_nbr2', 0),
                         sr_ndmi=info.get('sr_ndmi', 0),
                         sr_ndvi=info.get('sr_ndvi', 0),
                         sr_savi=info.get('sr_savi', 0),
                         cloud=info.get('cloud', 0),
                         plot=info.get('plot_statistics', 0))


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
    ret = defaultdict(int)
    # ret = {'total': 0}
    opts = row[0]

    for key in SENSOR_KEYS:
        if key in opts:
            num = len(opts[key]['inputs'])
            ret['total'] += num

            if 'plot_statistics' in opts and opts['plot_statistics']:
                ret['plot_statistics'] += num

            for prod in opts[key]['products']:
                if prod == 'l1' and ('projection' in opts or
                                     'image_extents' in opts):
                    ret['customized_source_data'] += num
                else:
                    ret[prod] += num

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
        opts = prod_options[urllib2.unquote(orderid)]

        if 'plot_statistics' in opts and opts['plot_statistics']:
            results['plot_statistics'] += 1

        for key in SENSOR_KEYS:
            if key in opts:
                # Scene names get truncated during distribution
                if [x for x in opts[key]['inputs'] if scene in x]:
                    results['total'] += 1

                    for prod in opts[key]['products']:
                        if prod == 'l1' and ('projection' in opts or
                                             'image_extents' in opts):
                            results['customized_source_data'] += 1
                        else:
                            results[prod] += 1

    results['title'] = 'Downloads by Product'
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
              and ordering_order.order_source = %s
              and name != 'plot' ''',
           '''select COUNT(*)
              from ordering_scene
              inner join ordering_order on ordering_scene.order_id = ordering_order.id
              where ordering_order.order_date::date >= %s
              and ordering_order.order_date::date <= %s
              and ordering_order.orderid not like '%%@usgs.gov-%%'
              and ordering_order.order_source = %s
              and name != 'plot' ''')

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


def date_range(offset=0):
    """
    Builds two strings for the 1st and last day of
    the previous month, ISO 8601 'YYYY-MM-DD'

    :param offset: Months to offset this calculation by
    :return: 1st day, last day
    """
    first = datetime.datetime.today().replace(day=1)
    last = first - datetime.timedelta(days=2)

    if offset:
        first = first.replace(month=first.month-offset)
        last = last.replace(month=first.month-offset)

    num_days = calendar.monthrange(last.year, last.month)[1]

    begin_date = '{0}-{1}-1'.format(last.year, last.month)
    end_date = '{0}-{1}-{2}'.format(last.year, last.month, num_days)

    return (datetime.datetime.strptime(begin_date, DATE_FMT).date(),
            datetime.datetime.strptime(end_date, DATE_FMT).date())


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


def process_monthly_metrics(cfg, env, remote_dir, local_dir, begin, stop):
    """
    Put together metrics for the previous month then
    email the results out

    :param cfg: database connection info (host, port, username, password)
    :type cfg: dict
    :param env: dev/tst/ops (used to get the hostname of the external download servers)
    :type env: str
    :param remote_dir: location of download logs (nginx)
    :type remote_dir: str
    :param local_dir: location to save log files
    :type local_dir: str
    :param begin: timestamp to begin searching the logs
    :type begin: datetime.date
    :param stop: timestamp to stop searching the logs
    :type stop: datetime.date
    """
    msg = ''
    receive, sender, debug = get_addresses(cfg)
    subject = EMAIL_SUBJECT.format(begin=begin, stop=stop)

    try:
        # Fetch the web log
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        dmzinfo = utils.query_connection_info(cfg, env)
        files = utils.find_remote_files_sudo(dmzinfo['host'], dmzinfo['username'], dmzinfo['password'], dmzinfo['port'],
                                             remote_dir, LOG_FILENAME)
        files = utils.subset_by_date(files, begin, stop, LOG_FILE_TIMESTAMP)

        # Process the web log file
        log_glob = os.path.join(local_dir, LOG_FILENAME)
        infodict, order_paths = calc_dlinfo(log_glob, begin, stop)
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
    rng = date_range()
    defaults = {'remote': '/var/log/nginx/archive/',
                'begin': rng[0],
                'stop': rng[1],
                'conf_file': utils.CONF_FILE,
                'dir': os.path.join(os.path.expanduser('~'), 'temp-logs')}
    # TODO: Add category (collections, pre-collections...)

    opts = arg_parser(defaults)
    cfg = utils.get_cfg(opts['conf_file'], section='config')

    process_monthly_metrics(cfg, opts['env'], opts['remote'], opts['dir'], opts['begin'], opts['stop'])


if __name__ == '__main__':
    run()
