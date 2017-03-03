#!/usr/bin/env python
import re
import glob
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
LOG_FILENAME = 'edclpdsftp.cr.usgs.gov-' # Change to ssl-access-log
LOG_FILE_TIMESTAMP = '%Y%m%d' + '.gz'

EMAIL_SUBJECT = 'LSRD ESPA Metrics for {begin} to {stop}'
ORDER_SOURCES = ('ee', 'espa')

SENSOR_KEYS = ('tm4', 'tm5', 'etm7', 'olitirs8', 'oli8',
               'tm4_collection', 'tm5_collection', 'etm7_collection', 'olitirs8_collection', 'oli8_collection',
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
    parser.add_argument('-d', '--dir', dest='dir',
                        default=defaults['dir'],
                        help='Directory to temporarily store logs')
    parser.add_argument('--sensors', dest='sensors',
                        default=defaults['sensors'],
                        nargs='+', help='Sensors to include (ALL) or {}'
                        .format(SENSOR_KEYS))

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
              ' {title}\n'
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
              ' LST: {lst}\n'
              ' DSWE: {swe}\n'
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
                         lst=info.get('lst', 0),
                         swe=info.get('swe', 0),
                         plot=info.get('plot_statistics', 0))


def top_users_boiler(info):
    """
    Boiler plate text for user Top Scenes Ordered Info for orders

    :param info: values to insert into the boiler plate
    :type info: list of tuples
    :return: formatted string
    """
    boiler = ('\n==========================================\n'
              ' Scenes ordered by Top Users\n'
              '==========================================\n')
    boiler += ''.join(' {%s[0]}: {%s[1]}\n' % (i, i) for i in range(10))

    return boiler.format(*info)


def db_prodinfo(dbinfo, begin_date, end_date, sensors):
    """
    Queries the database to build the ordered product counts
    dates are given as ISO 8601 'YYYY-MM-DD'

    :param dbinfo: Database connection information
    :type dbinfo: dict
    :param begin_date: Date to start the counts on
    :type begin_date: str
    :param end_date: Date to end the counts on
    :type end_date: str
    :param sensors: which sensors to process (['tm4','etm7',...])
    :type sensors: tuple
    :return: Dictionary of count values
    """
    sql = ('SELECT product_opts '
           'FROM ordering_order '
           'left join lateral jsonb_object_keys(product_opts) sensors on True '
           'WHERE order_date::date >= %s '
           'AND order_date::date <= %s '
           'AND sensors in %s '
           "and product_opts->sensors ? 'inputs' "
           "group by product_opts, id")

    init = {'total': 0}

    with DBConnect(**dbinfo) as db:
        db.select(sql, (begin_date, end_date, sensors))
        results = reduce(counts_prodopts, [process_db_prodopts(r, sensors) for r in db.fetcharr], init)

    results['title'] = 'What was Ordered'
    return results


def process_db_prodopts(row, sensors=SENSOR_KEYS):
    ret = defaultdict(int)
    # ret = {'total': 0}
    opts = row[0]

    for key in sensors:
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


def landsat_output_regex(filename):
    """
    Convert a download location into information for landsat scene-ids
    :param filename: full path to download resource
    :return: dict
    """
    fname = os.path.basename(filename)
    sceneid = fname.split('-')[0]
    regex_pre = '^(?P<sensor>L\w{2})[0-9]{6}[0-9]{7}$'
    regex_collect = '^(?P<sensor>L\w{3})[0-9]{6}[0-9]{8}(?P<collect>\w{4})$'
    for regex in [regex_pre, regex_collect]:
        res = re.match(regex, sceneid)
        if res:
            return res.groupdict()


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
        oid = urllib2.unquote(orderid)

        if oid not in prod_options:
            continue

        opts = prod_options[oid]

        if 'plot_statistics' in opts and opts['plot_statistics']:
            results['plot_statistics'] += 1

        for key in SENSOR_KEYS:
            if key in opts:
                # Scene names get truncated during distribution
                res = [x for x in opts[key]['inputs'] if scene in x]

                info = landsat_output_regex(scene)
                if info:
                    if 'collect' in info: # This is a landsat collection scene
                        # scene = LE070430332014070901T1, x = LE07_L1TP_043033_20140709_20160909_01_T1
                        # scene_regex = 'LE07_\\w{4}_043033_20140709_'
                        scene_regex = scene[0:4] + '_\w{4}_' + scene[4:10] + '_' + scene[10:18] + '_'
                        res = [x for x in opts[key]['inputs'] if re.match(scene_regex, x)]

                if res:
                    results['total'] += 1

                    for prod in opts[key]['products']:
                        if prod == 'l1' and ('projection' in opts or
                                             'image_extents' in opts):
                            results['customized_source_data'] += 1
                        else:
                            results[prod] += 1

    results['title'] = 'Downloads by Product'
    return results


def calc_dlinfo(log_glob, start_date, end_date, sensors):
    """
    Count the total tarballs downloaded from /orders/ and their combined size

    :param log_glob: Glob for Log Format file path (e.g. '/path/to/logs*')
    :type log_glob: str
    :param start_date: Compares >= timestamp in log
    :type start_date: datetime.date
    :param end_date: Compares <= timestamp in log
    :type end_date: datetime.date
    :param sensors: which sensors to process (['tm4','etm7',...])
    :type sensors: tuple
    :return: Dictionary of values
    """
    infodict = {'tot_dl': 0,
                'tot_vol': 0.0}
    bytes_in_a_gb = 1073741824.0

    files = glob.glob(log_glob)
    if len(files) < 1:
        raise IOError('Could not find %s' % log_glob)
    files = utils.subset_by_date(files, start_date, end_date, LOG_FILE_TIMESTAMP)
    if len(files) < 1:
        raise RuntimeError('No files found in date range: %s' % log_glob)

    order_paths = set()
    for log_file in files:
        print('* Parse: {}'.format(log_file))
        with gzip.open(log_file) as log:
            for line in log:
                gr = filter_log_line(line, start_date, end_date)
                if gr:
                    if get_sensor_name(gr['resource']) not in sensors:
                        # Difficult to say if statistics should be counted...
                        # if not gr['resource'].endswith('statistics.tar.gz'):
                        continue
                    infodict['tot_vol'] += int(gr['size'])
                    infodict['tot_dl'] += 1
                    order_paths.add(gr['resource'])

    # Bytes to GB
    infodict['tot_vol'] /= bytes_in_a_gb

    return infodict, list(order_paths)


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
    # Leaving the old nginx log output style for previous months
    regexs = [(r'(?P<ip>.*?) - \[(?P<datetime>.*?)\] "(?P<method>.*?) (?P<resource>.*?) (?P<protocol>.*?)" '
              r'(?P<status>\d+) (?P<len>\d+) (?P<range>.*?) (?P<size>\d+) \[(?P<reqtime>\d+\.\d+)\] "(?P<referrer>.*?)" '
              r'"(?P<agent>.*?)"'),
              (r'(?P<ip>.*?) (?P<logname>.*?) (?P<user>.*?) \[(?P<datetime>.*?)\] "(?P<method>.*?) (?P<resource>.*?) '
              r'(?P<status>\d+)" (?P<size>\d+) (?P<referrer>\d+) "(?P<agent>.*?)" "(?P<extra>.*?)"'),
             (r'(?P<ip>[0-9\.]*) .* \[(?P<datetime>.*)\] \"(?P<method>[A-Z]*) (?P<resource>.*) '
               r'(?P<protocol>.*)\" (?P<status>\d+) (?P<size>\d+) "(?P<referrer>.*?)" "(?P<agent>.*)"'),
              (r'(?P<ip>[0-9\.]*) .* \[(?P<datetime>.*)\] "(?P<method>[A-Z]*) (?P<resource>.*) (?P<protocol>.*)" '
               r'(?P<status>\d+) (?P<len>\d+) (?P<range>.*) (?P<size>\d+) \[(?P<reqtime>\d+\.\d+)\] "(?P<referrer>.*)" '
               r'"(?P<agent>.*)"')
              ]

    if ('tar.gz' in line) and ('GET' in line):
        for regex in regexs:
            res = re.match(regex, line)
            if res:
                break
        if res:
            gr = res.groupdict()
            ts, tz = gr['datetime'].split()
            dt = datetime.datetime.strptime(ts, r'%d/%b/%Y:%H:%M:%S').date()

            if ((gr['status'] in ['200', '206']) and
                    gr['method'] == 'GET' and
                    '.tar.gz' in gr['resource'] and
                    '/orders/' in gr['resource'] and
                    start_date <= dt <= end_date):

                return gr
            else:
                return False
        else:
            raise ValueError('! Unable to parse download line: \n\t{}'.format(line))
    else:
        return False


def get_sensor_name(filename):
    """
    Converts a filename into a sensor key (SENSOR_KEYS)
    :param filename: product output path [/orders/<oid>/<prod>-<time>.tar.gz]
    :return: str
    """
    lut = {'LC8': 'olitirs8', 'LC08': 'olitirs8_collection',
           'LO8': 'oli8', 'LT8': 'tirs8',
           'LO08': 'oli8_collection', 'LT08': 'tirs8_collection',
           'LE7': 'etm7', 'LE07': 'etm7_collection',
           'LT5': 'tm5', 'LT05': 'tm5_collection',
           'LT4': 'tm4', 'LT04': 'tm4_collection',
           'MOD09A1': 'mod09a1', 'MOD09GA': 'mod09ga', 'MOD09GQ': 'mod09gq',
           'MOD09Q1': 'mod09q1', 'MOD13A1': 'mod13a1', 'MOD13A2': 'mod13a2',
           'MOD13A3': 'mod13a3', 'MOD13Q1': 'mod13q1',
           'MYD09A1': 'myd09a1', 'MYD09GA': 'myd09ga', 'MYD09GQ': 'myd09gq',
           'MYD09Q1': 'myd09q1', 'MYD13A1': 'myd13a1', 'MYD13A2': 'myd13a2',
           'MYD13A3': 'myd13a3', 'MYD13Q1': 'myd13q1'}
    fname = os.path.basename(filename)
    for prefix, sensor in lut.iteritems():
        if fname.startswith(prefix):
            return sensor


def db_scenestats(source, begin_date, end_date, sensors, dbinfo):
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
    :param sensors: which sensors to process (['tm4', 'etm7', ...])
    :type sensors: tuple
    :param dbinfo: Database connection information
    :type dbinfo: dict
    :return: Dictionary of the counts
    """
    sql = ('''select coalesce(sum(jsonb_array_length(product_opts->sensors->'inputs')),0)
              from ordering_order
              left join lateral jsonb_object_keys(product_opts) sensors on True
              where ordering_order.order_date::date >= %s
              and ordering_order.order_date::date <= %s
              and ordering_order.orderid like '%%@usgs.gov-%%'
              and ordering_order.order_source = %s
              and sensors in %s
              and product_opts->sensors ? 'inputs' ''',
           '''select coalesce(sum(jsonb_array_length(product_opts->sensors->'inputs')),0)
              from ordering_order
              left join lateral jsonb_object_keys(product_opts) sensors on True
              where ordering_order.order_date::date >= %s
              and ordering_order.order_date::date <= %s
              and ordering_order.orderid not like '%%@usgs.gov-%%'
              and ordering_order.order_source = %s
              and sensors in %s
              and product_opts->sensors ? 'inputs' ''')

    counts = {'scenes_month': 0,
              'scenes_usgs': 0,
              'scenes_non': 0}

    with DBConnect(**dbinfo) as db:
        for q in sql:
            db.select(q, (begin_date, end_date, source, sensors))

            if 'not like' in q:
                counts['scenes_non'] += int(db[0][0])
            else:
                counts['scenes_usgs'] += int(db[0][0])

    counts['scenes_month'] = counts['scenes_usgs'] + counts['scenes_non']

    return counts


def db_orderstats(source, begin_date, end_date, sensors, dbinfo):
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
    :param sensors: which sensor types to process (['tm4', 'etm7',...])
    :type sensors: tuple
    :param dbinfo: Database connection information
    :type dbinfo: dict
    :return: Dictionary of the counts
    """
    sql = ('''select COUNT(distinct orderid)
              from ordering_order
              left join lateral jsonb_object_keys(product_opts) sensors on True
              where order_date::date >= %s
              and order_date::date <= %s
              and orderid like '%%@usgs.gov-%%'
              and order_source = %s
              and sensors in %s
              and product_opts->sensors ? 'inputs' ;''',
           '''select COUNT(distinct orderid)
              from ordering_order
              left join lateral jsonb_object_keys(product_opts) sensors on True
              where order_date::date >= %s
              and order_date::date <= %s
              and orderid not like '%%@usgs.gov-%%'
              and order_source = %s
              and sensors in %s
              and product_opts->sensors ? 'inputs' ;''')

    counts = {'orders_month': 0,
              'orders_usgs': 0,
              'orders_non': 0}

    with DBConnect(**dbinfo) as db:
        for q in sql:
            db.select(q, (begin_date, end_date, source, sensors))

            if 'not like' in q:
                counts['orders_non'] += int(db[0][0])
            else:
                counts['orders_usgs'] += int(db[0][0])

    counts['orders_month'] = counts['orders_usgs'] + counts['orders_non']

    return counts


def db_uniquestats(source, begin_date, end_date, sensors, dbinfo):
    """
    Queries the database to get the total number of unique users
    dates are given as ISO 8601 'YYYY-MM-DD'

    :param source: EE or ESPA
    :type source: str
    :param begin_date: Date to start the count on
    :type begin_date: str
    :param end_date: Date to stop the count on
    :type end_date: str
    :param sensors: which sensor types to process (['tm4', 'etm7',...])
    :type sensors: tuple
    :param dbinfo: Database connection information
    :type dbinfo: dict
    :return: Dictionary of the count
    """
    sql = '''select count(distinct(split_part(orderid, '-', 1)))
             from ordering_order
             left join lateral jsonb_object_keys(product_opts) sensors on True
             where order_date::date >= %s
             and order_date::date <= %s
             and order_source = %s
             and sensors in %s
             and product_opts->sensors ? 'inputs' ;'''

    with DBConnect(**dbinfo) as db:
        db.select(sql, (begin_date, end_date, source, sensors))
        return db[0][0]


def db_top10stats(begin_date, end_date, sensors, dbinfo):
    """
    Queries the database to get the total number of unique users
    dates are given as ISO 8601 'YYYY-MM-DD'

    :param source: EE or ESPA
    :type source: str
    :param begin_date: Date to start the count on
    :type begin_date: str
    :param end_date: Date to stop the count on
    :type end_date: str
    :param sensors: which sensor types to process (['tm4', 'etm7',...])
    :type sensors: tuple
    :param dbinfo: Database connection information
    :type dbinfo: dict
    :return: Dictionary of the count
    """
    sql = '''select u.email, coalesce(sum(jsonb_array_length(product_opts->sensors->'inputs')),0) scenes
             from ordering_order o
             left join lateral jsonb_object_keys(product_opts) sensors on True
             join auth_user u
                  on o.user_id = u.id
             where o.order_date::date >= %s
             and o.order_date::date <= %s
             and sensors in %s
             and product_opts->sensors ? 'inputs'
             group by u.email
             order by scenes desc
             limit 10'''

    with DBConnect(**dbinfo) as db:
        db.select(sql, (begin_date, end_date, sensors))
        return db[:]


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


def fetch_web_logs(dbconfig, env, outdir, begin, stop):
    """
    Connect to weblog storage location and move weblogs locally

    :param dbconfig: database connection info (host, port, username, password)
    :param env: dev/tst/ops (to get hostname of the external download servers)
    :param outdir: location to save log files
    :param begin: timestamp to begin searching the logs
    :param stop: timestamp to stop searching the logs
    :return: None
    """
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    dmzinfo = utils.query_connection_info(dbconfig, env)
    for log_loc in dmzinfo['log_locs']:
        host, remote_dir = log_loc.split(':')
        client = utils.RemoteConnection(host, user=dmzinfo['username'],
                                        password=dmzinfo['password'])
        files = client.list_remote_files(remote_dir=remote_dir,
                                         prefix=LOG_FILENAME)
        files = utils.subset_by_date(files, begin, stop, LOG_FILE_TIMESTAMP)
        for remote_path in files:
            filename = ("{host}_{fname}"
                        .format(host=host, fname=os.path.basename(remote_path)))
            local_path = os.path.join(outdir, filename)
            if not os.path.exists(local_path):
                client.download_remote_file(remote_path=remote_path,
                                            local_path=local_path)


def process_monthly_metrics(cfg, env, local_dir, begin, stop, sensors):
    """
    Put together metrics for the previous month then
    email the results out

    :param cfg: database connection info (host, port, username, password)
    :type cfg: dict
    :param env: dev/tst/ops (used to get the hostname of the external download servers)
    :type env: str
    :param local_dir: location to save log files
    :type local_dir: str
    :param begin: timestamp to begin searching the logs
    :type begin: datetime.date
    :param stop: timestamp to stop searching the logs
    :type stop: datetime.date
    :param sensors: which landsat/modis sensors to process (['tm4', 'etm7',...])
    :type sensors: tuple
    """
    fetch_web_logs(cfg, env, local_dir, begin, stop)

    log_glob = os.path.join(local_dir, '*' + LOG_FILENAME + '*access_log*.gz')
    infodict, order_paths = calc_dlinfo(log_glob, begin, stop, sensors)
    infodict['title'] = ('On-demand - Total Download Info\n Sensors:{}'
                         .format(','.join(sensors)))
    msg = download_boiler(infodict)
    
    # Downloads by Product
    orders_scenes = extract_orderid(order_paths)
    
    if len(orders_scenes):
        prod_opts = db_dl_prodinfo(cfg, orders_scenes)
        infodict = tally_product_dls(orders_scenes, prod_opts)
        msg += prod_boiler(infodict)

    # On-Demand users and orders placed information
    for source in ORDER_SOURCES:
        infodict = db_orderstats(source, begin, stop, sensors, cfg)
        infodict.update(db_scenestats(source, begin, stop, sensors, cfg))
        infodict['tot_unique'] = db_uniquestats(source, begin, stop, sensors, cfg)
        infodict['who'] = source.upper()
        msg += ondemand_boiler(infodict)


    # Orders by Product
    infodict = db_prodinfo(cfg, begin, stop, sensors)
    msg += prod_boiler(infodict)

    # Top 10 users by scenes ordered
    info = db_top10stats(begin, stop, sensors, cfg)
    if len(info) == 10:
        msg += top_users_boiler(info)

    print(msg)
    return msg


def run():
    rng = date_range()
    defaults = {'begin': rng[0],
                'stop': rng[1],
                'conf_file': utils.CONF_FILE,
                'dir': os.path.join(os.path.expanduser('~'), 'temp-logs'),
                'sensors': 'ALL'}

    opts = arg_parser(defaults)
    cfg = utils.get_cfg(opts['conf_file'], section='config')
    if opts['sensors'] == 'ALL':
        opts['sensors'] = [k for k in SENSOR_KEYS if k != 'invalid']

    msg = ''
    receive, sender, debug = get_addresses(cfg)
    subject = EMAIL_SUBJECT.format(begin=opts['begin'], stop=opts['stop'])
    try:
        msg = process_monthly_metrics(cfg,
                                      opts['environment'],
                                      opts['dir'],
                                      opts['begin'],
                                      opts['stop'],
                                      tuple(opts['sensors']))

    except Exception:
        exc_msg = str(traceback.format_exc()) + '\n\n' + msg
        utils.send_email(sender, debug, subject, exc_msg)
        msg = ('There was an error with statistics processing.\n'
               'The following have been notified of the error: {0}.'
               .format(', '.join(debug)))
        raise
    finally:
        utils.send_email(sender, receive, subject, msg)


if __name__ == '__main__':
    run()
