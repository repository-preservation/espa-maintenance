#!/usr/bin/env python
"""
    Multi-threaded log parsing, merging into a single file (likely, no longer time-ordered)

    Author: Jake Brinkmann <jacob.brinkmann.ctr@usgs.gov>
    Date: 11/01/2017
"""
import re
import glob
import datetime
import argparse
import traceback
import os
import gzip
import urllib.request, urllib.error, urllib.parse
import multiprocessing as mp

from . import utils

DATE_FMT = '%Y-%m-%d'
LOG_FILENAME = 'edclpdsftp.cr.usgs.gov-' # Change to ssl-access-log
LOG_FILE_TIMESTAMP = '%Y%m%d' + '.gz'

REGEXES = [
    (r'(?P<ip>.*?) - \[(?P<datetime>.*?)\] "(?P<method>.*?) (?P<resource>.*?) (?P<protocol>.*?)" '
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
REGEXES = [re.compile(r) for r in REGEXES]


def arg_parser(defaults):
    """
    Process the command line arguments
    """
    parser = argparse.ArgumentParser(description="LSRD ESPA Metrics")

    parser.add_argument('-b', '--begin', dest='begin',
                        default=defaults['begin'],
                        help='Start date to search (%s)' %
                        defaults['begin'].strftime(DATE_FMT))
    parser.add_argument('-s', '--stop', dest='stop',
                        default=defaults['stop'],
                        help='End date to search (%s)' %
                        defaults['stop'].strftime(DATE_FMT))
    parser.add_argument('-d', '--dir', dest='dir',
                        default=defaults['dir'],
                        help='Directory to temporarily store logs')

    args = parser.parse_args()
    defaults.update(args.__dict__)

    for _ in ['begin', 'stop']:
        if type(defaults[_]) is str:
            defaults[_] = datetime.datetime.strptime(defaults[_], DATE_FMT).date()

    return defaults


def parse_dls(log_glob, start_date, end_date, resource_regex):
    """
    Count the total tarballs downloaded from /orders/ and their combined size

    :param log_glob: Glob for Log Format file path (e.g. '/path/to/logs*')
    :type log_glob: str
    :param start_date: Compares >= timestamp in log
    :type start_date: datetime.date
    :param end_date: Compares <= timestamp in log
    :type end_date: datetime.date

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
        print(('* Parse: {}'.format(log_file)))
        with gzip.open(log_file) as log:
            for line in log:
                gr = filter_log_line(line, start_date, end_date)
                if gr:
                    if get_sensor_name(gr['resource']) not in sensors:
                        # Difficult to say if statistics should be counted...
                        # if not gr['resource'].endswith('statistics.tar.gz'):
                        continue
                    rparts = gr['resource'].split('/')
                    if len(rparts) != 4:
                        raise ValueError('Unexpected directory structure: %s'
                                         % rparts)
                    elif rparts[1] not in valid_orderids:
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
    :param sensors: which landsat/modis sensors to process
    :type sensors: tuple
    """
    valid_orderids = db_fetch_ordered(cfg, begin, stop, sensors)

    pickle_file = os.path.join(local_dir, '{0:%Y%m%d}-{1:%Y%m%d}.pkl'.format(begin, stop))
    if not os.path.exists(pickle_file):
        fetch_web_logs(cfg, env, local_dir, begin, stop)

        log_glob = os.path.join(local_dir, '*' + LOG_FILENAME + '*access_log*.gz')
        infodict, order_paths = calc_dlinfo(log_glob, begin, stop, sensors, valid_orderids)
        pickle.dump((infodict, order_paths), open(pickle_file, 'wb'))
    else:
        (infodict, order_paths) = pickle.load(open(pickle_file, 'rb'))

    infodict['title'] = ('On-demand - Total Download Info\n Sensors:{}'
                         .format(sensors))
    msg = download_boiler(infodict)

    # Downloads by Product
    orders_scenes = extract_orderid(order_paths)

    if len(orders_scenes):
        prod_opts = db_dl_prodinfo(cfg, orders_scenes)
        # WARNING: THIS IGNORES ORDERS FROM TST/DEV ENVIRONMENTS
        #          (included in total download volume)
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
    if len(info) > 0:
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
    elif opts['sensors'] == 'MODIS':
        opts['sensors'] = [k for k in SENSOR_KEYS if k.startswith('m')]
    elif opts['sensors'] == 'LANDSAT':
        opts['sensors'] = [k for k in SENSOR_KEYS
                           if any([s in k for s in ('tm4', 'tm5', 'etm7', 'oli')])]

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
        if not opts['debug']:
            utils.send_email(sender, debug, subject, exc_msg)
        msg = ('There was an error with statistics processing.\n'
               'The following have been notified of the error: {0}.'
               .format(', '.join(debug)))
        raise
    finally:
        if not opts['debug']:
            utils.send_email(sender, receive, subject, msg)


if __name__ == '__main__':
    run()
