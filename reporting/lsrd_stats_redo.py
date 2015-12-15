#!/usr/bin/env python
import re
import datetime

from dbconnect import DBConnect
from utils import get_cfg
from utils import send_email


# This info should come from a config file
EMAIL_FROM = 'espa@espa.cr.usgs.gov'
EMAIL_TO = ['klsmith@usgs.gov']
EMAIL_SUBJECT = 'LSRD Monthly Statistics'


def download_info(info):
    boiler = '\n==========================================\n' \
             ' On-demand - Download Info\n' \
             '==========================================\n' \
             'Total number of ordered scenes downloaded through ESPA order interface order links: {tot_dl}\n' \
             'Total volume of ordered scenes downloaded (GB): {tot_vol}\n' \

    return boiler.format(**info)


def ondemand_info(info):
    boiler = '\n==========================================\n' \
             ' On-demand - {who}\n' \
             '==========================================\n' \
             ' Total scenes ordered in the month for {who} interface: {sc_month}\n' \
             ' Number of scenes ordered in the month (USGS) for {who} interface: {sc_usgs}\n' \
             ' Number of scenes ordered in the month (non-USGS) for {who} interface: {sc_non}\n' \
             ' Total orders placed in the month for {who} interface: {or_month}\n' \
             ' Number of total orders placed in the month (USGS) for {who} interface: {or_usgs}' \
             ' Number of total orders placed in the month (non-USGS) for {who} interface: {or_non}' \
             ' Total number of unique On-Demand users for {who} interface: {tot_unique}\n'

    return boiler.format(**info)


def calc_dlinfo(log_file):
    infodict = {'tot_dl': 0,
                'tot_vol': 0.0}

    # (ip, logname, user, datetime, method, resource, status, size, referrer, agent)
    regex = r'(.*?) (.*?) (.*?) \[(.*?)\] "(.*?) (.*?) (.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'

    with open(log_file, 'r') as log:
        for line in log:
            try:
                gr = re.match(regex, line).groups()
                if gr[7] == '200' and gr[4] == 'GET' and '.tar.gz' in gr[5]:
                    infodict['tot_vol'] += int(gr[8])
                    infodict['tot_dl'] += 1
            except:
                continue

    # Bytes to GB
    infodict['tot_vol'] /= 1073741824.0

    return infodict


def db_scenestats(source, month, dbinfo):
    sql = (r"select COUNT(*) as usgs_scene_orders "
           r"from ordering_scene "
           r"inner join ordering_order on ordering_scene.order_id = ordering_order.id "
           r"where ordering_order.order_date::text {0}like \'{1}-%\' "
           r"and ordering_order.orderid like '%@usgs.gov-%' "
           r"and ordering_order.order_source = '{2}';")

    results = {'sc_month': 0,
               'sc_usgs': 0,
               'sc_non': 0}

    mods = ('', 'not ')

    with DBConnect(**dbinfo) as db:
        for mod in mods:
            db.select(sql.format(mod, month, source))

            if mod:
                results['sc_non'] += int(db[0][0])
            else:
                results['sc_usgs'] += int(db[0][0])

    results['sc_month'] = results['sc_usgs'] + results['sc_non']

    return results


def db_orderstats(source, month, dbinfo):
    sql = (r"select COUNT(*) "
           r"from ordering_order "
           r"where order_date::text like \'{0}-%\' "
           r"and ordering_order.order_source = '{1}'")

    results = {'or_month': 0,
               'or_usgs': 0,
               'or_non': 0}

    mods = ('', 'not ')

    with DBConnect(**dbinfo) as db:
        for mod in mods:
            db.select(sql.format(mod, month, source))

            if mod:
                results['or_non'] += int(db[0][0])
            else:
                results['or_usgs'] += int(db[0][0])

    results['or_month'] = results['or_usgs'] + results['or_non']

    return results


def prev_month():
    first = datetime.datetime.today().replace(day=1)
    last_month = first - datetime.timedelta(days=2)

    return last_month.strftime("%Y-%m")


def run():
    pass


if __name__ == '__main__':
    run()
