#!/usr/bin/env python
"""Create useful charts for insights into the monthly metrics"""

import datetime
import logging

import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
import shapely
import shapely.ops
import geopandas as gp
import numpy as np
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon
from matplotlib.ticker import FuncFormatter

from dbconnect import DBConnect

FIGSIZE = (16, 9)
MINALPHA = 0.035
MAXALPHA = 1.0
COLOR = '#e31a1c'


def load_wrs(filename='wrs2_asc_desc/wrs2_asc_desc.shp'):
    """Read WRS2 features shapefile."""
    logging.info('Read %s', filename)
    wrs = gp.GeoDataFrame.from_file(filename)
    # PATH, ROW, geometry
    return wrs


def query_scene_count(dbinfo, start, end, who=None):
    """Query count of scenes ordered per path/row.

    Arsg:
        dbinfo (dict): database connection info
        start (str): begining of period range (e.g. '2018-04-01')
        end (str): end of periof range (e.g. '2018-04-30')

    Returns:
        np.array: number of scenes per path/row

    """
    sql = '''
        select  count(*) n_scenes,
                case when split_part(s.name, '_', 2) = ''
                    then right(left(s.name,6),3)
                    else left(split_part(s.name, '_', 3), 3) end as path,

                case when split_part(s.name, '_', 2) = ''
                    then right(left(s.name,9),3)
                    else right(split_part(s.name, '_', 3), 3) end as row

        from ordering_scene s
        left join ordering_order o on o.id=s.order_id

        where
            o.order_date::date >= '{0}'
            and o.order_date::date <= '{1}'
            and s.sensor_type = 'landsat'
            {2}

        group by path, row

        ;
        '''

    email_str = '' if who is 'ALL' else ("and o.email = '%s'" % who)

    with DBConnect(**dbinfo) as db:
        dat = sqlio.read_sql_query(sql.format(start, end, email_str), db.conn)

    dat['path'] = dat['path'].astype(int)
    dat['row'] = dat['row'].astype(int)
    dat['alpha'] = dat['n_scenes'].apply(lambda v: get_alpha
                                         (v, MAXALPHA, MINALPHA,
                                          dat['n_scenes'].min(),
                                          dat['n_scenes'].max()))
    dat = dat.sort_values(by='alpha')
    return (
        dat[['path', 'row', 'alpha']].values,
        dat['n_scenes'].min(),
        dat['n_scenes'].max()
    )


def get_poly_wrs(path, row, features=None, facecolor='w'):
    """Extract longitude/latitude box for a path/row from shapefile."""
    prid = '{}_{}'.format(path, row)
    ix = (features['PATH'] == path) & (features['ROW'] == row)
    geom = features[ix]

    if (len(geom) != 1):
        raise AssertionError('Non-unique path/row (%s) in shapefile!', prid)

    poly = geom.geometry.values[0]
    if poly.geom_type == 'Polygon':
        lons, lats = poly.exterior.coords.xy
    elif poly.geom_type == 'MultiPolygon':
        lons, lats = [], []
        for subpoly in poly:
            ln, la = subpoly.exterior.coords.xy
            lons += ln
            lats += la
    return lons, lats


def plot_poly(lons, lats, mapm, **kwargs):
    """Convert bounding box into a styled Patch object."""
    return Polygon(zip(*mapm(lons, lats)), **kwargs)


def make_basemap(path_rows_alpha,
                 water='white', earth='grey', color=COLOR):
    """Create a heatmap of WRS2 path/rows ordered."""
    fig, ax = plt.subplots(figsize=FIGSIZE)
    mapm = Basemap(
        llcrnrlon=-180, llcrnrlat=-85,
        urcrnrlon=180, urcrnrlat=85,
        projection='mill'
    )

    features = load_wrs()

    mapm.drawmapboundary()
    mapm.drawcoastlines()
    mapm.fillcontinents(color=earth, lake_color=water)
    mapm.drawmapboundary(fill_color=water)
    mapm.drawcountries()
    mapm.drawmeridians(np.arange(-180, 180, 60),
                       labels=[False, False, False, True])
    mapm.drawparallels(np.arange(-80, 80, 20),
                       labels=[True, False, False, False])

    ax = plt.gca()
    for path, row, alpha in path_rows_alpha:
        lons, lats = get_poly_wrs(path, row, features)
        delta_lons = abs(max(lons) - min(lons))
        if delta_lons > 180:
            def p_dateline(x):
                """International dateline longitude wrap-around."""
                if x > 0:
                    return x-360
                return x
            lons = map(p_dateline, lons)
        patch = plot_poly(lons, lats, mapm, facecolor=color, edgecolor=color,
                          alpha=alpha)
        ax.add_patch(patch)


def get_alpha(x, b, a, mmin, mmax):
    """Create alpha level from scaled values."""
    return a + (((b-a)*(x-mmin))/(mmax-mmin))


def create_fake_cb(mmin, mmax, color, step=100):
    """Generate a fake discrete colorbar for the Path objects."""
    mymap = mpl.colors.LinearSegmentedColormap.from_list('mycolors',
                                                         ['white', color])

    # Using contourf to provide my colorbar info, then clearing the figure
    Z = [[0, 0], [0, 0]]
    plt.subplots(figsize=FIGSIZE, facecolor='w')
    levels = range(mmin, mmax+step, step)
    CS3 = plt.contourf(Z, levels, cmap=mymap)
    plt.clf()

    return CS3


def scrub_email(address):
    """
    Remove the local-part from an email address
    for the sake of anonymity
    :param address: <str>
    :return: <str>
    """
    domain = address.split('@')[1]

    return 'user@{}'.format(domain)


def pathrow_heatmap(dbinfo, start, end, user='ALL', color=COLOR):
    """Create graphic for number of scenes per path/row."""
    alphas, mmin, mmax = query_scene_count(dbinfo, start, end, user)
    cb = create_fake_cb(mmin, mmax, color)
    make_basemap(alphas)
    plt.title('Landsat Scenes (path/row) Ordered\nUSER {}: {} - {}'
              .format(scrub_email(address=user),
                      start,
                      end), fontsize=14)
    cbar = plt.colorbar(cb)
    cbar.ax.set_title('  Scenes', weight='bold', fontsize=14)
    cbar.ax.tick_params(labelsize=12)
    pltfname = '/tmp/paths_rows_ordered_{}.png'.format(user)
    plt.savefig(pltfname, bbox_inches='tight')
    return pltfname


def query_sensor_count(dbinfo, start, end, sensors=None):
    """Select aggregate number of scenes sorted by sensor."""
    sql = '''
        select count(s.name) n_scenes,
                left(s.name, 4) sensor,
                extract(month from o.order_date) mm,
                extract(year from o.order_date) yy
        from ordering_scene s
            join ordering_order o on o.id=s.order_id
        where o.order_date::date >= '{0}'
            and o.order_date::date <= '{1}'
            and s.sensor_type = 'landsat'
        group by sensor, yy, mm'''

    with DBConnect(**dbinfo) as db:
        dat = sqlio.read_sql_query(sql.format(start, end), db.conn)

    d2 = dat.pivot_table(index='mm', values='n_scenes',
                         columns='sensor').fillna(0)

    def p_month_name(mmm):
        """Format integers as month names."""
        return datetime.date(2017, mmm, 1).strftime('%b')
    d2.index = d2.index.astype(int).map(p_month_name)

    if 'LO08' in d2.columns:
        d2['LC08'] += d2['LO08']
    return d2[sensors]


def sensor_barchart(dbinfo, start, end):
    """Generate barchart for number of scenes per sensor."""
    fig, ax = plt.subplots(figsize=FIGSIZE)
    sensors = ['LT04', 'LT05', 'LE07', 'LC08']
    my_colors = [['#d62728', '#2ca02c', '#1f77b4', '#ff7f0e']]

    dat = query_sensor_count(dbinfo, start, end, sensors)

    dat[sensors].loc[start.strftime('%b')
                     ].plot(kind='bar', ax=ax, color=my_colors)
    plt.title('Scenes Ordered - {}'.format(end.strftime('%B %Y')))
    plt.ylabel('# Scenes')
    plt.xlabel('Spacecraft')
    def fmt_yaxis(y, _):
        """Scale y-axis by 1,000 for k"""
        return '{}k'.format(int(y/1e3))
    ax.yaxis.set_major_formatter(FuncFormatter(fmt_yaxis))
    plt.xticks(range(len(sensors)), sensors, rotation=45)

    # Label bars with exact height
    for p in ax.patches:
        ax.annotate(str(int(p.get_height())),
                    (p.get_x() * 1.005, p.get_height() * 1.005),
                    fontsize=18)

    pltfname = '/tmp/n_scenes_ordered_this_month.png'
    plt.margins(0.0, 0.1)
    plt.savefig(pltfname, bbox_inches='tight')
    return pltfname
