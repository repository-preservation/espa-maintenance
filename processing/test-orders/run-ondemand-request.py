#! /usr/bin/env python

'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Execute test orders using the local environment.

History:
  Created April/2014 by Ron Dilley, USGS/EROS
'''

import os
import sys
import socket
import logging
import json
from argparse import ArgumentParser

import settings
import sensor
import utilities

import parameters


# ============================================================================
def build_argument_parser():
    '''
    Description:
      Build the command line argument parser.
    '''

    # Create a command line argument parser
    description = "Configures and executes a test order"
    parser = ArgumentParser(description=description)

    # Add parameters
    parser.add_argument('--keep-log',
                        action='store_true', dest='keep_log', default=False,
                        help="keep the log file")

    parser.add_argument('--request',
                        action='store', dest='request', required=True,
                        help="request to process")

    parser.add_argument('--master',
                        action='store_true', dest='master', default=False,
                        help="use the master products file")

    parser.add_argument('--plot',
                        action='store_true', dest='plot', default=False,
                        help="generate plots")

    parser.add_argument('--pre',
                        action='store_true', dest='pre', default=False,
                        help="use a -PRE order suffix")

    parser.add_argument('--post',
                        action='store_true', dest='post', default=False,
                        help="use a -POST order suffix")

    return parser
# END - build_argument_parser


# ============================================================================
def process_test_order(request_file, products_file, env_vars,
                       keep_log, plot, pre, post):
    '''
    Description:
      Process the test order file.
    '''

    logger = logging.getLogger(__name__)

    template_file = 'template.json'
    template_dict = None

    tmp_order = 'tmp-test-order'

    order_id = (request_file.split('.json')[0]).replace("'", '')

    if pre:
        order_id = ''.join([order_id, '-PRE'])

    if post:
        order_id = ''.join([order_id, '-POST'])

    have_error = False
    status = True
    error_msg = ''

    products = list()
    if not plot:
        with open(products_file, 'r') as scenes_fd:
            while (1):
                product = scenes_fd.readline().strip()
                if not product:
                    break
                products.append(product)
    else:
        products = ['plot']

    logger.info("Processing Products [%s]" % ', '.join(products))

    with open(template_file, 'r') as template_fd:
        template_contents = template_fd.read()
        if not template_contents:
            raise Exception("Template file [%s] is empty" % template_file)

        template_dict = json.loads(template_contents)
        if template_dict is None:
            logger.error("Loading template.json")

    for product in products:
        logger.info("Processing Product [%s]" % product)

        with open(request_file, 'r') as request_fd:
            request_contents = request_fd.read()
            if not request_contents:
                raise Exception("Order file [%s] is empty" % request_file)

            request_dict = json.loads(request_contents)
            if request_dict is None:
                logger.error("Loading [%s]" % request_file)

            # Merge the requested options with the template options, to create
            # a new dict with the requested options overriding the template.
            new_dict = template_dict.copy()
            new_dict.update(request_dict)
            new_dict['options'] = template_dict['options'].copy()
            new_dict['options'].update(request_dict['options'])

            # Turn it into a string for follow-on processing
            order_contents = json.dumps(new_dict, indent=4, sort_keys=True)

            logger.info("Processing Request File [%s]" % request_file)

            with open(tmp_order, 'w') as tmp_fd:

                logger.info("Creating [%s]" % tmp_order)

                tmp_line = order_contents

                # Update the order for the developer
                tmp = product[:3]
                download_url = 'null'
                dev_cache_dir = env_vars['dev_cache_dir']['value']
                is_modis = False
                if tmp == 'MOD' or tmp == 'MYD':
                    is_modis = True

                # for plots
                if not is_modis and not plot:
                    product_path = ('%s/%s/%s%s'
                                    % (env_vars['dev_data_dir']['value'],
                                       product[:3], product, '.tar.gz'))

                    logger.info("Using Product Path [%s]" % product_path)
                    if not os.path.isfile(product_path):
                        error_msg = ("Missing product data (%s)"
                                     % product_path)
                        have_error = True
                        break

                    download_url = ('file://%s' % product_path)

                elif not plot:
                    if tmp == 'MOD':
                        base_source_path = settings.TERRA_BASE_SOURCE_PATH
                    else:
                        base_source_path = settings.AQUA_BASE_SOURCE_PATH

                    short_name = sensor.instance(product).short_name
                    version = sensor.instance(product).version
                    archive_date = utilities.date_from_doy(
                        sensor.instance(product).year,
                        sensor.instance(product).doy)
                    xxx = '%s.%s.%s' % (str(archive_date.year).zfill(4),
                                        str(archive_date.month).zfill(2),
                                        str(archive_date.day).zfill(2))

                    product_path = ('%s/%s.%s/%s' % (base_source_path,
                                                     short_name,
                                                     version,
                                                     xxx))

                    if tmp == 'MOD' or tmp == 'MYD':
                        download_url = ('http://%s/%s/%s.hdf'
                                        % (settings.MODIS_INPUT_CHECK_HOST,
                                           product_path,
                                           product))

                sensor_name = 'plot'
                if not plot:
                    sensor_name = sensor.instance(product).sensor_name
                    logger.info("Processing Sensor [%s]" % sensor_name)
                else:
                    logger.info("Processing Plot Request")

                tmp_line = tmp_line.replace('\n', '')
                tmp_line = tmp_line.replace("ORDER_ID", order_id)
                tmp_line = tmp_line.replace("SCENE_ID", product)

                if sensor_name in ['tm', 'etm', 'olitirs']:
                    tmp_line = tmp_line.replace("PRODUCT_TYPE", 'landsat')
                elif sensor_name in ['terra', 'aqua']:
                    tmp_line = tmp_line.replace("PRODUCT_TYPE", 'modis')
                else:
                    tmp_line = tmp_line.replace("PRODUCT_TYPE", 'plot')

                if plot:
                    statistics_host = socket.gethostname()
                    statistics_directory = '%s/%s' % (dev_cache_dir, order_id)

                    tmp_line = tmp_line.replace("STATISTICS_DIRECTORY",
                                                statistics_directory)
                    tmp_line = tmp_line.replace("STATISTICS_HOST",
                                                statistics_host)

                destination_directory = '%s/%s' % (dev_cache_dir, order_id)
                tmp_line = tmp_line.replace("DESTINATION_DIRECTORY",
                                            destination_directory)

                tmp_line = tmp_line.replace("DOWNLOAD_URL", download_url)

                tmp_fd.write(tmp_line)

                # Validate again, since we modified it
                parms = json.loads(tmp_line)
                # parms = parameters.instance(json.loads(tmp_line))
                print(json.dumps(parms, indent=4, sort_keys=True))

            # END - with tmp_order
        # END - with request_file

        if have_error:
            logger.error(error_msg)
            return False

        keep_log_str = ''
        if keep_log:
            keep_log_str = '--keep-log'

        cmd = ("cd ..; cat test-orders/%s | ./ondemand_mapper.py %s"
               % (tmp_order, keep_log_str))

        output = ''
        try:
            logger.info("Processing [%s]" % cmd)
            output = utilities.execute_cmd(cmd)
            if len(output) > 0:
                print output
        except Exception, e:
            logger.exception("Processing failed")
            status = False

    os.unlink(tmp_order)

    return status


# ============================================================================
if __name__ == '__main__':
    '''
    Description:
        Main code for executing a test order.
    '''

    logging.basicConfig(format=('%(asctime)s.%(msecs)03d %(process)d'
                                ' %(levelname)-8s'
                                ' %(filename)s:%(lineno)d:%(funcName)s'
                                ' -- %(message)s'),
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.DEBUG)

    logger = logging.getLogger(__name__)

    # Build the command line argument parser
    parser = build_argument_parser()

    env_vars = dict()
    env_vars = {'dev_data_dir': {'name': 'DEV_DATA_DIRECTORY',
                                 'value': None},
                'dev_cache_dir': {'name': 'DEV_CACHE_DIRECTORY',
                                  'value': None},
                'espa_work_dir': {'name': 'ESPA_WORK_DIR',
                                  'value': None}}

    missing_environment_variable = False
    for var in env_vars:
        env_vars[var]['value'] = os.environ.get(env_vars[var]['name'])

        if env_vars[var]['value'] is None:
            logger.warning("Missing environment variable [%s]"
                           % env_vars[var]['name'])
            missing_environment_variable = True

    # Terminate FAILURE if missing environment variables
    if missing_environment_variable:
        logger.critical("Please fix missing environment variables")
        sys.exit(1)

    # Parse the command line arguments
    args = parser.parse_args()

    request_file = "%s.json" % args.request.replace("'", "\'")
    if not os.path.isfile(request_file):
        logger.critical("Request file [%s] does not exist" % request_file)
        sys.exit(1)

    products_file = None
    if not args.plot:
        products_file = "%s.products" % args.request

        if args.master:
            # Use the master file instead
            products_file = "%s.master.products" % args.request

        if not os.path.isfile(products_file):
            logger.critical("No products file exists for [%s]"
                            % args.request)
            sys.exit(1)

    # Avoid the creation of the *.pyc files
    os.environ['PYTHONDONTWRITEBYTECODE'] = '1'

    if not process_test_order(request_file, products_file, env_vars,
                              args.keep_log, args.plot, args.pre, args.post):
        logger.critical("Request [%s] failed to process" % args.request)
        sys.exit(1)

    # Terminate SUCCESS
    sys.exit(0)
