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
import logging
import json
from argparse import ArgumentParser

try:
    import settings
except:
    from espa_common import settings

try:
    import sensor
except:
    from espa_common import sensor

try:
    import utilities
except:
    from espa_common import utilities

import parameters


MASTER_PLOT_FILE = 'master-plot.json'


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

    parser.add_argument('--order-id',
                        action='store', dest='order_id', required=True,
                        help="order_id to plot")

    return parser
# END - build_argument_parser


# ============================================================================
def process_test_order(order_id, env_vars, keep_log):
    '''
    Description:
      Process the test order file.
    '''

    logger = logging.getLogger(__name__)

    tmp_order = 'tmp-' + MASTER_PLOT_FILE

    have_error = False
    status = True
    error_msg = ''

    with open(MASTER_PLOT_FILE, 'r') as order_fd:
        order_contents = order_fd.read()
        if not order_contents:
            raise Exception("Order file [%s] is empty" % MASTER_PLOT_FILE)

        # Validate using our parameter object
        # order = parameters.instance(json.loads(order_contents))
        # print json.dumps(order, indent=4, sort_keys=True)

        with open(tmp_order, 'w') as tmp_fd:

            tmp_line = order_contents

            # Update the order for the developer
            source_host = 'localhost'

            tmp_line = tmp_line.replace('\n', '')

            tmp_line = \
                tmp_line.replace("DEV_CACHE_DIRECTORY",
                                 env_vars['dev_cache_dir']['value'])
            tmp_line = tmp_line.replace("ORDER_ID", order_id)

            tmp_fd.write(tmp_line)

#            # Validate again, since we modified it
#            parms = parameters.instance(json.loads(tmp_line))
#            logger.info(json.dumps(parms, indent=4, sort_keys=True))

        # END - with tmp_order
    # END - with MASTER_PLOT_FILE

    if have_error:
        logger.error(error_msg)
        return False

    keep_log_str = ''
    if keep_log:
        keep_log_str = '--keep-log'

    cmd = ("cd ..; cat test-orders/%s | ./lpcs_mapper.py %s"
           % (tmp_order, keep_log_str))

    output = ''
    try:
        logger.info("Processing [%s]" % cmd)
        output = utilities.execute_cmd(cmd)

    except Exception, e:
        raise

    finally:
        if len(output) > 0:
            print output

    os.unlink(tmp_order)
# END - process_test_order


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
                        level=logging.INFO)

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

    # Avoid the creation of the *.pyc files
    os.environ['PYTHONDONTWRITEBYTECODE'] = '1'

    try:
        process_test_order(args.order_id, env_vars, args.keep_log)
    except Exception, e:
        logger.exception("Plot request for order [%s] failed to process"
                         % args.order_id)
        # Terminate FAILURE
        sys.exit(1)

    # Terminate SUCCESS
    sys.exit(0)
