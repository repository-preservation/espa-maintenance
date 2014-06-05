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
import json
import subprocess
from cStringIO import StringIO
from argparse import ArgumentParser


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
    parser.add_argument('--debug',
                        action='store_true', dest='debug', default=False,
                        help="turn debug logging on")

    parser.add_argument('--order-file',
                        action='store', dest='order_file', required=True,
                        help="order file to process")

    return parser
# END - build_argument_parser


# ============================================================================
def process_test_order(order_file, env_vars):
    '''
    Description:
      Process the test order file.
    '''

    tmp_order = 'tmp-' + order_file
    order_fd = open(order_file, 'r')
    tmp_fd = open(tmp_order, 'w')
    order_id = order_file.split('.json')[0]

    have_error = False
    error_msg = ''
    while 1:
        line = order_fd.readline()
        if not line:
            break

        tmp_line = line

        is_modis = False
        order = json.loads(line)
        scene = order['scene']

        tmp = scene[:3]
        if tmp == 'MOD' or tmp == 'MYD':
            is_modis = True

        if not is_modis:
            scene_path = env_vars['dev_data_dir']['value'] + '/' \
                + order['scene']
            scene_path += '.tar.gz'

            if not os.path.isfile(scene_path):
                error_msg = "Missing scene data (%s)" % scene_path
                have_error = True
                break

        tmp_line = tmp_line.replace("ORDER_ID", order_id)
        tmp_line = tmp_line.replace("DEV_DATA_DIRECTORY",
                                    env_vars['dev_data_dir']['value'])
        tmp_line = tmp_line.replace("DEV_CACHE_DIRECTORY",
                                    env_vars['dev_cache_dir']['value'])

        tmp_fd.write(tmp_line)

    order_fd.close()
    tmp_fd.close()

    if have_error:
        print error_msg
        return False

    cmd = "cd ..; cat test-orders/%s | ./cdr_ecv_mapper.py 2>&1" \
          " | tee test-orders/%s.log" % (tmp_order, order_file)

    output = ''
    proc = None
    status = True
    try:
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, shell=True)
        output = proc.communicate()[0]

        print output

        if proc.returncode < 0:
            print "Application terminated by signal [%s]" % cmd
            status = False

        elif proc.returncode != 0:
            print "Application failed to execute [%s]" % cmd
            status = False

        else:
            application_exitcode = proc.returncode >> 8
            if application_exitcode != 0:
                print "Application [%s] returned error code [%d]" \
                    % (cmd, application_exitcode)
                status = False

    except Exception, e:
        print str(e)
        status = False

    finally:
        del proc
        proc = None

    os.unlink(tmp_order)

    return status


# ============================================================================
if __name__ == '__main__':
    '''
    Description:
        Main code for executing a test order.
    '''

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
            print "Missing environment variable " + env_vars[var]['name']
            missing_environment_variable = True

    # Terminate FAILURE if missing environment variables
    if missing_environment_variable:
        print "Please fix missing environment variables"
        sys.exit(1)

    # Parse the command line arguments
    args = parser.parse_args()

    if not os.path.isfile(args.order_file):
        print "Order file (%s) does not exist" % args.order_file
        sys.exit(1)

    if not process_test_order(args.order_file, env_vars):
        print "Order file (%s) failed to process" % args.order_file
        sys.exit(1)

    # Terminate SUCCESS
    sys.exit(0)
