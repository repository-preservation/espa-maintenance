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
import glob
from argparse import ArgumentParser
import filecmp

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
    parser.add_argument('--request',
                        action='store', dest='request', required=True,
                        help="request to process")

    return parser
# END - build_argument_parser


# ============================================================================
def verify_order_results(env_vars, products_file,
                         master_location, work_location):
    '''
    Description:
      Process the test order file.
    '''

    logger = logging.getLogger(__name__)

    status = True

    items_text = None
    with open(products_file, 'r') as items_fd:
        items_text = items_fd.read()

    if not items_text:
        logger.warning("No items found to process!")
        return False

    items = sorted(filter(None, items_text.split('\n')))
    master_items = glob.glob('/'.join([master_location, '*']))
    work_items = glob.glob('/'.join([work_location, '*']))

    master_items = sorted([os.path.basename(x) for x in master_items])
    work_items = sorted([os.path.basename(x) for x in work_items])

    # Make sure we have matches
    if ((items != master_items) or (items != work_items)):
        logger.warning("Item lists do not match!")
        return False

    # Process through the items
    for item in items:
        logger.info("Processing Item [%s]" % item)

        # Figure out the directory paths
        master_dir = '/'.join([master_location, item, 'work'])
        work_dir = '/'.join([work_location, item, 'work'])

        # Grab the files in the work directory
        master_items = glob.glob('/'.join([master_dir, '*']))
        work_items = glob.glob('/'.join([work_dir, '*']))

        # Only need the basenames
        master_items = sorted([os.path.basename(x) for x in master_items])
        work_items = sorted([os.path.basename(x) for x in work_items])

        # Report the files that we have files in one directory and not the
        # other, the easy way
        for mm in list(set(master_items) - set(work_items)):
            logger.warning("[%s] found only in [%s]" % (mm, master_dir))
            status = False
        for ww in list(set(work_items) - set(master_items)):
            logger.warning("[%s] found only in [%s]" % (ww, work_dir))
            status = False

        # Combine the two lists into one unique list
        compare_items = list(set(master_items) | set(work_items))

        # Let python do the comparison
        (matches, mismatches, errors) = filecmp.cmpfiles(master_dir, work_dir,
                                                         compare_items,
                                                         shallow=False)

        # Report the files that do not match
        for mismatch in mismatches:
            logger.error("[%s] Does not match" % mismatch)
            status = False

    return status


# ============================================================================
if __name__ == '__main__':
    '''
    Description:
        Main code for executing a test order.
    '''

    # Avoid the creation of the *.pyc files
    os.environ['PYTHONDONTWRITEBYTECODE'] = '1'

    # Configure the logging
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
                'dev_master_products_dir': {'name': 'DEV_MASTER_PRODUCTS_DIR',
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

    # Verify that the master location exists
    master_location = os.path.join(env_vars['dev_master_products_dir']['value'],
                                   args.request)

    if not os.path.isdir(master_location):
        logger.critical("Master Location for [%s] does not exist"
                        % args.request)
        sys.exit(1)

    # Verify that the work location exists
    work_location = os.path.join(env_vars['espa_work_dir']['value'],
                                 args.request)

    if not os.path.isdir(work_location):
        logger.critical("Work Location for [%s] does not exist"
                        % args.request)
        sys.exit(1)

    # Verify that the products file exists
    products_file = "%s.products" % args.request

    if not os.path.isfile(products_file):
        logger.critical("Products file [%s] does not exist" % products_file)
        sys.exit(1)

    # Verify the results
    if not verify_order_results(env_vars, products_file,
                                master_location, work_location):
        logger.critical("Comparison **** FAILED ****")
        sys.exit(1)

    logger.info("Comparison **** SUCCESSFULL ****")

    # Terminate SUCCESS
    sys.exit(0)
