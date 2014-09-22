#! /usr/bin/env python

'''
License:
  "NASA Open Source Agreement 1.3"

Description:
  Provides methods for creating and distributing products.

History:
  Original Development (cdr_ecv.py) by David V. Hill, USGS/EROS
  Created Jan/2014 by Ron Dilley, USGS/EROS
    - Gutted the original implementation from cdr_ecv.py and placed it in this
      file.
'''

import os
import sys
import glob
from time import sleep
from argparse import ArgumentParser

# espa-common objects and methods
from espa_constants import *

# imports from espa/espa_common
try:
    from espa_logging import EspaLogging
except:
    from espa_common.espa_logging import EspaLogging

try:
    import settings
except:
    from espa_common import settings

try:
    import utilities
except:
    from espa_common import utilities

# local objects and methods
import espa_exception as ee
import parameters
import util
import transfer


# ============================================================================
def build_argument_parser():
    '''
    Description:
      Build the command line argument parser.
    '''

    # Create a command line argument parser
    description = "Provides methods for creating and distributing products"
    parser = ArgumentParser(description=description)

    # Parameters
    parameters.add_debug_parameter(parser)

    parser.add_argument('--test_deliver_product',
                        action='store_true', dest='test_deliver_product',
                        default=False,
                        help="test the delivery code which also tests"
                             " package_product and distribute_product")

    parser.add_argument('--test_package_product',
                        action='store_true', dest='test_package_product',
                        default=False,
                        help="test the packaging code")

    parser.add_argument('--test_distribute_product',
                        action='store_true', dest='test_distribute_product',
                        default=False,
                        help="test the distributing code")

    # Used by package and deliver and distribute
    parameters.add_destination_parameters(parser)

    # Used by package and deliver
    parser.add_argument('--product_name',
                        action='store', dest='product_name', required=False,
                        help="basename of the product to distribute")

    # Used by deliver
    parameters.add_work_directory_parameter(parser)

    parser.add_argument('--package_directory',
                        action='store', dest='package_directory',
                        default=os.curdir,
                        help="package directory on the localhost")

    parser.add_argument('--sleep_seconds',
                        action='store', dest='sleep_seconds',
                        default=settings.DEFAULT_SLEEP_SECONDS,
                        help="number of seconds to sleep after a failure"
                             " before retrying")

    # Used by distribute
    parser.add_argument('--product_file',
                        action='store', dest='product_file', required=False,
                        help="full path of the product to distribute")

    parser.add_argument('--cksum_file',
                        action='store', dest='cksum_file', required=False,
                        help="full path of the checksum file to distribute"
                             " and verify")

    return parser
# END - build_argument_parser


# ============================================================================
def tar_product(product_full_path, product_files):
    '''
    Description:
      Create a tar ball of the specified files.
    '''

    cmd = ['tar', '-cf', '%s.tar' % product_full_path]
    cmd.extend(product_files)
    cmd = ' '.join(cmd)

    output = ''
    try:
        output = utilities.execute_cmd(cmd)
    except Exception, e:
        raise ee.ESPAException(ee.ErrorCodes.packaging_product,
                               str(e)), None, sys.exc_info()[2]
    finally:
        if len(output) > 0:
            logger = EspaLogging.get_logger('espa.processing')
            logger.info(output)
# END - tar_product


# ============================================================================
def gzip_product(product_full_path):
    '''
    Description:
      Create a gzip ball of the specified files.
    '''

    cmd = ' '.join(['gzip', product_full_path])

    output = ''
    try:
        output = utilities.execute_cmd(cmd)
    except Exception, e:
        raise ee.ESPAException(ee.ErrorCodes.packaging_product,
                               str(e)), None, sys.exc_info()[2]
    finally:
        if len(output) > 0:
            logger = EspaLogging.get_logger('espa.processing')
            logger.info(output)
# END - gzip_product


# ============================================================================
def package_product(source_directory, destination_directory, product_name):
    '''
    Description:
      Package the contents of the source directory into a gzipped tarball
      located in the destination directory and generates a checksum file for it

      The filename will be prefixed with the specified product name

    Returns:
      product_full_path - The full path to the product including filename
      cksum_full_path - The full path to the check sum including filename
      cksum_value - The checksum value
    '''

    logger = EspaLogging.get_logger('espa.processing')

    product_full_path = os.path.join(destination_directory, product_name)

    # Remove any old products from the destination directory
    old_product_list = glob.glob("%s*" % product_full_path)
    for old_product in old_product_list:
        os.unlink(old_product)

    # Change to the source directory
    current_directory = os.getcwd()
    os.chdir(source_directory)

    try:
        # Tar the files
        logger.info("Packaging completed product to %s.tar.gz"
                    % product_full_path)

        product_files = glob.glob("*")
        tar_product(product_full_path, product_files)

        # It has the tar extension now
        product_full_path = '%s.tar' % product_full_path

        # Compress the product tar
        gzip_product(product_full_path)

        # It has the gz extension now
        product_full_path = '%s.gz' % product_full_path

        # Change file permissions
        logger.info("Changing file permissions on %s to 0644"
                    % product_full_path)
        os.chmod(product_full_path, 0644)

        # Verify that the archive is good
        output = ''
        cmd = ' '.join(['tar', '-tf', product_full_path])
        try:
            output = utilities.execute_cmd(cmd)
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.packaging_product,
                                   str(e)), None, sys.exc_info()[2]
        finally:
            if len(output) > 0:
                logger.info(output)

        # If it was good then create a checksum file
        cksum_output = ''
        cmd = ' '.join(['cksum', product_full_path])
        try:
            cksum_output = utilities.execute_cmd(cmd)
        except Exception, e:
            if len(cksum_output) > 0:
                logger.info(cksum_output)
            raise ee.ESPAException(ee.ErrorCodes.packaging_product,
                                   str(e)), None, sys.exc_info()[2]

        # Name of the checksum file created
        cksum_filename = "%s.cksum" % product_name
        # Get the base filename of the file that was checksum'd
        cksum_prod_filename = os.path.basename(product_full_path)

        logger.debug("Checksum file = %s" % cksum_filename)
        logger.debug("Checksum'd file = %s" % cksum_prod_filename)

        # Make sure they are strings
        cksum_values = cksum_output.split()
        cksum_value = "%s %s %s" % (str(cksum_values[0]),
                                    str(cksum_values[1]),
                                    str(cksum_prod_filename))
        logger.info("Generating cksum: %s" % cksum_value)

        cksum_full_path = os.path.join(destination_directory, cksum_filename)

        try:
            with open(cksum_full_path, 'wb+') as cksum_fd:
                cksum_fd.write(cksum_value)
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.packaging_product,
                                   "Error building cksum file"), \
                None, sys.exc_info()[2]

    finally:
        # Change back to the previous directory
        os.chdir(current_directory)

    return (product_full_path, cksum_full_path, cksum_value)
# END - package_product


# ============================================================================
def distribute_product(destination_host, destination_directory,
                       destination_username, destination_pw,
                       product_filename, cksum_filename):
    '''
    Description:
      Transfers the product and associated checksum to the specified directory
      on the destination host

    Returns:
      cksum_value - The check sum value from the destination
      destination_product_file - The full path on the destination

    Note:
      - It is assumed ssh has been setup for access between the localhost
        and destination system
    '''

    logger = EspaLogging.get_logger('espa.processing')

    # Create the destination directory on the destination host
    logger.info("Creating destination directory %s on %s"
                % (destination_directory, destination_host))
    cmd = ' '.join(['ssh', '-q', '-o', 'StrictHostKeyChecking=no',
                    destination_host, 'mkdir', '-p', destination_directory])

    output = ''
    try:
        logger.debug(' '.join(["mkdir cmd:", cmd]))
        output = utilities.execute_cmd(cmd)
    except Exception, e:
        raise ee.ESPAException(ee.ErrorCodes.packaging_product,
                               str(e)), None, sys.exc_info()[2]
    finally:
        if len(output) > 0:
            logger.info(output)

    # Figure out the destination full paths
    destination_cksum_file = '%s/%s' \
        % (destination_directory, os.path.basename(cksum_filename))
    destination_product_file = '%s/%s' \
        % (destination_directory, os.path.basename(product_filename))

    # Remove any pre-existing files
    # Grab the first part of the filename, which is not unique
    remote_filename_parts = destination_product_file.split('-')
    remote_filename_parts[-1] = '*'  # Replace the last element of the list
    remote_filename = '-'.join(remote_filename_parts)  # Join with '-'

    cmd = ' '.join(['ssh', '-q', '-o', 'StrictHostKeyChecking=no',
                    destination_host, 'rm', '-f', remote_filename])
    output = ''
    try:
        logger.debug(' '.join(["rm remote file cmd:", cmd]))
        output = utilities.execute_cmd(cmd)
    except Exception, e:
        raise ee.ESPAException(ee.ErrorCodes.packaging_product,
                               str(e)), None, sys.exc_info()[2]
    finally:
        if len(output) > 0:
            logger.info(output)

    # Transfer the checksum file
    transfer.transfer_file('localhost', cksum_filename, destination_host,
                           destination_cksum_file,
                           destination_username=destination_username,
                           destination_pw=destination_pw)

    # Transfer the product file
    transfer.transfer_file('localhost', product_filename, destination_host,
                           destination_product_file,
                           destination_username=destination_username,
                           destination_pw=destination_pw)

    # Get the remote checksum value
    cksum_value = ''
    cmd = ' '.join(['ssh', '-q', '-o', 'StrictHostKeyChecking=no',
                    destination_host, 'cksum', destination_product_file])
    try:
        logger.debug(' '.join(["ssh cmd:", cmd]))
        cksum_value = utilities.execute_cmd(cmd)
    except Exception, e:
        if len(cksum_value) > 0:
            logger.error(cksum_value)
        raise ee.ESPAException(ee.ErrorCodes.packaging_product,
                               str(e)), None, sys.exc_info()[2]

    return (cksum_value, destination_product_file, destination_cksum_file)
# END - distribute_product


# ============================================================================
def distribute_statistics(scene, work_directory,
                          destination_host, destination_directory,
                          destination_username, destination_pw):
    '''
    Description:
      Transfers the statistics to the specified directory on the destination
      host

    Returns:
      cksum_value - The check sum value from the destination
      destination_product_file - The full path on the destination

    Note:
      - It is assumed ssh has been setup for access between the localhost
        and destination system
      - It is assumed a stats directory exists under the current directory
    '''

    logger = EspaLogging.get_logger('espa.processing')

    # Change to the source directory
    current_directory = os.getcwd()
    os.chdir(work_directory)

    try:
        stats_directory = os.path.join(destination_directory, 'stats')
        stats_files = ''.join(['stats/', scene, '*'])

        # Create the stats directory on the destination host
        logger.info("Creating stats directory %s on %s"
                    % (stats_directory, destination_host))
        cmd = ' '.join(['ssh', '-q', '-o', 'StrictHostKeyChecking=no',
                        destination_host, 'mkdir', '-p', stats_directory])

        output = ''
        try:
            logger.debug(' '.join(["mkdir cmd:", cmd]))
            output = utilities.execute_cmd(cmd)
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.packaging_product,
                                   str(e)), None, sys.exc_info()[2]
        finally:
            if len(output) > 0:
                logger.info(output)

        # Remove any pre-existing stats
        cmd = ' '.join(['ssh', '-q', '-o', 'StrictHostKeyChecking=no',
                        destination_host, 'rm', '-f',
                        '%s/%s*' % (stats_directory, scene)])
        output = ''
        try:
            logger.debug(' '.join(["rm remote stats cmd:", cmd]))
            output = utilities.execute_cmd(cmd)
        except Exception, e:
            raise ee.ESPAException(ee.ErrorCodes.packaging_product,
                                   str(e)), None, sys.exc_info()[2]
        finally:
            if len(output) > 0:
                logger.info(output)

        # Transfer the stats files
        transfer.transfer_file('localhost', stats_files, destination_host,
                               stats_directory,
                               destination_username=destination_username,
                               destination_pw=destination_pw)

        logger.info("Verifying statistics transfers")
        # NOTE - Re-purposing the stats_files variable
        stats_files = glob.glob(stats_files)
        for file_name in stats_files:
            local_cksum_value = 'a b c'
            remote_cksum_value = 'b c d'

            # Generate a local checksum value
            cmd = ' '.join(['cksum', file_name])
            try:
                logger.debug(' '.join(["cksum cmd:", cmd]))
                local_cksum_value = utilities.execute_cmd(cmd)
            except Exception, e:
                if len(local_cksum_value) > 0:
                    logger.error(local_cksum_value)
                raise ee.ESPAException(ee.ErrorCodes.packaging_product,
                                       str(e)), None, sys.exc_info()[2]

            # Generate a remote checksum value
            remote_file = os.path.join(destination_directory, file_name)
            cmd = ' '.join(['ssh', '-q', '-o', 'StrictHostKeyChecking=no',
                            destination_host, 'cksum', remote_file])
            try:
                remote_cksum_value = utilities.execute_cmd(cmd)
            except Exception, e:
                if len(remote_cksum_value) > 0:
                    logger.error(remote_cksum_value)
                raise ee.ESPAException(ee.ErrorCodes.packaging_product,
                                       str(e)), None, sys.exc_info()[2]

            # Checksum validation
            if local_cksum_value.split()[0] != remote_cksum_value.split()[0]:
                raise ee.ESPAException(ee.ErrorCodes.verifing_checksum,
                                       "Failed checksum validation between"
                                       " %s and %s:%s" % (file_name,
                                                          destination_host,
                                                          remote_file))

    finally:
        # Change back to the previous directory
        os.chdir(current_directory)
# END - distribute_statistics


# ============================================================================
def deliver_product(scene, work_directory, package_directory, product_name,
                    destination_host, destination_directory,
                    destination_username, destination_pw,
                    include_statistics=False,
                    sleep_seconds=settings.DEFAULT_SLEEP_SECONDS):
    '''
    Description:
      Packages the product and distributes it to the destination.
      Verification of the local and remote checksum values is performed.

    Note:
        X attempts are made for each part of the delivery
    '''

    logger = EspaLogging.get_logger('espa.processing')

    # Package the product files
    # Attempt X times sleeping between each attempt
    attempt = 0
    while True:
        try:
            (product_full_path, cksum_full_path, local_cksum_value) = \
                package_product(work_directory, package_directory,
                                product_name)
        except Exception, e:
            logger.error("An exception occurred processing %s"
                         % product_name)
            logger.error("Exception Message: %s" % str(e))
            if attempt < settings.MAX_PACKAGING_ATTEMPTS:
                sleep(sleep_seconds)  # sleep before trying again
                attempt += 1
                continue
            else:
                raise ee.ESPAException(ee.ErrorCodes.packaging_product,
                                       str(e)), None, sys.exc_info()[2]
        break

    # Distribute the product
    # Attempt X times sleeping between each attempt
    attempt = 0
    while True:
        try:
            (remote_cksum_value, destination_product_file,
             destination_cksum_file) = \
                distribute_product(destination_host, destination_directory,
                                   destination_username, destination_pw,
                                   product_full_path, cksum_full_path)
        except Exception, e:
            logger.error("An exception occurred processing %s"
                         % product_name)
            logger.error("Exception Message: %s" % str(e))
            if attempt < settings.MAX_DELIVERY_ATTEMPTS:
                sleep(sleep_seconds)  # sleep before trying again
                attempt += 1
                continue
            else:
                raise ee.ESPAException(ee.ErrorCodes.distributing_product,
                                       str(e)), None, sys.exc_info()[2]
        break

    # Checksum validation
    if local_cksum_value.split()[0] != remote_cksum_value.split()[0]:
        raise ee.ESPAException(ee.ErrorCodes.verifing_checksum,
                               "Failed checksum validation between"
                               " %s and %s:%s" % (product_full_path,
                                                  destination_host,
                                                  destination_product_file))

    # Distribute the statistics directory if they were generated
    if include_statistics:
        # Attempt X times sleeping between each attempt
        attempt = 0
        while True:
            try:
                distribute_statistics(scene, work_directory,
                                      destination_host, destination_directory,
                                      destination_username, destination_pw)
            except Exception, e:
                logger.error("An exception occurred processing %s"
                             % product_name)
                logger.error("Exception Message: %s" % str(e))
                if attempt < settings.MAX_DELIVERY_ATTEMPTS:
                    sleep(sleep_seconds)  # sleep before trying again
                    attempt += 1
                    continue
                else:
                    raise ee.ESPAException(ee.ErrorCodes.distributing_product,
                                           str(e)), None, sys.exc_info()[2]
            break

        logger.info("Statistics distribution complete for %s" % product_name)

    logger.info("Product delivery complete for %s:%s"
                % (destination_host, destination_product_file))

    # Let the caller know where we put these on the destination system
    return (destination_product_file, destination_cksum_file)
# END - deliver_product


# ============================================================================
if __name__ == '__main__':
    '''
    Description:
      Read parameters from the command line and pass them to the main
      delivery routine.
    '''

    # Build the command line argument parser
    parser = build_argument_parser()

    # Parse the command line arguments
    args = parser.parse_args()

    # Configure logging
    EspaLogging.configure('espa.processing', order='test',
                          product='product', debug=args.debug)
    logger = EspaLogging.get_logger('espa.processing')

    try:
        # Test requested routine
        if args.test_deliver_product:

            if not args.product_name:
                raise Exception("Missing required product_name argument")

            deliver_product(args.work_directory, args.package_directory,
                            args.product_name, args.destination_host,
                            args.destination_directory, args.destination_host,
                            args.destination_pw, args.sleep_seconds)

            logger.info("Successfully delivered product %s"
                        % args.product_name)

        elif args.test_package_product:
            (product_full_path, cksum_full_path, cksum_value) = \
                package_product(args.work_directory,
                                args.destination_directory,
                                args.product_name)

            logger.info("Product Path: %s" % product_full_path)
            logger.info("Checksum Path: %s" % cksum_full_path)
            logger.info("Checksum Value: %s" % cksum_value)
            logger.info("Successfully packaged product %s"
                        % args.product_name)

        elif args.test_distribute_product:
            (cksum_value, destination_full_path, destination_cksum_file) = \
                distribute_product(args.destination_host,
                                   args.destination_directory,
                                   args.destination_host,
                                   args.destination_pw,
                                   args.product_file,
                                   args.cksum_file)
            logger.info("Successfully distributed product %s"
                        % args.product_file)

    except Exception, e:
        if hasattr(e, 'output'):
            logger.error("Output [%s]" % e.output)
        logger.exception("Processing failed")
        sys.exit(EXIT_FAILURE)

    sys.exit(EXIT_SUCCESS)
