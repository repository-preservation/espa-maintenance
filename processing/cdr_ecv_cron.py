#! /usr/bin/env python

'''
    FILE: cdr_ecv_cron.py

    PURPOSE: Master run script for new Hadoop jobs.  Queries the xmlrpc
             service to find scenes that need to be processed and
             builds/executes a Hadoop job to process them.

    PROJECT: Land Satellites Data Systems Science Research and Development
             (LSRD) at the USGS EROS

    LICENSE: NASA Open Source Agreement 1.3

    HISTORY:

    Date              Programmer               Reason
    ----------------  ------------------------ --------------------------------
    09/12/2013        David V. Hill            Initial addition of this header.
    Jan/2014          Ron Dilley               Updated for recent processing
                                               enhancements.
'''

import os
import sys
import json
import xmlrpclib
from datetime import datetime
import urllib
import traceback
from argparse import ArgumentParser

# espa-common objects and methods
from espa_constants import *
from espa_logging import log

# local objects and methods
import util
import settings


# ============================================================================
def process_products(args):
    '''
    Description:
      Queries the xmlrpc service to see if there are any products that need
      to be processed with the specified priority and/or user.  If there are,
      this method builds and executes a hadoop job and updates the status for
      each order through the xmlrpc service."
    '''

    rpcurl = os.environ.get('ESPA_XMLRPC')
    server = None

    # Create a server object if the rpcurl seems valid
    if (rpcurl is not None and rpcurl.startswith('http://')
            and len(rpcurl) > 7):

        server = xmlrpclib.ServerProxy(rpcurl, allow_none=True)
    else:
        log("Missing or invalid environment variable ESPA_XMLRPC")

    home_dir = os.environ.get('HOME')
    hadoop_executable = "%s/bin/hadoop/bin/hadoop" % home_dir

    # Verify xmlrpc server
    if server is None:
        log("xmlrpc server was None... exiting")
        sys.exit(EXIT_FAILURE)

    user = server.get_configuration('landsatds.username')
    if len(user) == 0:
        log("landsatds.username is not defined... exiting")
        sys.exit(EXIT_FAILURE)

    pw = urllib.quote(server.get_configuration('landsatds.password'))
    if len(pw) == 0:
        log("landsatds.password is not defined... exiting")
        sys.exit(EXIT_FAILURE)

    host = server.get_configuration('landsatds.host')
    if len(host) == 0:
        log("landsatds.host is not defined... exiting")
        sys.exit(EXIT_FAILURE)

    # adding this so we can disable on-demand processing via the admin console
    ondemand_enabled = server.get_configuration('ondemand_enabled')

    # determine the appropriate priority value to request
    priority = args.priority
    if priority == 'all':
        priority = None  # for 'get_scenes_to_process' None means all

    # determine the appropriate hadoop queue to use
    hadoop_job_queue = settings.HADOOP_QUEUE_MAPPING[args.priority]

    if not ondemand_enabled.lower() == 'true':
        log("on demand disabled...")
        sys.exit(EXIT_SUCCESS)

    try:
        log("Checking for scenes to process...")
        scenes = server.get_scenes_to_process(args.limit, args.user, priority)
        if scenes:
            # Figure out the name of the order file
            stamp = datetime.now()
            espa_job_name = ('%s_%s_%s_%s_%s_%s-espa_job'
                             % (str(stamp.month), str(stamp.day),
                                str(stamp.year), str(stamp.hour),
                                str(stamp.minute), str(stamp.second)))

            log(' '.join(["Found scenes to process,",
                          "generating job number:", espa_job_name]))

            espa_job_filename = '%s%s' % (espa_job_name, '.txt')
            espa_job_filepath = os.path.join('/tmp', espa_job_filename)

            # Create the order file full of all the scenes requested
            with open(espa_job_filepath, 'w+') as espa_fd:
                for scene in scenes:
                    line = json.loads(scene)

                    (orderid, sceneid, options) = (line['orderid'],
                                                   line['scene'],
                                                   line['options'])

                    line['xmlrpcurl'] = rpcurl

                    # Add the usernames and passwords to the options
                    options['source_username'] = user
                    options['destination_username'] = user
                    options['source_pw'] = pw
                    options['destination_pw'] = pw

                    line['options'] = options

                    line_entry = json.dumps(line)
                    log(line_entry)

                    # Pad the entry so hadoop will properly split the jobs
                    filler_count = (settings.ORDER_BUFFER_LENGTH -
                                    len(line_entry))
                    order_line = ''.join([line_entry,
                                          ('#' * filler_count), '\n'])

                    # Write out the order line
                    espa_fd.write(order_line)
                # END - for scene
            # END - with espa_fd

            # Specify the location of the order file on the hdfs
            hdfs_target = 'requests/%s' % espa_job_filename

            # Define command line to store the job file in hdfs
            hadoop_store_command = [hadoop_executable, 'dfs', '-copyFromLocal',
                                    espa_job_filepath, hdfs_target]

            jars = os.path.join(home_dir, 'bin/hadoop/contrib/streaming',
                                'hadoop-streaming*.jar')
            # Define command line to execute the hadoop job
            # Be careful it is possible to have conflicts between module names
            hadoop_run_command = \
                [hadoop_executable, 'jar', jars,
                 '-D', 'mapred.task.timeout=%s' % settings.HADOOP_TIMEOUT,
                 '-D', 'mapred.reduce.tasks=0',
                 '-D', 'mapred.job.queue.name=%s' % hadoop_job_queue,
                 '-D', 'mapred.job.name="%s"' % espa_job_name,
                 '-file', '%s/espa-site/processing/cdr_ecv.py' % home_dir,
                 '-file', ('%s/espa-site/processing/cdr_ecv_mapper.py'
                           % home_dir),
                 '-file', '%s/espa-site/processing/modis.py' % home_dir,
                 '-file', '%s/espa-site/processing/browse.py' % home_dir,
                 '-file', '%s/espa-site/processing/distribution.py' % home_dir,
                 '-file', ('%s/espa-site/processing/espa_exception.py'
                           % home_dir),
                 '-file', '%s/espa-site/processing/metadata.py' % home_dir,
                 '-file', '%s/espa-site/processing/parameters.py' % home_dir,
                 '-file', '%s/espa-site/processing/science.py' % home_dir,
                 '-file', '%s/espa-site/processing/solr.py' % home_dir,
                 '-file', '%s/espa-site/processing/staging.py' % home_dir,
                 '-file', '%s/espa-site/processing/statistics.py' % home_dir,
                 '-file', '%s/espa-site/processing/transfer.py' % home_dir,
                 '-file', '%s/espa-site/processing/util.py' % home_dir,
                 '-file', '%s/espa-site/processing/warp.py' % home_dir,
                 '-file', '%s/espa-site/common/sensor.py' % home_dir,
                 '-file', '%s/espa-site/common/settings.py' % home_dir,
                 '-file', '%s/espa-site/common/utilities.py' % home_dir,
                 '-mapper', ('%s/espa-site/processing/cdr_ecv_mapper.py'
                             % home_dir),
                 '-cmdenv', 'ESPA_WORK_DIR=$ESPA_WORK_DIR',
                 '-cmdenv', 'HOME=$HOME',
                 '-cmdenv', 'USER=$USER',
                 '-cmdenv', 'ANC_PATH=$ANC_PATH',
                 '-cmdenv', 'ESUN=$ESUN',
                 '-input', hdfs_target,
                 '-output', hdfs_target + '-out']

            # Define the executables to clean up hdfs
            hadoop_delete_request_command1 = [hadoop_executable, 'dfs',
                                              '-rmr', hdfs_target]
            hadoop_delete_request_command2 = [hadoop_executable, 'dfs',
                                              '-rmr', hdfs_target + '-out']

            # ----------------------------------------------------------------
            log("Storing request file to hdfs...")
            output = ''
            try:
                cmd = ' '.join(hadoop_store_command)
                print("Store cmd:%s" % cmd)

                output = util.execute_cmd(cmd)
            except Exception, e:
                log("Error storing files to HDFS... exiting")
                sys.exit(EXIT_FAILURE)
            finally:
                if len(output) > 0:
                    log(output)

            # ----------------------------------------------------------------
            # Update the scene list as queued so they don't get pulled down
            # again now that these jobs have been stored in hdfs
            product_list = list()
            for scene in scenes:
                line = json.loads(scene)
                orderid = line['orderid']
                sceneid = line['scene']
                product_list.append((orderid, sceneid))

                log("Adding scene:%s orderid:%s to queued list"
                    % (sceneid, orderid))

            server.queue_products(product_list, 'CDR_ECV cron driver',
                                  espa_job_name)

            log("Deleting local request file copy [%s]" % espa_job_filepath)
            os.unlink(espa_job_filepath)

            # ----------------------------------------------------------------
            log("Running hadoop job...")
            output = ''
            try:
                cmd = ' '.join(hadoop_run_command)
                print("Run cmd:%s" % cmd)
                output = util.execute_cmd(cmd)
            except Exception, e:
                log("Error running Hadoop job...")
            finally:
                if len(output) > 0:
                    log(output)

            # ----------------------------------------------------------------
            log("Deleting hadoop job request file from hdfs....")
            output = ''
            try:
                cmd = ' '.join(hadoop_delete_request_command1)
                output = util.execute_cmd(cmd)
            except Exception, e:
                log("Error deleting hadoop job request file")
            finally:
                if len(output) > 0:
                    log(output)

            # ----------------------------------------------------------------
            log("Deleting hadoop job output...")
            output = ''
            try:
                cmd = ' '.join(hadoop_delete_request_command2)
                output = util.execute_cmd(cmd)
            except Exception, e:
                log("Error deleting hadoop job output")
            finally:
                if len(output) > 0:
                    log(output)

        else:
            log("No scenes to process....")

    except xmlrpclib.ProtocolError, e:
        log("A protocol error occurred: %s" % str(e))
        tb = traceback.format_exc()
        log(tb)

    except Exception, e:
        log("Error Processing Scenes: %s" % str(e))
        tb = traceback.format_exc()
        log(tb)

    finally:
        server = None
# END - process_products


# ============================================================================
if __name__ == '__main__':
    '''
    Description:
      Execute the core processing routine.
    '''

    # Check required variables that this script should fail on if they are not
    # defined
    required_vars = ['ESPA_XMLRPC', 'ESPA_WORK_DIR', 'ANC_PATH', 'PATH',
                     'HOME']
    for env_var in required_vars:
        if (env_var not in os.environ or os.environ.get(env_var) is None
                or len(os.environ.get(env_var)) < 1):

            log("$%s is not defined... exiting" % env_var)
            sys.exit(EXIT_FAILURE)

    # Create a command line argument parser
    description = ("Builds and kicks-off hadoop jobs for the espa processing"
                   " system (to process product requests)")
    parser = ArgumentParser(description=description)

    # Add parameters
    valid_priorities = sorted(settings.HADOOP_QUEUE_MAPPING.keys())
    parser.add_argument('--priority',
                        action='store', dest='priority', required=False,
                        default='all', choices=valid_priorities,
                        help="only process requests with this priority:"
                             " one of (%s)" % ', '.join(valid_priorities))

    parser.add_argument('--limit',
                        action='store', dest='limit', required=False,
                        default='500',
                        help="specify the max number of requests to process")

    parser.add_argument('--user',
                        action='store', dest='user', required=False,
                        default=None,
                        help="only process requests for the specified user")

    # Parse the command line arguments
    args = parser.parse_args()

    # Setup and submit products to hadoop for processing
    process_products(args)

    sys.exit(EXIT_SUCCESS)
