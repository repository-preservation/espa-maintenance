#! /usr/bin/env python

'''
    FILE: lpcs_cron.py

    PURPOSE: Queries the xmlrpc service to find orders that need to be
             processed and then builds/executes a Hadoop job to process them.

    PROJECT: Land Satellites Data Systems Science Research and Development
             (LSRD) at the USGS EROS

    LICENSE: NASA Open Source Agreement 1.3

    HISTORY:

    Date              Programmer               Reason
    ----------------  ------------------------ -------------------------------
    Feb/2014          Ron Dilley               Initial implementation
    Sept/2014         Ron Dilley               Updated to use espa_common and
                                               our python logging setup
                                               Updated to use Hadoop
'''

import os
import sys
import json
import xmlrpclib

# espa-common objects and methods
from espa_constants import EXIT_FAILURE
from espa_constants import EXIT_SUCCESS

# imports from espa/espa_common
from espa_common import settings, utilities
from espa_common.espa_logging import EspaLogging

LOGGER_NAME = 'espa.cron.plot'


# ============================================================================
def process_plot_requests():
    '''
    Description:
      Queries the xmlrpc service to see if there are any scenes that need to
      be processed.  If there are, this method builds and executes a plot job
      and updates the status in the database through the xmlrpc service.
    '''

    # Get the logger for this task
    logger = EspaLogging.get_logger(LOGGER_NAME)

    rpcurl = os.environ.get('ESPA_XMLRPC')
    server = None

    # Create a server object if the rpcurl seems valid
    if (rpcurl is not None and rpcurl.startswith('http://')
            and len(rpcurl) > 7):

        server = xmlrpclib.ServerProxy(rpcurl)
    else:
        raise Exception("Missing or invalid environment variable ESPA_XMLRPC")

    home_dir = os.environ.get('HOME')
    hadoop_executable = "%s/bin/hadoop/bin/hadoop" % home_dir

    # Verify xmlrpc server
    if server is None:
        msg = "xmlrpc server was None... exiting"
        raise Exception(msg)

    user = server.get_configuration('landsatds.username')
    if len(user) == 0:
        msg = "landsatds.username is not defined... exiting"
        raise Exception(msg)

    pw = urllib.quote(server.get_configuration('landsatds.password'))
    if len(pw) == 0:
        msg = "landsatds.password is not defined... exiting"
        raise Exception(msg)

    host = server.get_configuration('landsatds.host')
    if len(host) == 0:
        msg = "landsatds.host is not defined... exiting"
        raise Exception(msg)

    # Use ondemand_enabled to determine if we should be processing or not
    ondemand_enabled = server.get_configuration('ondemand_enabled')

    # Plotting doesn't request by priority but we may want to change the
    # queue it uses
    priority = args.priority
    if priority == 'all':
        priority = 'high'  # If all was specified default to the high queue

    # Use the high queue for now if it is determined later that we need a more
    # specialized queue, one will need to be created.
    hadoop_job_queue = settings.HADOOP_QUEUE_MAPPING[priority]

    if not ondemand_enabled.lower() == 'true':
        raise Exception("on demand disabled... exiting")

    try:
        logger.info("Checking for requests to process...")
        orders = server.get_scenes_to_process(args.limit, args.user, None,
                                              ['plot'])

        if orders:
            # Figure out the name of the job file
            stamp = datetime.now()
            job_name = ('%s_%s_%s_%s_%s_%s-%s-espa_job'
                        % (str(stamp.month), str(stamp.day),
                           str(stamp.year), str(stamp.hour),
                           str(stamp.minute), str(stamp.second),
                           str(priority)))

            logger.info("Found requests to process:")

            job_filename = '%s%s' % (job_name, '.txt')
            job_filepath = os.path.join('/tmp', job_filename)

            # Create the requests file full of all the orders requested
            with open(job_filepath, 'w+') as job_fd:
                for request in orders:
                    line = json.loads(request)

                    (orderid, product_type, options) = (line['orderid'],
                                                        line['product_type'],
                                                        line['options'])

                    line['xmlrpcurl'] = rpcurl

                    # Add the usernames and passwords to the options
                    options['source_username'] = user
                    options['destination_username'] = user
                    options['source_pw'] = pw
                    options['destination_pw'] = pw

                    line['options'] = options

                    line_entry = json.dumps(line)
                    logger.info(line_entry)

                    # Pad the entry so hadoop will properly split the jobs
                    filler_count = (settings.ORDER_BUFFER_LENGTH -
                                    len(line_entry))
                    order_line = ''.join([line_entry,
                                          ('#' * filler_count), '\n'])

                    # Write out the request line
                    job_fd.write(order_line)
                # END - for scene
            # END - with espa_fd

            # Specify the location of the requests file on the hdfs
            hdfs_target = 'requests/%s' % job_filename

            # Define command line to store the job file in hdfs
            hadoop_store_command = [hadoop_executable, 'dfs', '-copyFromLocal',
                                    job_filepath, hdfs_target]

            jars = os.path.join(home_dir, 'bin/hadoop/contrib/streaming',
                                'hadoop-streaming*.jar')

            # Define command line to execute the hadoop job
            # Be careful it is possible to have conflicts between module names
            hadoop_run_command = \
                [hadoop_executable, 'jar', jars,
                 '-D', 'mapred.task.timeout=%s' % settings.HADOOP_TIMEOUT,
                 '-D', 'mapred.reduce.tasks=0',
                 '-D', 'mapred.job.queue.name=%s' % hadoop_job_queue,
                 '-D', 'mapred.job.name="%s"' % job_name,
                 '-file', '%s/espa-site/processing/cdr_ecv.py' % home_dir,
                 '-file', ('%s/espa-site/processing/lpcs_mapper.py'
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
                 '-file', ('%s/espa-site/espa_common/espa_logging.py'
                           % home_dir),
                 '-file', '%s/espa-site/espa_common/sensor.py' % home_dir,
                 '-file', '%s/espa-site/espa_common/settings.py' % home_dir,
                 '-file', '%s/espa-site/espa_common/utilities.py' % home_dir,
                 '-mapper', ('%s/espa-site/processing/lpcs_mapper.py'
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
            logger.info("Storing request file to hdfs...")
            output = ''
            try:
                cmd = ' '.join(hadoop_store_command)
                logger.info("Store cmd:%s" % cmd)

                output = utilities.execute_cmd(cmd)
            except Exception, e:
                msg = "Error storing files to HDFS... exiting"
                raise Exception(msg)
            finally:
                if len(output) > 0:
                    logger.info(output)

            # ----------------------------------------------------------------
            # Update the scene list as queued so they don't get pulled down
            # again now that these jobs have been stored in hdfs
            product_list = list()
            for scene in scenes:
                line = json.loads(scene)
                orderid = line['orderid']
                product_type = line['product_type']
                product_list.append((orderid, product_type))

                logger.info("Adding product_type:%s orderid:%s to queued list"
                            % (product_type, orderid))

            server.queue_products(product_list, 'CDR_ECV cron driver',
                                  espa_job_name)

            logger.info("Deleting local request file copy [%s]"
                        % espa_job_filepath)
            os.unlink(espa_job_filepath)

            # ----------------------------------------------------------------
            logger.info("Running hadoop job...")
            output = ''
            try:
                cmd = ' '.join(hadoop_run_command)
                logger.info("Run cmd:%s" % cmd)

                output = utilities.execute_cmd(cmd)
            except Exception, e:
                logger.exception("Error running Hadoop job...")
            finally:
                if len(output) > 0:
                    logger.info(output)

            # ----------------------------------------------------------------
            logger.info("Deleting hadoop job request file from hdfs....")
            output = ''
            try:
                cmd = ' '.join(hadoop_delete_request_command1)
                output = utilities.execute_cmd(cmd)
            except Exception, e:
                logger.exception("Error deleting hadoop job request file")
            finally:
                if len(output) > 0:
                    logger.info(output)

            # ----------------------------------------------------------------
            logger.info("Deleting hadoop job output...")
            output = ''
            try:
                cmd = ' '.join(hadoop_delete_request_command2)
                output = utilities.execute_cmd(cmd)
            except Exception, e:
                logger.exception("Error deleting hadoop job output")
            finally:
                if len(output) > 0:
                    logger.info(output)

        else:
            logger.info("No requests to process:")

    except xmlrpclib.ProtocolError, e:
        logger.exception("A protocol error occurred")

    except Exception, e:
        logger.exception("Error Processing Plots")

    finally:
        server = None
# END - process_plot_requests


# ============================================================================
if __name__ == '__main__':
    '''
    Description:
      Execute the core processing routine.
    '''

    # Create a command line argument parser
    description = ("Builds and kicks-off hadoop jobs for the espa processing"
                   " system (to process plot requests)")
    parser = ArgumentParser(description=description)

    # Add parameters
    valid_priorities = sorted(settings.HADOOP_QUEUE_MAPPING.keys())
    parser.add_argument('--priority',
                        action='store', dest='priority', required=True,
                        choices=valid_priorities,
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

    # Configure and get the logger for this task
    EspaLogging.configure(LOGGER_NAME)
    logger = EspaLogging.get_logger(LOGGER_NAME)

    # Check required variables that this script should fail on if they are not
    # defined
    required_vars = ('ESPA_XMLRPC', "ESPA_WORK_DIR", "ANC_PATH", "PATH",
                     "HOME")
    for env_var in required_vars:
        if (env_var not in os.environ or os.environ.get(env_var) is None
                or len(os.environ.get(env_var)) < 1):

            logger.critical("$%s is not defined... exiting" % env_var)
            sys.exit(EXIT_FAILURE)

    try:
        process_plot_requests(args)
    except Exception, e:
        logger.exception("Processing failed")
        sys.exit(EXIT_FAILURE)

    sys.exit(EXIT_SUCCESS)
