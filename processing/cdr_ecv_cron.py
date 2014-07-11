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
import time
import json
import xmlrpclib
from datetime import datetime
import urllib

# espa-common objects and methods
from espa_constants import *
from espa_logging import log

# local objects and methods
import util
import settings


# ============================================================================
def run_scenes():
    '''
    Description:
      Queries the xmlrpc service to see if there are any scenes that need to
      be processed.  If there are, this method builds and executes a hadoop
      job and updates the xmlrpc service to flag all the scenes as "queued"
    '''

    rpcurl = os.environ.get('ESPA_XMLRPC')
    server = None

    # Create a server object if the rpcurl seems valid
    if (rpcurl is not None and rpcurl.startswith('http://')
            and len(rpcurl) > 7):

        server = xmlrpclib.ServerProxy(rpcurl)
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
    if not ondemand_enabled.lower() == 'true':
        log("on demand disabled...")
        sys.exit(EXIT_SUCCESS)

    try:
        log("Checking for scenes to process...")
        scenes = server.get_scenes_to_process()
        if scenes:
            # Figure out the name of the order file
            stamp = datetime.now()
            ordername = ('%s_%s_%s_%s_%s_%s-espa_job.txt') % \
                        (str(stamp.month), str(stamp.day),
                         str(stamp.year), str(stamp.hour),
                         str(stamp.minute), str(stamp.second))

            log(' '.join(["Found scenes to process,",
                          "generating job number:", ordername]))
            espaorderfile = os.path.join('/tmp', ordername)

            # Create the order file full of all the scenes requested
            with open(espaorderfile, 'w+') as espa_fd:
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
            hdfs_target = 'requests/%s' % ordername

            # Define command line to store the job file in hdfs
            hadoop_store_command = [hadoop_executable, 'dfs', '-copyFromLocal',
                                    espaorderfile, hdfs_target]

            jars = os.path.join(home_dir, 'bin/hadoop/contrib/streaming',
                                'hadoop-streaming*.jar')
            # Define command line to execute the hadoop job
            hadoop_run_command = \
                [hadoop_executable, 'jar', jars,
                 '-D', 'mapred.task.timeout=%s' % settings.HADOOP_TIMEOUT,
                 '-D', 'mapred.reduce.tasks=0',
                 '-D', 'mapred.job.queue.name=ondemand',
                 '-D', 'mapred.job.name="%s"' % ordername,
                 '-file', '%s/espa-site/espa/cdr_ecv.py' % home_dir,
                 '-file', '%s/espa-site/espa/cdr_ecv_mapper.py' % home_dir,
                 '-file', '%s/espa-site/espa/modis.py' % home_dir,
                 '-file', '%s/espa-site/espa/browse.py' % home_dir,
                 '-file', '%s/espa-site/espa/distribution.py' % home_dir,
                 '-file', '%s/espa-site/espa/espa_exception.py' % home_dir,
                 '-file', '%s/espa-site/espa/metadata.py' % home_dir,
                 '-file', '%s/espa-site/espa/parameters.py' % home_dir,
                 '-file', '%s/espa-site/espa/science.py' % home_dir,
                 '-file', '%s/espa-site/espa/solr.py' % home_dir,
                 '-file', '%s/espa-site/espa/staging.py' % home_dir,
                 '-file', '%s/espa-site/espa/statistics.py' % home_dir,
                 '-file', '%s/espa-site/espa/transfer.py' % home_dir,
                 '-file', '%s/espa-site/espa/util.py' % home_dir,
                 '-file', '%s/espa-site/espa/warp.py' % home_dir,
                 '-file', '%s/espa-site/espa/settings.py' % home_dir,
                 '-mapper', '%s/espa-site/espa/cdr_ecv_mapper.py' % home_dir,
                 '-cmdenv', 'ESPA_WORK_DIR=$ESPA_WORK_DIR',
                 '-cmdenv', 'HOME=$HOME',
                 '-cmdenv', 'USER=$USER',
                 '-cmdenv', 'ANC_PATH=$ANC_PATH',
                 '-cmdenv', 'ESUN=$ESUN',
                 '-input', hdfs_target,
                 '-output', hdfs_target, '-out']

            # Define the executables to clean up hdfs
            hadoop_delete_request_command1 = [hadoop_executable, 'dfs',
                                              '-rmr', hdfs_target]
            hadoop_delete_request_command2 = [hadoop_executable, 'dfs',
                                              '-rmr', hdfs_target, '-out']

            # ----------------------------------------------------------------
            log("Storing request file to hdfs...")
            output = ''
            try:
                cmd = ' '.join(hadoop_store_command)
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
            for scene in scenes:
                line = json.loads(scene)
                orderid = line['orderid']
                sceneid = line['scene']
                log("Updating scene:%s orderid:%s to queued" % (sceneid,
                                                                orderid))
                server.update_status(sceneid, orderid,
                                     'CDR_ECV cron driver', 'queued')

            log("Deleting local request file copy...")
            os.unlink(espaorderfile)

            # ----------------------------------------------------------------
            log("Running hadoop job...")
            output = ''
            try:
                cmd = ' '.join(hadoop_run_command)
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

    except Exception, e:
        log("Error Processing Scenes: %s" % str(e))

    finally:
        server = None
# END - run_scenes


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

    run_scenes()

    sys.exit(EXIT_SUCCESS)
