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
def runScenes():
    '''
    Description:
      Queries the xmlrpc service to see if there are any scenes that need to
      be processed.  If there are, this method builds and executes a hadoop
      job and updates the xmlrpc service to flag all the scenes as "queued"
    '''

    rpcurl = os.environ.get('ESPA_XMLRPC')
    home_dir = os.environ.get('HOME')
    server = xmlrpclib.ServerProxy(rpcurl)
    hadoop_executable = "%s/bin/hadoop/bin/hadoop" % home_dir

    # Verify xmlrpc server
    if server is None:
        log("xmlrpc server was None... exiting")
        sys.exit(EXIT_FAILURE)

    user = server.getConfiguration('landsatds.usrname')
    if len(user) == 0:
        log("landsatds.username is not defined... exiting")
        sys.exit(EXIT_FAILURE)

    pw = urllib.quote(server.getConfiguration('landsatds.password'))
    if len(pw) == 0:
        log("landsatds.password is not defined... exiting")
        sys.exit(EXIT_FAILURE)

    host = server.getConfiguration('landsatds.host')
    if len(host) == 0:
        log("landsatds.host is not defined... exiting")
        sys.exit(EXIT_FAILURE)

    try:
        log("Checking for scenes to process...")
        scenes = server.getScenesToProcess()
        if scenes:
            # Figure out the name of the order file
            stamp = datetime.now()
            ordername = ('%s_%s_%s_%s_%s_%s-espa_job.txt') % \
                        (str(stamp.month), str(stamp.day),
                         str(stamp.year), str(stamp.hour),
                         str(stamp.minute), str(stamp.second))

            log("Found scenes to process, generating job number:" + ordername)
            espaorderfile = '/tmp/' + ordername

            # Create the order file full of all the scenes requested
            fd = open(espaorderfile, 'w+')
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
                filler = ""
                filler_count = settings.order_buffer_length - len(line_entry)

                # Have to start at 1 here because the \n will be part of the
                # overall buffer bytes.
                filler = ''.join(['#' for count in range(1, filler_count)])
                order_line = line_entry + filler + '\n'
                fd.write(order_line)
            fd.close()

            # Specify the location of the order file on the hdfs
            hdfs_target = 'requests/%s' % ordername

            # Define command line to store the job file in hdfs
            hadoop_store_command = [hadoop_executable, 'dfs', '-copyFromLocal',
                                    espaorderfile, hdfs_target]

            jars = home_dir + \
                '/bin/hadoop/contrib/streaming/hadoop-streaming*.jar'
            # Define command line to execute the hadoop job
            hadoop_run_command = \
                [hadoop_executable, 'jar', jars,
                 '-D', 'mapred.task.timeout=%s' % settings.hadoop_timeout,
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
            try:
                cmd = ' '.join(hadoop_store_command)
                output = util.execute_cmd(cmd)
            except Exception, e:
                log("Error storing files to HDFS... exiting")
                sys.exit(EXIT_FAILURE)
            finally:
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
                server.updateStatus(sceneid, orderid,
                                    'CDR_ECV cron driver', 'queued')

            log("Deleting local request file copy...")
            os.unlink(espaorderfile)

            # ----------------------------------------------------------------
            log("Running hadoop job...")
            try:
                cmd = ' '.join(hadoop_run_command)
                output = util.execute_cmd(cmd)
            except Exception, e:
                log("Error running Hadoop job...")
            finally:
                log(output)

            # ----------------------------------------------------------------
            log("Deleting hadoop job request file from hdfs....")
            try:
                cmd = ' '.join(hadoop_delete_request_command1)
                output = util.execute_cmd(cmd)
            except Exception, e:
                log("Error deleting hadoop job request file")
            finally:
                log(output)

            # ----------------------------------------------------------------
            log("Deleting hadoop job output...")
            try:
                cmd = ' '.join(hadoop_delete_request_command2)
                output = util.execute_cmd(cmd)
            except Exception, e:
                log("Error deleting hadoop job output")
            finally:
                log(output)

        else:
            log("No scenes to process....")

    except xmlrpclib.ProtocolError, err:
        log("A protocol error occurred:%s" % err)
    finally:
        server = None


# ============================================================================
def cleanDistroCache():
    '''
    Description:
      Removes completed orders from the ordering database older than 15 days
      (since order completion) and places entries for each order/scene into
      our data warehouse
    '''

    rpcurl = os.environ.get('ESPA_XMLRPC')
    server = xmlrpclib.ServerProxy(rpcurl)
    scenes_with_paths = server.getScenesToPurge()
    if scenes_with_paths:
        for scene in scenes_with_paths:
            pass
            # clean it
            # server.updateStatus(scene, 'Purged')
    else:
        log("No scenes to purge...")


# ============================================================================
def usage():
    '''
    Description:
      Display the usage string to the user
    '''

    print ("Usage:")
    print ("\tcdr_ecv_cron.py run-scenes | clean-cache")


# ============================================================================
if __name__ == '__main__':
    '''
    Description:
      Read the command line and execute accordingly.
    '''

    if len(sys.argv) != 2:
        usage()
        sys.exit(EXIT_FAILURE)

    # Check required variables that this script should fail on if they are not
    # defined
    required_vars = ['ESPA_XMLRPC', "ESPA_WORK_DIR", "ANC_PATH", "PATH",
                     "HOME"]
    for env_var in required_vars:
        if env_var not in os.environ or os.environ.get(env_var) is None \
           or len(os.environ.get(env_var)) < 1:
            log("$%s is not defined... exiting" % env_var)
            sys.exit(-1)

    op = sys.argv[1]
    if op == 'run-scenes':
        runScenes()

    elif op == 'clean-cache':
        cleanDistroCache()

    else:
        usage()

    sys.exit(EXIT_SUCCESS)
