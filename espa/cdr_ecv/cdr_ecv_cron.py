#!/usr/local/bin/python

__author__ = "David V. Hill"
__license__ = "NASA Open Source Agreement 1.3"

import xmlrpclib
import time
import sys
from datetime import datetime
import os
import commands
import json

#set required variables that this script should fail on if they are not defined
required_vars = ('ESPA_XMLRPC', "ESPA_WORK_DIR", "ANC_PATH", "PATH", "HOME")
for r in required_vars:
    if not os.environ.has_key(r) or os.environ.get(r) is None or len(os.environ.get(r)) < 1:
        print ("$%s is not defined... exiting" % r)
        sys.exit(-1)

rpcurl = os.environ.get("ESPA_XMLRPC")

def runScenes(): 
    home_dir = os.environ['HOME']
    server = xmlrpclib.ServerProxy(rpcurl)
    hadoop_executable = "%s/bin/hadoop/bin/hadoop" % home_dir
    
    try:
        print ("Checking for scenes to process...")
        scenes = server.getScenesToProcess()
        if scenes:
            stamp = datetime.now()
            year,month,day = stamp.year,stamp.month,stamp.day
            hour,minute,second = stamp.hour,stamp.minute,stamp.second
            ordername = ('%s_%s_%s_%s_%s_%s-espa_job.txt') % (str(month),str(day),str(year),str(hour),str(minute),str(second))
            print ("Found scenes to process, generating job number:" + ordername)   
            espaorderfile = '/tmp/' + ordername
            
            f = open(espaorderfile, 'w+')
            for s in scenes:
                line = json.loads(s)
                orderid,sceneid,options = line['orderid'],line['scene'],line['options']
                line['xmlrpcurl'] = rpcurl 
                line_entry = json.dumps(line)
                               
                #pad the entry to 512 bytes so hadoop will properly split the jobs
                filler = ""
                entry_length = len(line_entry)
                for i in range(1, 512 - entry_length):
                    filler = filler + "#"
                order_line = line_entry + filler + '\n'
                f.write(order_line)
            f.close()
    
            hdfs_target = ' requests/' + ordername
        
            #define executable to store the job file in hdfs
            hadoop_store_command = hadoop_executable + ' dfs -copyFromLocal ' + espaorderfile + hdfs_target

    
            #define the executable to execute the hadoop job
            #had to define the timeouts to a ridiculous number os the jobs don't get killed before they are done.... currently set 
            #to 172800000, which is 2 days
            hadoop_run_command = hadoop_executable + ' jar %s/bin/hadoop/contrib/streaming/hadoop-streaming-0.20.203.0.jar' % home_dir
            hadoop_run_command = hadoop_run_command + ' -D mapred.task.timeout=172800000'
            hadoop_run_command = hadoop_run_command + ' -D mapred.reduce.tasks=0'
            hadoop_run_command = hadoop_run_command + ' -D mapred.job.queue.name=ondemand'
            hadoop_run_command = hadoop_run_command + ' -D mapred.job.name="' + ordername + '"'
            hadoop_run_command = hadoop_run_command + ' -file %s/espa-site/espa/mapper.py ' % home_dir
            hadoop_run_command = hadoop_run_command + ' -file %s/espa-site/espa/espa.py ' % home_dir
            hadoop_run_command = hadoop_run_command + ' -file %s/espa-site/espa/frange.py ' % home_dir
            hadoop_run_command = hadoop_run_command + ' -mapper %s/espa-site/espa/mapper.py ' % home_dir
            hadoop_run_command = hadoop_run_command + ' -cmdenv ESPA_WORK_DIR=$ESPA_WORK_DIR'
            hadoop_run_command = hadoop_run_command + ' -cmdenv HOME=$HOME'
            hadoop_run_command = hadoop_run_command + ' -cmdenv USER=$USER'

            #hadoop_run_command = hadoop_run_command + ' -cmdenv PATH=$PATH'
            hadoop_run_command = hadoop_run_command + ' -cmdenv ANC_PATH=$ANC_PATH'
            hadoop_run_commnad = hadoop_run_command + ' -cmdenv ESUN=$ESUN'
            hadoop_run_command = hadoop_run_command + ' -input ' + hdfs_target + ' '
            hadoop_run_command = hadoop_run_command + ' -output ' + hdfs_target + '-out'
        
            #define the executables to clean up hdfs
            hadoop_delete_request_command1 = hadoop_executable + ' dfs -rmr ' + hdfs_target
            hadoop_delete_request_command2 = hadoop_executable + ' dfs -rmr ' + hdfs_target + '-out'

            print ("Storing request file to hdfs...")
            status,output = commands.getstatusoutput(hadoop_store_command)
            if status != 0:
                print ("Error storing files to HDFS... exiting")
                print output
                sys.exit(1)
                     
            #update the scene list as queued so they don't get pulled down again now that these jobs have been stored
            #in hdfs
            for s in scenes:
                line = json.loads(s)
                orderid = line['orderid']
                sceneid = line['scene']
                print ("updating scene:%s orderid:%s to queued" % (sceneid, orderid))
                server.updateStatus(sceneid, orderid,'cron driver', 'queued')
                        
            print("Deleting local request file copy...")
            os.unlink(espaorderfile)

            print ("Running hadoop job...")
            status,output = commands.getstatusoutput(hadoop_run_command)
            print output
            if status != 0:
                print ("Error running Hadoop job...")
                print output
                
            print ("Deleting hadoop job request file from hdfs....")
            status,output = commands.getstatusoutput(hadoop_delete_request_command1)
            if status != 0:
                print ("Error deleting hadoop job request file")
                print output
                
            print ("Deleting hadoop job output...")
            status,output = commands.getstatusoutput(hadoop_delete_request_command2)
            if status != 0:
                print ("Error deleting hadoop job output")
                print output
                
        else:
            print ("No scenes to process....")

    except xmlrpclib.ProtocolError, err:
        print ("A protocol error occurred:%s" % err)
    finally:
        server = None
        

def cleanDistroCache():
    '''Removes completed orders from the ordering database
       older than 15 days (since order completion) and places
       entries for each order/scene into our data warehouse
    '''
    server = xmlrpclib.ServerProxy(rpcurl)
    scenes_with_paths = server.getScenesToPurge()
    if scenes_with_paths:
        for s in scenes_with_paths:
            pass
            #clean it
            #server.updateStatus(scene, 'Purged')
    else:
        print("No scenes to purge...")

def usage():
    print 'espa-cron.py run-scenes | clean-cache'
    
if __name__ == '__main__':
    if len(sys.argv) != 2:
        usage()
       
    else:
        op = sys.argv[1]
        if op == 'run-scenes':
            runScenes()
            
        elif op == 'clean-cache':
            cleanDistroCache()
          
        else:
            usage()
            



        
    
