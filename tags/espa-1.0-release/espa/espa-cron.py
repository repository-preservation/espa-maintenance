#!/usr/bin/env python
import xmlrpclib
import time
import sys
import paramiko
from datetime import datetime
import os
from subprocess import *

#correct this - might be ok if apache is always on localhost
#rpcurl = 'http://l8srlscp07.cr.usgs.gov/rpc'
rpcurl = 'http://localhost/rpc'

#correct this
#hadoop_executable = '/home/espa/bin/hadoop/bin/hadoop'

#server = xmlrpclib.ServerProxy(rpcurl)

def runScenes():
    server = xmlrpclib.ServerProxy(rpcurl)
    hadoop_executable = '/home/espa/bin/hadoop/bin/hadoop'
    try:
        #This should return a list of all scenes that were on order
        #but are now on cache.  If this client chooses to process this
        #list it should call updateStatus so the same list is not
        #processed twice.
        print ("Checking for scenes to process...")

        #TODO: Try/Catch this in case we can't contact the server
        scenes = server.getScenesToProcess()
        #if not scenes:
            #print ("No scenes to process.... exiting")
        

        if scenes:
            stamp = datetime.now()
            year = stamp.year
            month = stamp.month
            day = stamp.day
            hour = stamp.hour
            minute = stamp.minute
            second = stamp.second

            ordername = ('%s_%s_%s_%s_%s_%s-espa_job.txt') % (str(month),str(day),str(year),str(hour),str(minute),str(second))

            print ("Found scenes to process, generating job number:" + ordername)
               
            espaorderfile = '/tmp/' + ordername
            #need this to up the size of each entry so mapreduce will properly
            #split and run our jobs
        
            f = open(espaorderfile, 'w+')
            for s in scenes:
                orderid = s[0]
                sceneid = s[1]
                
                #pad the entry to 512 bytes so hadoop will properly split the jobs
                filler = ""
                entry_length = len(orderid.strip()) + len(sceneid.strip()) + len(rpcurl) + 4
                for i in range(1, 512 - entry_length):
                    filler = filler + "#"
                        
                f.write(orderid.strip() + '\t' + sceneid.strip() + '\t' + rpcurl + '\t' + filler +'\n')
            f.close()
            
            #return
                        
            hdfs_target = ' requests/' + ordername
        
            #define executable to store the job file in hdfs
            hadoop_store_command = hadoop_executable + ' dfs -copyFromLocal ' + espaorderfile + hdfs_target

            #print("HDFS store command:%s") % (hadoop_store_command)
            #define the executable to execute the hadoop job
            #had to define the timeouts to a ridiculous number os the jobs don't get killed before they are done.... currently set 
            #to 172800000, which is 2 days

            #hard coded values = MUY MAL
            hadoop_run_command = hadoop_executable + ' jar /home/espa/bin/hadoop/contrib/streaming/hadoop-streaming-0.20.203.0.jar'
            hadoop_run_command = hadoop_run_command + ' -D mapred.task.timeout=172800000'
            #hadoop_run_command = hadoop_run_command + ' -D mapred.map.tasks=20'
            hadoop_run_command = hadoop_run_command + ' -D mapred.reduce.tasks=0'
            #hadoop_run_command = hadoop_run_command + ' -D mapred.tasktracker.map.tasks.maximum=12'
            hadoop_run_command = hadoop_run_command + ' -D mapred.job.name="' + ordername + '"'
            #hadoop_run_command = hadoop_run_command + ' -file /home/espa/espa-site/espa/hadoop/sr_mapper.py '
            hadoop_run_command = hadoop_run_command + ' -file /home/espa/espa-site/espa/espa.py '
            #hadoop_run_command = hadoop_run_command + ' -mapper /home/espa/espa-site/espa/hadoop/sr_mapper.py '
            hadoop_run_command = hadoop_run_command + ' -mapper /home/espa/espa-site/espa/espa.py '
            hadoop_run_command = hadoop_run_command + ' -input ' + hdfs_target + ' '
            hadoop_run_command = hadoop_run_command + ' -output ' + hdfs_target + '-out'
        

            #print ("HDFS execute command:%s") % (hadoop_run_command)
        
            #define the executables to clean up hdfs
            hadoop_delete_request_command1 = hadoop_executable + ' dfs -rmr' + hdfs_target
            hadoop_delete_request_command2 = hadoop_executable + ' dfs -rmr' + hdfs_target + '-out'

            #print ("HDFS delete command 1:%s") % (hadoop_delete_request_command1)
            #print ("HDFS delete command 2:%s") % (hadoop_delete_request_command2)
        
            #copy the request file to hdfs
            print ("Storing request file to hdfs...")
            proc = Popen(hadoop_store_command, stdout=PIPE, stderr=PIPE, shell=True)
            f = proc.stdout
        
            proc.wait()
            f = proc.stdout
            contents = f.read()
            if len(contents) > 0:
                print ("Store request:%s" % contents)
            f.close()
        
            f = proc.stderr
            contents = f.read()
            if len(contents) > 0:
                print ("Store request error:%s" % contents)
            f.close()


            #update the scene list as queued so they don't get pulled down again now that these jobs have been stored
            #in hdfs
            for s in scenes:
                #name, orderid, processing_loc, status
                server.updateStatus(s[1],s[0],'cron driver', 'queued')
        

            print("Deleting local request file copy...")
            #delete the local copy of the request file
            os.unlink(espaorderfile)

            print ("Running hadoop job...")
            #run the hadoop job
            proc = Popen(hadoop_run_command,stdout=PIPE, stderr=PIPE, shell=True)
            proc.wait()
            f = proc.stdout
            contents = f.read()
            if len(contents) > 0:
                print ("Hadoop job:%s" % contents)
            f.close()
        
            f = proc.stderr
            contents = f.read()
            if len(contents) > 0:
                print ("Hadoop job error:%s" % contents)
            f.close()

            print ("Deleting hadoop job request file from hdfs....")
            #delete the hadoop job request file from hdfs
            proc = Popen(hadoop_delete_request_command1, stdout=PIPE, stderr=PIPE, shell=True)
            proc.wait()
            f = proc.stdout
            contents = f.read()
            if len(contents) > 0:
                print ("Hadoop job cleanup:%s" % contents)
            f.close()
        
            f = proc.stderr
            contents = f.read()
            if len(contents) > 0:
                print ("Hadoop job cleanup error:%s" % contents)
            f.close()

            print ("Deleting hadoop job output...")
            #delete the hadoop job output
            proc = Popen(hadoop_delete_request_command2, stdout=PIPE, stderr=PIPE, shell=True)
            proc.wait()
            f = proc.stdout
            contents = f.read()
            if len(contents) > 0:
                print ("Hadoop job output cleanup:%s" % contents)
            f.close()
        
            f = proc.stderr
            contents = f.read()
            if len(contents) > 0:
                print ("Hadoop job output cleanup error:%s" % contents)
            f.close()
        else:
            print ("No scenes to process....")

    except xmlrpclib.ProtocolError, err:
        print ("A protocol error occurred:%s" % err)
    finally:
        server = None
        

def cleanDistroCache():
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
            



        
    
