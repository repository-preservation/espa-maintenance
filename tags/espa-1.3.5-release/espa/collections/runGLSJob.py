#!/usr/bin/env python

from subprocess import *
import time
import sys
import paramiko
from datetime import datetime
import os


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: runGLSJob.py inputfile glsyear collection_name")
    else:
        hadoop_executable = '/home/espa/bin/hadoop/bin/hadoop'
        
        inputfile = sys.argv[1]
        if not os.path.exists(inputfile):
            print("%s does not exist") % inputfile
            exit(1)

        glsyear = sys.argv[2]

        collection_name = sys.argv[3]

        ordername = 'gls_' + glsyear

        inhandle = open(inputfile, 'r+')
        scenes = inhandle.readlines()
        inhandle.close()

        espaorderfile = '/tmp/' + 'gls_' + 'glsyear'
       
        f = open(espaorderfile, 'w+')

        for s in scenes:

            if len(s) < 2:
                continue

            
            s = s.replace('\n', '').strip()
            #parts = s.split('\t')

            #if len(parts) != 3:
            #    print "couldn't process line -->"
            #    print parts
            #    continue

            sceneid = s
            collection = collection_name
            year = glsyear
                
            #pad the entry to 512 bytes so hadoop will properly split the jobs
            filler = ""
            entry_length = len(sceneid.strip()) + len(collection.strip()) + len(year) + 4
            for i in range(1, 512 - entry_length):
                filler = filler + "#"                    

            f.write(sceneid.strip() + '\t' + collection.strip() + '\t' + year + '\t' + filler +'\n')
        f.close()
                                    
        hdfs_target = ' glsrequests/' + 'gls_' + glsyear
        
        #define executable to store the job file in hdfs
        hadoop_store_command = hadoop_executable + ' dfs -copyFromLocal ' + espaorderfile + hdfs_target

        #print("HDFS store command:%s") % (hadoop_store_command)
        #define the executable to execute the hadoop job
        #had to define the timeouts to a ridiculous number os the jobs don't get killed before they are done.... currently set 
        #to 172800000, which is 2 days

        
        #hard coded values = MUY MAL
        hadoop_run_command = hadoop_executable + ' jar /home/espa/bin/hadoop/contrib/streaming/hadoop-streaming-0.20.203.0.jar'
        hadoop_run_command = hadoop_run_command + ' -Dmapred.task.timeout=345600000'
        hadoop_run_command = hadoop_run_command + ' -Dmapred.reduce.tasks=0'
        hadoop_run_command = hadoop_run_command + ' -Dmapred.job.name="' + ordername + '"'
        hadoop_run_command = hadoop_run_command + ' -Dmapred.fairscheduler.pool=collection_pool'
        hadoop_run_command = hadoop_run_command + ' -file /home/espa/espa-site/espa/collections/espacollections.py'
        hadoop_run_command = hadoop_run_command + ' -mapper /home/espa/espa-site/espa/collections/espacollections.py'
        hadoop_run_command = hadoop_run_command + ' -input ' + hdfs_target
        hadoop_run_command = hadoop_run_command + ' -output ' + hdfs_target + '-out'
        
        print hadoop_run_command                     
                   
        hadoop_delete_request_command2 = hadoop_executable + ' dfs -rmr ' + hdfs_target + '-out'

        print ("HDFS delete command 2:%s") % (hadoop_delete_request_command2)
        
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
