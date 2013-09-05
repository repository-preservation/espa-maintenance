/*
  Name: lsrd_ondemand_metrics.pig

  Description:  Pig script to run on the LSRD/ESPA hadoop cluster to
                generate monthly metrics for LEDAPS ondemand orders
                submitted through EPSA interface and/or GLOVIS.
 
                In the future, this will probably support the other
                CDR/ECV distributions.

                So far, collecting:

                  * Number of overall downloaded ondemand scenes
                  * Volume of overall successfully distributed ordered 
                    ondemand scenes

                Going to implement:

                  * Unique downloader count (when I Landsat to fix their damn logs)

                To execute:

                  * With Hadoop cluster:
                      - set environment variable HADOOP_INSTALL 
                      - Upload logfiles to HDFS with 'hadoop fs -copyFromLocal' command
                      - Run: pig -l /path/to/pig.log -f /path/to/lsrd_collection_metrics.pig

                  * Without Hadoop cluster (standalone)
                      - Run:  pig -x local -l /path/to/pig.log -f /path/to/lsrd_collection_metrics.pig

                To gather metrics on another collection:

                  * Copy one of the ondemand sections data/filter pipelines and create a new STORE
                    for the output

  Author: Adam Dosch

  Date: 10-22-2012

  ##########################################################################################################
  # Change		Date		Author		Description
  ##########################################################################################################
  #  001		10-22-2012	Adam Dosch	Initial Release
  #  002		10-31-2012	Adam Dosch	Changing REGISTER path from /opt/pig to /home/espa/pig
  #  003		01-31-2013	Adam Dosch	Adding PigStorage comma-delimited section to STORE
  #							to store output in csv format
  #  004		02-04-2013	Adam Dosch      Changed LOAD log to dynamically be rotated apache
  #                                                     log for espa.cr.usgs.gov
  #
  ##########################################################################################################

*/

REGISTER /home/espa/pig/contrib/piggybank/java/piggybank.jar;

logs = LOAD '/opt/cots/apache/logs/espa.cr.usgs.gov-access_log.1' USING org.apache.pig.piggybank.storage.apachelog.WAFCombinedLogLoader AS (remoteAddr, remoteLogname, user, time, method, uri, proto, status, bytes);

/* LEDAPS ondemand pipeline filtering */
ondemandledapslogs = FILTER logs BY status == 200 AND uri MATCHES '^/orders/.*.tar.gz$' AND method == 'GET';

ondemandledapslogset = FOREACH ondemandledapslogs GENERATE uri, bytes;

grpondemandledapslogs = GROUP ondemandledapslogs all;

resultsondemandledaps = FOREACH grpondemandledapslogs {
	GENERATE COUNT(ondemandledapslogs) as tc, ((float)SUM(ondemandledapslogs.bytes)/1024/1024/1024) as tb;
};

STORE resultsondemandledaps INTO '/tmp/resultsondemandledaps' USING PigStorage(',');
