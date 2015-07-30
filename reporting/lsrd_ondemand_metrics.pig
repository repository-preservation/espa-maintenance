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
  #  004		02-04-2013	Adam Dosch  Changed LOAD log to dynamically be rotated apache
  #                                     log for espa.cr.usgs.gov
  #  005		01-30-2014	Adam Dosch	Renaming everythign and removing 'ledaps' off the
  #							pipeline names and results directory.
  #							Re-did apache log format to add "+" or "X" using the
  #							%X CustomLog variable to track downloads since we are
  #							not using mod_proxy to FTP and HTTP instead.  So we
  #							have no custom HTTP return code to invalidate an
  #							incomplete download.  Using %X, I have created an
  #							extra field on the end of the Apache Log that I must
  #							parse.  Had to Update WAFCombinedLogLoader Class to
  #							have that regex compiler match it as well.
  #							Added new fields 'referrer', 'userAgent' and the
  #							'downloadIndicator' to parse on log line
  #  006         05-01-2014 Adam Dosch  Updating logs 'LOAD' path to point to central pull-down area
  #  007         05-07-2014 Adam Dosch  Removing download indicator references --- Legacy apache, using nginx
  #                                     now.
  #
  ##########################################################################################################

*/

REGISTER /home/espa/pig/contrib/piggybank/java/piggybank.jar;

logs = LOAD '/data/logs/espa.cr.usgs.gov-access_log.1' USING org.apache.pig.piggybank.storage.apachelog.WAFCombinedLogLoader AS (remoteAddr, remoteLogname, user, time, method, uri, proto, status, bytes, referrer, userAgent);

/* LEDAPS ondemand pipeline filtering */
ondemandlogs = FILTER logs BY status == 200 AND uri MATCHES '^/orders/.*.tar.gz$' AND method == 'GET';

ondemandlogset = FOREACH ondemandlogs GENERATE uri, bytes;

grpondemandlogs = GROUP ondemandlogs all;

resultsondemand = FOREACH grpondemandlogs {
	GENERATE COUNT(ondemandlogs) as tc, ((float)SUM(ondemandlogs.bytes)/1024/1024/1024) as tb;
};

STORE resultsondemand INTO '/tmp/resultsondemand' USING PigStorage(',');
