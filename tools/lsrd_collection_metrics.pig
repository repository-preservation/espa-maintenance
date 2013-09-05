/*
  Name: lsrd_collection_metrics.pig

  Description:  Pig script to run on the LSRD/ESPA hadoop cluster to
                generate monthly metrics for GLS collection distribution

                So far, collecting:

                  * Unique downloader count (by distinct remote IP address)
                  * Number of downloaded scenes per collection and overall
                  * Volume of successfully distributed scenes per collection
                    and overall

                To execute:

                  * With Hadoop cluster:
                      - set environment variable HADOOP_INSTALL 
                      - Upload logfiles to HDFS with 'hadoop fs -copyFromLocal' command
                      - Run: pig -l /path/to/pig.log -f /path/to/lsrd_collection_metrics.pig

                  * Without Hadoop cluster (standalone)
                      - Run:  pig -x local -l /path/to/pig.log -f /path/to/lsrd_collection_metrics.pig

                To gather metrics on another collection:

                  * Copy one of the collection sections data/filter pipelines and create a new STORE
                    for the output

  Author: Adam Dosch

  Date: 10-21-2012

  ##########################################################################################################
  # Change		Date		Author		Description
  ##########################################################################################################
  #  001		10-21-2012	Adam Dosch	Initial Release
  #  002		02-04-2013	Adam Dosch	Changed LOAD log to dynamically be rotated apache
  #							log for espa.cr.usgs.gov
  #
  ##########################################################################################################

*/

REGISTER /home/espa/pig/contrib/piggybank/java/piggybank.jar;

logs = LOAD '/opt/cots/apache/logs/espa.cr.usgs.gov-access_log.1' USING org.apache.pig.piggybank.storage.apachelog.WAFCombinedLogLoader AS (remoteAddr, remoteLogname, user, time, method, uri, proto, status, bytes, referer, userAgent);

/* ESPA UI pipeline filtering */
espauilogs = FILTER logs BY status == 200 AND uri MATCHES '^/ui/?$' AND method == 'GET' AND NOT userAgent MATCHES '.*(Baiduspider|Twitterbot|YandexBot|msnbot|Googlebot|bingbot|Exabox|bitlybot|PaperLiBot|ShowyouBot|TweetmemeBot).*';

/* GLS data pipeline filtering */
allglslogs = FILTER logs BY status == 200 AND uri MATCHES '^/collections/gls-[0-9]{4}/.*.tar.gz$' AND method == 'GET' AND NOT userAgent MATCHES '.*(Baiduspider|Twitterbot|YandexBot|msnbot|Googlebot|bingbot|Exabox|bitlybot|PaperLiBot|ShowyouBot|TweetmemeBot).*';

gls2010logs = FILTER allglslogs BY uri MATCHES '^/collections/gls-2010/.*.tar.gz$';

gls2005logs = FILTER allglslogs BY uri MATCHES '^/collections/gls-2005/.*.tar.gz$';

gls2000logs = FILTER allglslogs BY uri MATCHES '^/collections/gls-2000/.*.tar.gz$';

espauilogset = FOREACH espauilogs GENERATE remoteAddr, referer, userAgent;

glslogs = FOREACH allglslogs GENERATE remoteAddr, uri, bytes;

gls2010logset = FOREACH gls2010logs GENERATE remoteAddr, uri, bytes;

gls2005logset = FOREACH gls2005logs GENERATE remoteAddr, uri, bytes;

gls2000logset = FOREACH gls2000logs GENERATE remoteAddr, uri, bytes;

grpespauilogs = GROUP espauilogs all;

grpespauirefererlogs = GROUP espauilogset by referer;

grpallglslogs = GROUP glslogs all;

grpgls2010logs = GROUP gls2010logs all;

grpgls2005logs = GROUP gls2005logs all;

grpgls2000logs = GROUP gls2000logs all;

resultsui = FOREACH grpespauilogs {
	uniquevisitors = DISTINCT espauilogs.remoteAddr;
        uniqueusgsvisitors = FILTER uniquevisitors by remoteAddr matches '(152.61.[0-9]{1,3}.[0-9]{1,3}|192.102.216.[0-9]{1,3}|136.177.[0-9]{1,3}.[0-9]{1,3}|144.47.[0-9]{1,3}.[0-9]{1,3}|192.41.213.[0-9]{1,3})';
	uniqueerosvisitors = FILTER uniqueusgsvisitors by remoteAddr matches '152.61.[0-9]{1,3}.[0-9]{1,3}';
	GENERATE COUNT(uniquevisitors) as ud, COUNT(uniqueusgsvisitors) as ud1, COUNT(uniqueerosvisitors) as ud2, COUNT(espauilogs.userAgent) as ud3;
};

resultsreferer = FOREACH grpespauirefererlogs {
	filteremptyreferer = FILTER espauilogset by not referer matches '-';
        uniquereferer = DISTINCT filteremptyreferer.referer;
 	GENERATE FLATTEN(uniquereferer), COUNT(filteremptyreferer) as referercount;
};

resultsrefererfinal = ORDER resultsreferer BY referercount DESC;

/*
resultsreferer = FOREACH grpespauirefererlogs GENERATE (chararray)espauilogset.referer, COUNT(grpespauirefererlogs);
*/

resultsall = FOREACH grpallglslogs {
        unique = DISTINCT glslogs.remoteAddr;
	GENERATE COUNT(unique) as ud, COUNT(glslogs) as tilecount, ((float)SUM(glslogs.bytes)/1024/1024/1024) as totalbytes;
};

results2010 = FOREACH grpgls2010logs {
        unique = DISTINCT gls2010logs.remoteAddr;
	GENERATE COUNT(unique) as ud, COUNT(gls2010logs) as tilecount10, ((float)SUM(gls2010logs.bytes)/1024/1024/1024) as totalbytes10;
};

results2005 = FOREACH grpgls2005logs {
        unique = DISTINCT gls2005logs.remoteAddr;
	GENERATE COUNT(unique) as ud, COUNT(gls2005logs) as tilecount05, ((float)SUM(gls2005logs.bytes)/1024/1024/1024) as totalbytes05;
};

results2000 = FOREACH grpgls2000logs {
        unique = DISTINCT gls2000logs.remoteAddr;
        GENERATE COUNT(unique) as ud, COUNT(gls2000logs) as tilecount00, ((float)SUM(gls2000logs.bytes)/1024/1024/1024) as totalbytes00;
};


STORE resultsall INTO '/tmp/resultsall' USING PigStorage(',');
STORE results2010 INTO '/tmp/results2010' USING PigStorage(',');
STORE results2005 INTO '/tmp/results2005' USING PigStorage(',');
STORE results2000 INTO '/tmp/results2000' USING PigStorage(',');
STORE resultsui INTO '/tmp/resultsui' USING PigStorage(',');
STORE resultsrefererfinal INTO '/tmp/resultsreferer' USING PigStorage(',');
