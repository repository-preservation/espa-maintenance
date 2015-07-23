#!/usr/bin/env bash
#
# auto_purge.sh
#
# Author: David Hill/Adam Dosch
#
# Date: 09-20-12
#
# Description: Script to automatically clean up ESPA orders older than 10 days from the database and from online cache disk
#
# Dependencies:   Mysql running on localhost with espa schema
#                 .my.cnf in defined USER's ~/ that contains the [client] section for auto-login to database
#                 passwordless ssh access to the landsat web server
#                 a notification_list file in the cwd with a list of names to send email reports to
#

ORDERPATH="/data2/LSRD/orders"

DISTRIBUTIONHOST="edclpdsftp.cr.usgs.gov"

USER="espa"

DF_CMD="df -mhP"

declare SKIPDBPURGE

datestr=`date +%m-%d-%y`

mkdir -p auto_purge_logs

if [ -z "$1" ]; then
   dumpfile="auto_purge_logs/$datestr-orders.txt"
else
   if [ -f $1 ]; then
      SKIPDBPURGE=1
      dumpfile=$1
   else
      echo
      echo "Enter valid file for dumpfile - $1 is not a file or does not exist"
      echo
      exit 1
   fi
fi

<<<<<<< HEAD
###reportfile="$datestr-report.txt"
reportfile="auto_purge_logs/report.txt"
=======
reportfile="$datestr-report.txt"
>>>>>>> 411ad05e33c3983ed1881cae8c5e3663ff1b7c69

if [ -z "$SKIPDBPURGE" ]; then
   echo "Creating oldorders.txt dump file for all completed orders older than 10 days"
   mysql -e 'use espa;select orderid from ordering_order where status = "complete" and DATEDIFF(CURDATE(),completion_date) > 10' > $dumpfile

   echo "Purging the database"
   mysql -e 'use espa;delete from ordering_scene where order_id in (select id from ordering_order where status = "complete" and DATEDIFF(CURDATE(),completion_date) > 10);delete from ordering_order where status = "complete" and DATEDIFF(CURDATE(),completion_date) > 10'
else
   echo "Skipping purge since we passed in custom dumpfile"
fi

disk_usage_before=`ssh -q ${USER}@${DISTRIBUTIONHOST} ${DF_CMD} $ORDERPATH`

for x in `cat $dumpfile`:
do
   echo "Removing $x from disk";
   ssh -q ${USER}@${DISTRIBUTIONHOST} rm -rf $ORDERPATH/$x
done

echo "Purge complete"

disk_usage_after=`ssh -q ${USER}@${DISTRIBUTIONHOST} ${DF_CMD} $ORDERPATH`

###touch $reportfile

if [ -f $reportfile ]; then
   rm -rf $reportfile && touch $reportfile
fi

echo "===================================" >> $reportfile
echo "Disk usage before purge" >> $reportfile
echo $disk_usage_before >> $reportfile
echo " " >> $reportfile
echo "===================================" >> $reportfile
echo "Disk usage after purge" >> $reportfile
echo $disk_usage_after >> $reportfile
echo " " >> $reportfile
echo "===================================" >> $reportfile
echo "Purged orders" >> $reportfile
echo $dumpfile >> $reportfile
echo " " >> $reportfile
echo "=== End of report ===" >> $reportfile

echo "Sending notifications"
mail -s "Purged orders for $datestr" `cat notification_list` < $reportfile
 
