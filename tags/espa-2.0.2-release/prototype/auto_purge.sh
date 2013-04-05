#!/usr/bin/env bash
#
# auto_purge.sh
#
# Author: David Hill/Adam Dosch
#
# Date: 09-20-12
#
# Description: Script to automatically clean up ESPA orders older than 14 days from the database and from online cache disk
#
# Dependencies:   Mysql running on localhost with espa schema
#                 .my.cnf in defined USER's ~/ that contains the [client] section for auto-login to database
#                 passwordless ssh access to the landsat web server
#                 a notification_list file in the cwd with a list of names to send email reports to
#

ORDERPATH="/data2/orders"

DISTRIBUTIONHOST="edclxs70.cr.usgs.gov"

USER="espa"

DF_CMD="df -mhP"

datestr=`date +%m-%d-%y`
dumpfile="$datestr-orders.txt"
###reportfile="$datestr-report.txt"
reportfile="report.txt"
disk_usage_before=`ssh -q ${USER}@${DISTRIBUTIONHOST} ${DF_CMD} $ORDERPATH`

#echo $datestr
#echo $dumpfile
echo "Creating oldorders.txt dump file for all completed orders older than 14 days"
mysql -e 'use espa;select orderid from ordering_order where DATEDIFF(CURDATE(),completion_date) > 14' > $dumpfile

echo "Purging the database"
mysql -e 'use espa;delete from ordering_scene where order_id in (select id from ordering_order where DATEDIFF(CURDATE(),completion_date) > 14);delete from ordering_order where DATEDIFF(CURDATE(),completion_date) > 14'


for x in `cat $dumpfile`:
do
   echo "Removing $x from disk";
   ssh -q ${USER}@${DISTRIBUTIONHOST} rm -rf $ORDERPATH/$x
done

echo "Purge complete"

disk_usage_after=`ssh -q ${USER}@${DISTRIBUTIONHOST} ${DF_CMD} $ORDERPATH`

###touch $reportfile

if [ -f $reportfile ]; then
   \rm -rf $reportfile && touch $reportfile
fi

cat "===================================" >> $reportfile
cat "Disk usage before purge" >> $reportfile
cat $disk_usage_before >> $reportfile
cat " " >> $reportfile
cat "===================================" >> $reportfile
cat "Disk usage after purge" >> $reportfile
cat $disk_usage_after >> $reportfile
cat " " >> $reportfile
cat "===================================" >> $reportfile
cat "Purged orders" >> $reportfile
cat $dumpfile >> $reportfile
cat " " >> $reportfile
cat "=== End of report ===" >> $reportfile

echo "Sending notifications"
mail -s "Purged orders for $datestr" `cat notification_list` < $reportfile
 
