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

ORDERPATH="/data2/science_lsrd/LSRD/orders"

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


###reportfile="$datestr-report.txt"
reportfile="auto_purge_logs/$datestr-report.txt"


if [ -z "$SKIPDBPURGE" ]; then
   echo "Creating oldorders.txt dump file for all completed orders older than 10 days"
   mysql -e 'use espa;select orderid from ordering_order where status = "complete" and DATEDIFF(CURDATE(),completion_date) > 10' > $dumpfile

   echo "Purging the database"
   mysql -e 'use espa;delete from ordering_scene where order_id in (select id from ordering_order where status = "complete" and DATEDIFF(CURDATE(),completion_date) > 10);delete from ordering_order where status = "complete" and DATEDIFF(CURDATE(),completion_date) > 10'
else
   echo "Skipping purge since we passed in custom dumpfile"
fi

# gather metrics to report
orders_placed_today=`mysql -N -e 'use espa;select count(*) from ordering_order where order_date >= now() - INTERVAL 1 DAY';`
orders_complete_today=`mysql -N -e 'use espa;select count(*) from ordering_order where completion_date >= now() - INTERVAL 1 DAY';`

orders_placed_week=`mysql -N -e 'use espa;select count(*) from ordering_order where order_date >= now() - INTERVAL 7 DAY';`
orders_complete_week=`mysql -N -e 'use espa;select count(*) from ordering_order where completion_date >= now() - INTERVAL 7 DAY';`

scenes_ordered_today=`mysql -N -e 'use espa;select count(s.name) from ordering_order o, ordering_scene s  where o.order_date >= now() - INTERVAL 1 DAY and o.id = s.order_id';`
scenes_complete_today=`mysql -N -e 'use espa;select count(s.name) from ordering_scene s  where s.completion_date >= now() - INTERVAL 1 DAY';`
 
scenes_ordered_week=`mysql -N -e 'use espa;select count(s.name) from ordering_order o, ordering_scene s  where o.order_date >= now() - INTERVAL 7 DAY and o.id = s.order_id';`
scenes_complete_week=`mysql -N -e 'use espa;select count(s.name) from ordering_scene s  where s.completion_date >= now() - INTERVAL 7 DAY';`

open_orders=`mysql -N -e 'use espa;select count(*) from ordering_order where status = "ordered"';`
open_scenes=`mysql -N -e 'use espa;select count(s.name) from ordering_scene s where s.status in ("onorder", "oncache", "queued", "processing")';`

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
echo "Past 24 Hours" >> $reportfile
echo "Orders Placed:$orders_placed_today" >> $reportfile
echo "Orders Completed:$orders_complete_today" >> $reportfile
echo "Scenes Placed:$scenes_ordered_today" >> $reportfile
echo "Scenes Completed:$scenes_complete_today" >> $reportfile
echo " " >> $reportfile
echo "Past 7 Days" >> $reportfile
echo "Orders Placed:$orders_placed_week" >> $reportfile
echo "Orders Completed:$orders_complete_week" >> $reportfile
echo "Scenes Placed:$scenes_ordered_week" >> $reportfile
echo "Scenes Completed:$scenes_complete_week" >> $reportfile
echo " " >> $reportfile
echo "===================================" >> $reportfile
echo "Open orders:$open_orders" >> $reportfile
echo "Open scenes:$open_scenes" >> $reportfile
echo " " >> $reportfile
echo "===================================" >> $reportfile
echo "Purged orders" >> $reportfile
cat $dumpfile >> $reportfile
echo " " >> $reportfile
echo "=== End of report ===" >> $reportfile

echo "Sending notifications"
mail -s "Purged orders for $datestr" -r "espa@usgs.gov (ESPA AutoPurge Cron @ $HOSTNAME)" `cat notification_list` < $reportfile
 
