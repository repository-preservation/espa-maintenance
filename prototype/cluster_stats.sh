#!/usr/bin/env bash

#Run this from l8srlscp05 to print the cluster report

for x in `cat bin/hadoop/conf/slaves`;do echo $x;echo "Memory:"; ssh $x free |grep Mem:|awk '{print $2}';echo "CPU Count:";ssh $x cat /proc/cpuinfo |grep -c processor; echo "Disk Space:"; ssh $x df --direct -mh /data |grep -i /data | awk '{print $2}';  echo "";done


