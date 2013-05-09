#!/usr/bin/env bash

hadoop jar /home/espadev/bin/hadoop/contrib/streaming/hadoop-streaming-0.20.203.0.jar \
-D mapred.task.timeout=172800000 -D mapred.reduce.tasks=0 -D mapred.job.queue.name='glsbrowse' \
-D mapred.job.name='browse_gen_2000' -file /home/espadev/espa-site/espa/mapper.py -file /home/espadev/espa-site/espa/espa.py \
-file /home/espadev/espa-site/espa/frange.py -mapper /home/espadev/espa-site/espa/mapper.py -cmdenv ESPA_WORK_DIR=$ESPA_WORK_DIR \
-cmdenv ANC_PATH=$ANC_PATH -cmdenv ESUN=$ESUN -input browse_requests/2000.txt  -output browse_requests/2000-out

