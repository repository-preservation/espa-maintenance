#!/usr/bin/env bash

#remove previous data if there's anything there
hadoop dfs -rmr md-in
hadoop dfs -rmr md-out

#try to make the directory in case it's not there
hadoop dfs -mkdir md-in
hadoop dfs -mkdir md-out

workingdir=/data/espa/workingdir
mkdir $workingdir
cd $workingdir

gzfiles=(LANDSAT_ETM.xml.gz LANDSAT_ETM_SLC_OFF.xml.gz LANDSAT_TM.xml.gz)
files=(LANDSAT_ETM.xml LANDSAT_ETM_SLC_OFF.xml LANDSAT_TM.xml)

for x in ${gzfiles[*]};
do
#pull down all the metadata files and uncompress them
wget http://landsat.usgs.gov/metadata_service/bulk_metadata_files/$x
pigz -d $x
done


#wget http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_ETM_SLC_OFF.xml.gz
#gunzip LANDSAT_ETM_SLC_OFF.xml.gz

#wget http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_TM.xml.gz
#gunzip LANDSAT_TM.xml.gz

#move our files into hdfs, store them with a block size of 16MB
for x in ${files[*]};
do
echo "Storing $x in HDFS"
hadoop dfs -Ddfs.replication=1 -Ddfs.block.size=$[16 * 1024 * 1024] -copyFromLocal $x md-in/$x
done

#-D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \

echo "Running MapReduce"
hadoop jar ~/bin/hadoop/contrib/streaming/hadoop-streaming-0.20.203.0.jar \
-D mapred.task.timeout=172800000 -D mapred.job.name=metadata_generation \
-D mapred.reduce.tasks=50 \
-D mapred.compress.map.output=true \
-D mapred.reduce.tasks.speculative.execution=false \
-D mapred.reduce.parallel.copies=15 \
-D mapred.output.compress=true \
-D mapred.compress.map.output=true \
-D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
-D mapred.map.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
-D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
-file ~/checkout/espa/trunk/espa/mapreduce/metadata/xml_mapper.py \
-file ~/checkout/espa/trunk/espa/mapreduce/metadata/xml_reducer.py \
-mapper ~/checkout/espa/trunk/espa/mapreduce/metadata/xml_mapper.py \
-reducer ~/checkout/espa/trunk/espa/mapreduce/metadata/xml_reducer.py \
-combiner ~/checkout/espa/trunk/espa/mapreduce/metadata/xml_reducer.py \
-inputreader 'StreamXmlRecordReader,begin=<metaData>,end=</metaData>' \
-input md-in/ -output md-out/metadata-out

#clean up the tmp space
cd ../
rm -rf $workingdir


#get the output of the jobs
echo "Copying output to /home/espa/solr_index"
#cd /home/espa
cd $HOME
rm -rf solr_index
mkdir solr_index
cd solr_index
hadoop dfs -copyToLocal md-out/metadata-out/part* .
hadoop dfs -rmr -skipTrash md-out/*
echo "Done"

#rebuild solr index
