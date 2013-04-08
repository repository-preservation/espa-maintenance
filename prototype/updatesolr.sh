#!/usr/bin/env bash

#For multiple files:
#Update the index in solr
echo "Indexing csvs in cwd"
for x in `ls|grep csv`;do curl 'http://localhost:8983/solr/update/csv?fieldnames=sceneid,acquisitionDate,sensor,path,row,upperLeftCornerLatLong,upperRightCornerLatLong,lowerLeftCornerLatLong,lowerRightCornerLatLong,sceneCenterLatLong,sunElevation,sunAzimuth,groundStation,collection,&separator=;&skipLines=1&overwrite=true' --data-binary @$x -H 'Content-type:text/csv; charset=utf-8';done

echo "Commit the changes to the solr index"
#Commit the index after updates have been done:
curl 'http://localhost:8983/solr/update/csv?commit=true'

echo "Done"

