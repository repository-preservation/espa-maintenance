#!/bin/bash
set -e

# Helper script to automate the different reports (All, Landsat, MODIS, VIIRS)
logdir=~/monthly-logs/

mkdir -p $logdir
python lsrd_stats.py -e ops -c ~/.usgs/.cfgnfo_metrics -d $logdir
python lsrd_stats.py -e ops -c ~/.usgs/.cfgnfo_metrics -d $logdir --sensors MODIS
python lsrd_stats.py -e ops -c ~/.usgs/.cfgnfo_metrics -d $logdir --sensors VIIRS
python lsrd_stats.py -e ops -c ~/.usgs/.cfgnfo_metrics -d $logdir --plotting

# Need to save space month-to-month
rm -Rf $logdir/*
