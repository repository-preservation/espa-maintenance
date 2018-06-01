#!/bin/bash
set -e

# Helper script to automate the different reports (All, Landsat, or MODIS)
logdir=~/monthly-logs/

mkdir -p $logdir
python lsrd_stats.py -e ops -c ~/.usgs/.cfgnfo_metrics -d $logdir --plotting
python lsrd_stats.py -e ops -c ~/.usgs/.cfgnfo_metrics -d $logdir --sensors MODIS

# Need to save space month-to-month
rm -Rf $logdir/*
