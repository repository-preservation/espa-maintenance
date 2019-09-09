#!/bin/bash
# Helper script to automate the different reports (All, Landsat, MODIS, VIIRS)

set -e  # bash option -e: Abort script at first error,
        # when a command exits with non-zero status
        # (except in until or while loops, if-tests, list constructs)

logdir=~/monthly-logs/

mkdir -p $logdir  # -p: no error if existing, make parent directories as needed
python lsrd_stats.py -e ops -c ~/.usgs/.cfgnfo_metrics -d $logdir
python lsrd_stats.py -e ops -c ~/.usgs/.cfgnfo_metrics -d $logdir --sensors MODIS
python lsrd_stats.py -e ops -c ~/.usgs/.cfgnfo_metrics -d $logdir --sensors VIIRS
python lsrd_stats.py -e ops -c ~/.usgs/.cfgnfo_metrics -d $logdir --plotting

# Need to save space month-to-month
rm -Rf $logdir/*
