#!/usr/bin/env python

"""
Author: David V. Hill

Purpose:  This is the primary logic module
          for the Land Product Validation System

License:  NASA Open Source Agreement 1.3
"""
import datetime

def log(module, msg):
    now = datetime.datetime.now()
    print("(%s) %s-%s-%s %s:%s.%s %s" % (module,
                                         now.year,
                                  now.month,
                                  now.day,
                                  now.hour,
                                  now.minute,
                                  now.second,
                                  msg))

if __name__ == '__main__':
    log("LPVS", "Checking input parameters.")
    log("LPVS", "Building working directories.")
    log("LPVS", "Searching for requested MODIS Tile.")
    log("LPVS", "Located MODIS Tile, transferring to work directory.")
    log("LPVS", "Searching for requested Landsat scene.")
    log("LPVS", "Located Landsat scene, transferring to work directory.")
    log("LPVS", "Applying reprojection to MODIS and Landsat.")
    log("LPVS", "Subsetting to requested spatial area.")
    log("LPVS", "Running statistical analysis against MODIS and Landsat.")
    log("LPVS", "Packaging results.")
    log("LPVS", "Distributing results.")
    log("LPVS", "Cleaning temporary directories.")
    log("LPVS", "Processing complete.")
