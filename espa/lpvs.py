#!/usr/bin/env python

"""
Author: David V. Hill

Purpose:  This is the primary logic module
          for the Land Product Validation System

License:  NASA Open Source Agreement 1.3
"""
import datetime
import util

if __name__ == '__main__':
    util.log("LPVS", "Checking input parameters.")
    util.log("LPVS", "Building working directories.")
    util.log("LPVS", "Searching for requested MODIS Tile.")
    util.log("LPVS", "Located MODIS Tile, transferring to work directory.")
    util.log("LPVS", "Searching for requested Landsat scene.")
    util.log("LPVS", "Located Landsat scene, transferring to work directory.")
    util.log("LPVS", "Applying reprojection to MODIS and Landsat.")
    util.log("LPVS", "Subsetting to requested spatial area.")
    util.log("LPVS", "Running statistical analysis against MODIS and Landsat.")
    util.log("LPVS", "Packaging results.")
    util.log("LPVS", "Distributing results.")
    util.log("LPVS", "Cleaning temporary directories.")
    util.log("LPVS", "Processing complete.")
