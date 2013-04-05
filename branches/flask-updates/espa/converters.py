#!/usr/bin/env python
import csv
import sys
import math

class Converter_Tools:
    def __init__(self):
        # use pass statement since nothing needs to be done
        pass

    #########################################################################
    # Module: sqr
    #
    # Description: This function will compute the square of the value.
    #
    # Returns:
    #    floating point value representing the square
    #
    # Developer History:
    #     Gail Schmidt    Original Development          June 2012
    #
    # Notes:
    #########################################################################
    def sqr (self, inval):
        return math.pow (inval, 2)
    
    #########################################################################
    # Module: distance.py
    #
    # Description: This function will compute the distance between two points.
    #
    # Returns:
    #    floating point value representing the distance
    #
    # Developer History:
    #     Gail Schmidt    Original Development          June 2012
    #
    # Notes:
    #  1. Need to handle points which cross the international dateline.
    #########################################################################
    def distance (self, pt1_lat, pt1_lon, pt2_lat, pt2_lon):
        if ((pt1_lon > 170) and (pt2_lon < -170)):
            dist_lon = 180.0 - pt1_lon + abs (-180.0 - pt2_lon)
            tmpval = self.sqr(dist_lon) + self.sqr(pt2_lat - pt1_lat)
        elif ((pt1_lon < -170) and (pt2_lon > 170)):
            dist_lon = 180.0 - pt2_lon + abs (-180.0 - pt1_lon)
            tmpval = self.sqr(dist_lon) + self.sqr(pt2_lat - pt1_lat)
        else:
            tmpval = self.sqr(pt2_lon - pt1_lon) + self.sqr(pt2_lat - pt1_lat)
        return math.sqrt (tmpval) 

##### End Converter_Tools class


class LL2PR_Converter:
    # Data attributes
    WRS_FILE = "etc/WRSCornerPoints.csv"   # WRS-2 table
    MODIS_FILE = "etc/MODISTileCornerPoints.csv"    # MODIS 10 degree tile table
    LAT = 0                         # Latitude index for lat/long points
    LON = 1                         # Longitude index for lat/long points

    def __init__(self):
        # use pass statement since nothing needs to be done
        pass
 
    #########################################################################
    # Module: pathrow_to_tile.py
    #
    # Description: This script will take an input WRS-2 path/row and return
    #     the respective 10 degree MODIS tile(s) required to cover that
    #     path/row.
    #
    # Inputs:
    #     <wrs_path> is the WRS-2 path number
    #     <wrs_row> is the WRS-2 row number
    #
    # Returns:
    #     Array of integers.  The first value is the number of tiles.  The
    #     second value is the horizontal tile number, followed by the vertical
    #     tile number.  From there the remaining tile numbers will be staggered
    #     (horizontal, vertical) in the array.
    #
    # Developer History:
    #     Gail Schmidt    Original Development          May 2012
    #########################################################################
    def pathrow_to_tile(self, wrs_path, wrs_row):
        # Validate the path/row values.  Valid paths are from 1 to 233.  Valid
        # rows are from 1 to 248.
        if (wrs_path < 1) or (wrs_path > 233):
            print 'pathrow_to_tile: Path argument is invalid: ', wrs_path
            return 0
        if (wrs_row < 1) or (wrs_row > 248):
            print 'pathrow_to_tile: Row argument is invalid: ', wrs_row
            return 0
        
        # Open the WRS-2 table of lat/longs.  This table is of the form path,
        # row, ctr_lat, ctr_lon, ul_lat, ul_lon, ur_lat, ur_lon, ll_lat, ll_lon,
        # lr_lat, lr_lon
        wrsFile = open (self.WRS_FILE, 'rb')
        wrsReader = csv.reader (wrsFile, delimiter= ',')
        
        # Determine the bounding coordinates of the input path/row in lat/long.
        # Loop through the input WRS file looking for the user-specified path
        # and row.
        header_line = wrsReader.next()    # skip the header
        found = False
        for wrs_line in wrsReader:
#            print 'DEBUG path/row:', wrs_line[0], wrs_line[1]
            path = int(wrs_line[0])
            row = int(wrs_line[1])
            if (path == wrs_path) and (row == wrs_row):
                # Found the specified path/row in the table.  Now grab the
                # lat/long values for each corner point and store as lat/long
                # pairs.
                wrs_center = [float(wrs_line[2]), float(wrs_line[3])]
                wrs_ul = [float(wrs_line[4]), float(wrs_line[5])]
                wrs_ur = [float(wrs_line[6]), float(wrs_line[7])]
                wrs_ll = [float(wrs_line[8]), float(wrs_line[9])]
                wrs_lr = [float(wrs_line[10]), float(wrs_line[11])]
        
                # If this is an ascending row (123-245) then the corners need
                # to be flipped around. UL -> LR, UR -> LL, LL -> UR, LR -> UL.
                # Basically switch UL with LR and LL with UR.  Row 122 is the
                # southernmost row.  Row 246 is the northernmost row.
                if 123 <= row <= 245:
                    mytemp = wrs_lr[self.LAT]
                    wrs_lr[self.LAT] = wrs_ul[self.LAT]
                    wrs_ul[self.LAT] = mytemp
                    mytemp = wrs_lr[self.LON]
                    wrs_lr[self.LON] = wrs_ul[self.LON]
                    wrs_ul[self.LON] = mytemp
                    mytemp = wrs_ll[self.LAT]
                    wrs_ll[self.LAT] = wrs_ur[self.LAT]
                    wrs_ur[self.LAT] = mytemp
                    mytemp = wrs_ll[self.LON]
                    wrs_ll[self.LON] = wrs_ur[self.LON]
                    wrs_ur[self.LON] = mytemp
        
                # Determine if there is left or right sided wrap-around for
                # this scene
                left_wrap = False
                if ((wrs_center[self.LON] < -170.0) and \
                    ((wrs_ul[self.LON] > 170.0) or (wrs_ll[self.LON] > 170.0))):
                    left_wrap = True
#                    print 'DEBUG Left wrap'
            
                    # Which corner is farthest west
                    if ((wrs_ul[self.LON] > 170.0) and (wrs_ll[self.LON] > 170.0)):
                        # Both points wrap around the dateline
                        if wrs_ul[self.LON] < wrs_ll[self.LON]:
                            wrs_left_bound = wrs_ul[self.LON]
                        else:
                            wrs_left_bound = wrs_ll[self.LON]
                    elif wrs_ul[self.LON] > 170.0:
                        # UL corner wraps around
                        wrs_left_bound = wrs_ul[self.LON]
                    else:
                        # LL corner wraps around
                        wrs_left_bound = wrs_ll[self.LON]
            
                right_wrap = False
                if ((wrs_center[self.LON] > 170.0) and \
                    ((wrs_ur[self.LON] < -170.0) or (wrs_lr[self.LON] < -170.0))):
                    right_wrap = True
#                    print 'DEBUG Right wrap'
            
                    # Which corner is farthest east
                    if ((wrs_ur[self.LON] < -170.0) and (wrs_lr[self.LON] < -170.0)):
                        # Both points wrap around the dateline. Find the least
                        # negative as our eastern boundary.
                        if wrs_ur[self.LON] > wrs_lr[self.LON]:
                            wrs_right_bound = wrs_ur[self.LON]
                        else:
                            wrs_right_bound = wrs_lr[self.LON]
                    elif wrs_ur[self.LON] < -170.0:
                        # UR corner wraps around
                        wrs_right_bound = wrs_ur[self.LON]
                    else:
                        # LR corner wraps around
                        wrs_right_bound = wrs_lr[self.LON]
            
                # Determine the distance between the UR - UL longitudes.  Then
                # compute the 4% drift that is possible for Landsat from the
                # nominal scene centers in the provided table.  If there is
                # right or left scene wrap, then the scene distance needs to be
                # handled a bit differently.  Look at the distance between the
                # UL and the 180.0 line and then the UR and the -180.0 line.
                # Remember right or left wrap could be either the upper or the
                # lower corners.
                if (right_wrap == True and wrs_ur[self.LON] < -170.0) or \
                   (left_wrap == True and wrs_ul[self.LON] > 170.0):
                    lon_drift = (abs(-180.0 - wrs_ur[self.LON]) + \
                        (180.0 - wrs_ul[self.LON])) * 0.04;
                else:
                    lon_drift = abs (wrs_ur[self.LON] - wrs_ul[self.LON]) * 0.04;
#                print 'DEBUG Longitudinal drift:', lon_drift
        
                # Determine the bounding lat/long coordinates for this
                # path/row.  If there is a left or right wrap, that bounding
                # coord has already been determined.
                if left_wrap != True:
                    if wrs_ul[self.LON] < wrs_ll[self.LON]:
                        wrs_left_bound = wrs_ul[self.LON]
                    else:
                        wrs_left_bound = wrs_ll[self.LON]
            
                if right_wrap != True:
                    if wrs_ur[self.LON] > wrs_lr[self.LON]:
                        wrs_right_bound = wrs_ur[self.LON]
                    else:
                        wrs_right_bound = wrs_lr[self.LON]
            
                if wrs_ul[self.LAT] > wrs_ur[self.LAT]:
                    wrs_upper_bound = wrs_ul[self.LAT]
                else:
                    wrs_upper_bound = wrs_ur[self.LAT]
            
                if wrs_ll[self.LAT] < wrs_lr[self.LAT]:
                    wrs_lower_bound = wrs_ll[self.LAT]
                else:
                    wrs_lower_bound = wrs_lr[self.LAT]
            
                # Add the longitudinal drift to the bounding extents
                wrs_left_bound -= lon_drift
                wrs_right_bound += lon_drift
#                print 'DEBUG WRS Left bound:', wrs_left_bound
#                print 'DEBUG WRS Right bound:', wrs_right_bound
#                print 'DEBUG WRS Upper bound:', wrs_upper_bound
#                print 'DEBUG WRS Lower bound:', wrs_lower_bound
            
                # Handle the international dateline wrap-around
                if (wrs_left_bound < -180.0):
                    wrap = wrs_left_bound + 180.0
                    wrs_left_bound = 180.0 + wrap
                if (wrs_left_bound > 180.0):
                    wrap = wrs_left_bound - 180.0
                    wrs_left_bound = -180.0 + wrap
                if (wrs_right_bound < -180.0):
                    wrap = wrs_right_bound + 180.0
                    wrs_right_bound = 180.0 + wrap
                if (wrs_right_bound > 180.0):
                    wrap = wrs_right_bound - 180.0
                    wrs_right_bound = -180.0 + wrap
#                print 'DEBUG WRS Left bound (after wrap):', wrs_left_bound
#                print 'DEBUG WRS Right bound (after wrap):', wrs_right_bound
#                print 'DEBUG WRS Upper bound (after wrap):', wrs_upper_bound
#                print 'DEBUG WRS Lower bound (after wrap):', wrs_lower_bound
        
                # No need to keep searching if specified path/row was found
                found = True
                break;
        
        # If specified path/row was not found then exit with an error
        if found != True:
            print 'Specified path/row was not found.'
            return 0
        
        # Close the WRS input file
        wrsFile.close()
        
        # Open the MODIS 10 degree table of lat/longs.  This table is of the
        # form iv, ih, lon_min, lon_max, lat_min, lat_max.
        modisFile = open (self.MODIS_FILE, 'rb')
        modisReader = csv.reader (modisFile, delimiter= ',')
        
        # Determine which tile the WRS UL, UR, LL, and LR corners reside in
        horiz_tiles = []               # list of tiles
        vert_tiles = []                # list of tiles
        nmodis_tiles = 0
        header_line = modisReader.next()    # skip the header
        for modis_line in modisReader:
            # Get the tile numbers
            vtile = int(modis_line[0])
            htile = int(modis_line[1])
#            print 'DEBUG horiz/vert:', htile, vtile
        
            # Store the bounding corner points for this tile.
            modis_left_bound = float(modis_line[2])
            modis_right_bound = float(modis_line[3])
            modis_lower_bound = float(modis_line[4])
            modis_upper_bound = float(modis_line[5])
#            print 'DEBUG MODIS upper bound:', modis_upper_bound
#            print 'DEBUG MODIS lower bound:', modis_lower_bound
#            print 'DEBUG MODIS left bound:', modis_left_bound
#            print 'DEBUG MODIS right bound:', modis_right_bound
        
            # The MODIS tile coordinates have fill values for lat/longs which
            # don't exist.  These tiles need to be skipped.
            if (modis_upper_bound == -99.0) or (modis_lower_bound == -99.0) or \
               (modis_left_bound == -999.0) or (modis_right_bound == -999.0):
                continue
        
            # If any of the WRS corner lat/longs fall within the tile
            # boundaries, then include this tile in the list of MODIS tiles
            # needed to cover the specified WRS path/row.
            if ((modis_lower_bound <= wrs_upper_bound <= modis_upper_bound) and  \
                (modis_left_bound <= wrs_left_bound <= modis_right_bound)) or    \
               ((modis_lower_bound <= wrs_upper_bound <= modis_upper_bound) and  \
                (modis_left_bound <= wrs_right_bound <= modis_right_bound)) or   \
               ((modis_lower_bound <= wrs_lower_bound <= modis_upper_bound) and  \
                (modis_left_bound <= wrs_left_bound <= modis_right_bound)) or    \
               ((modis_lower_bound <= wrs_lower_bound <= modis_upper_bound) and  \
                (modis_left_bound <= wrs_right_bound <= modis_right_bound)):
                # Keep this tile
                horiz_tiles.append(htile)
                vert_tiles.append(vtile)
                nmodis_tiles += 1

        # Close the MODIS input file
        modisFile.close() 

        # Sort the scenes based on which scene center is closest to the
        # lat/long point
        results = [nmodis_tiles]
        for loop in range(nmodis_tiles):
            results.append (horiz_tiles[loop])
            results.append (vert_tiles[loop])

        return results


    #########################################################################
    # Module: tile_to_pathrow.py
    #
    # Description: This script will take an input 10 degree MODIS tile and
    #     return the respective descending WRS-2 path/rows which cover that
    #     tile.
    #
    # Inputs:
    #     <htile> is the MODIS horizontal tile number
    #     <vtile> is the MODIS vertical tile number
    #
    # Returns:
    #     Array of integers.  The first value is the number of path/rows.
    #     The second value is the first path, followed by the first row.  From
    #     there the remaining path/rows will be staggered in the array.
    #
    # Developer History:
    #     Gail Schmidt    Original Development          June 2012
    #
    # Notes:
    # 1. There will be a large number of path/rows which cover the bounding
    #    coordinates of the MODIS tile.  The algorithm will list the path/rows
    #    in order of which scene center is closest to the center of the MODIS
    #    tile.
    # 2. Some scenes cross the international dateline, so those coordinates
    #    need to be handled appropriately.
    # 3. This application currently skips over ascending rows (123 - 245). Rows
    #    1 through 121 descend to the southmost row, which is row 122.  Rows
    #    123 to 245 comprise the ascending portion of the orbit.  Row 246 is
    #    the northernmost row.  And rows 247 and 248 begin the descending
    #    portion of the next orbit (path) leading to row 1.  If the ascending
    #    rows are processed, then the corner points will need to be flipped.
    #    UL switched with LR and UR switched with LL.
    #    UL -> LR, UR -> LL, LL -> UR, LR -> UL
    ########################################################################
    def tile_to_pathrow(self, htile, vtile):
        # Validate the tile values.  Valid htiles are from 0 to 35.  Valid
        # vtiles are from 0 to 17.
        if (htile < 0) or (htile > 35):
            print 'tile_to_pathrow: Horizontal tile argument is invalid: ', \
                htile
            return 0
        if (vtile < 0) or (vtile > 17):
            print 'tile_to_pathrow: Vertical tile argument is invalid: ', vtile
            return 0
        
        # Open the MODIS 10 degree table of lat/longs.  This table is of the
        # form iv, ih, lon_min, lon_max, lat_min, lat_max.
        modisFile = open (self.MODIS_FILE, 'rb')
        modisReader = csv.reader (modisFile, delimiter= ',')
        
        # Instantiate the converter tools class for later use
        tools = Converter_Tools()

        # Determine the bounding coordinates of the input MODIS tile.  Loop
        # through the input MODIS file looking for the user-specified tile.
        header_line = modisReader.next()    # skip the header
        tile_center = [0]*2                 # tile center initialized to 0
        found = False
        for modis_line in modisReader:
            # Get the tile numbers
            curr_vtile = int(modis_line[0])
            curr_htile = int(modis_line[1])
#            print 'DEBUG horiz/vert:', curr_htile, curr_vtile
        
            if (curr_htile == htile) and (curr_vtile == vtile):
                # Found the specified MODIS tile in the table.  Now grab the
                # bounding coordinates.
                modis_left_bound = float(modis_line[2])
                modis_right_bound = float(modis_line[3])
                modis_lower_bound = float(modis_line[4])
                modis_upper_bound = float(modis_line[5])
#                print 'DEBUG horiz/vert:', curr_htile, curr_vtile
#                print 'DEBUG MODIS upper bound:', modis_upper_bound
#                print 'DEBUG MODIS lower bound:', modis_lower_bound
#                print 'DEBUG MODIS left bound:', modis_left_bound
#                print 'DEBUG MODIS right bound:', modis_right_bound
        
                # The MODIS tile coordinates have fill values for lat/longs
                # which don't exist.  If that's the case for the specified
                # tile, then return with a warning message as there isn't much
                # else to be done with this tile.
                if (modis_upper_bound == -99.0) or \
                   (modis_lower_bound == -99.0) or \
                   (modis_left_bound == -999.0) or \
                   (modis_right_bound == -999.0):
                    print 'tile_to_pathrow: Specified MODIS tile is fill. ' \
                        'Cannot determine path/row.'
                    return 0
        
                # Determine the center of the MODIS tile bounding coords
                tile_center[self.LAT] = modis_upper_bound - \
                    (modis_upper_bound - modis_lower_bound) * 0.5
                tile_center[self.LON] = modis_left_bound + \
                    (modis_right_bound - modis_left_bound) * 0.5
#                print 'DEBUG tile center lat/long:', tile_center[self.LAT], \
#                     tile_center[self.LON]
        
                # No need to keep searching if the specified tile has been found
                found = True
                break;
        
        # If specified tile was not found then exit with an error
        if found != True:
            print 'Specified MODIS tile was not found.'
            return 0
        
        # Close the MODIS input file
        modisFile.close()
        
        # Open the WRS-2 table of lat/longs.  This table is of the form path,
        # row, ctr_lat, ctr_lon, ul_lat, ul_lon, ur_lat, ur_lon, ll_lat,
        # ll_lon, lr_lat, lr_lon
        wrsFile = open (self.WRS_FILE, 'rb')
        wrsReader = csv.reader (wrsFile, delimiter= ',')
        
        # Determine which path/row the bounding coordinate corners reside in
        path_list = []                # list of paths
        row_list = []                 # list of rows
        dist_center = []              # distance of scene center to tile center
        npathrow = 0                      # initialize to 0
        header_line = wrsReader.next()    # skip the header
        for wrs_line in wrsReader:
#            print 'DEBUG path/row:', wrs_line[0], wrs_line[1]
#            print 'DEBUG npathrow:', npathrow
            # Read the lat/long values for each center and corner point and
            # store as lat/long pairs
            path = int(wrs_line[0])
            row = int(wrs_line[1])
            wrs_center = [float(wrs_line[2]), float(wrs_line[3])]
            wrs_ul = [float(wrs_line[4]), float(wrs_line[5])]
            wrs_ur = [float(wrs_line[6]), float(wrs_line[7])]
            wrs_ll = [float(wrs_line[8]), float(wrs_line[9])]
            wrs_lr = [float(wrs_line[10]), float(wrs_line[11])]
        
            # If this is an ascending row (123-245) which is nighttime data,
            # then skip to the next row
            if 123 <= row <= 245:
                continue
        
            # If this is an ascending row (123-245) then the corners need to be
            # flipped around. UL -> LR, UR -> LL, LL -> UR, LR -> UL. Basically
            # switch UL with LR and LL with UR. Row 122 is the southernmost row.
            # Row 246 is the northernmost row.
#            if 123 <= row <= 245:
#                mytemp = wrs_lr[self.LAT]
#                wrs_lr[self.LAT] = wrs_ul[self.LAT]
#                wrs_ul[self.LAT] = mytemp
#                mytemp = wrs_lr[self.LON]
#                wrs_lr[self.LON] = wrs_ul[self.LON]
#                wrs_ul[self.LON] = mytemp
#                mytemp = wrs_ll[self.LAT]
#                wrs_ll[self.LAT] = wrs_ur[self.LAT]
#                wrs_ur[self.LAT] = mytemp
#                mytemp = wrs_ll[self.LON]
#                wrs_ll[self.LON] = wrs_ur[self.LON]
#                wrs_ur[self.LON] = mytemp
        
            # Determine if there is left or right sided wrap-around for this
            # scene
            left_wrap = False
            if ((wrs_center[self.LON] < -170.0) and \
                ((wrs_ul[self.LON] > 170.0) or (wrs_ll[self.LON] > 170.0))):
                left_wrap = True
#                print 'DEBUG Left wrap'
        
                # Which corner is farthest west
                if ((wrs_ul[self.LON] > 170.0) and (wrs_ll[self.LON] > 170.0)):
                    # Both points wrap around the dateline
                    if wrs_ul[self.LON] < wrs_ll[self.LON]:
                        wrs_left_bound = wrs_ul[self.LON]
                    else:
                        wrs_left_bound = wrs_ll[self.LON]
                elif wrs_ul[self.LON] > 170.0:
                    # UL corner wraps around
                    wrs_left_bound = wrs_ul[self.LON]
                else:
                    # LL corner wraps around
                    wrs_left_bound = wrs_ll[self.LON]
        
            right_wrap = False
            if ((wrs_center[self.LON] > 170.0) and \
                ((wrs_ur[self.LON] < -170.0) or \
                (wrs_lr[self.LON] < -170.0))):
                right_wrap = True
#                print 'DEBUG Right wrap'
        
                # Which corner is farthest east
                if ((wrs_ur[self.LON] < -170.0) and \
                   (wrs_lr[self.LON] < -170.0)):
                    # Both points wrap around the dateline. Find the least
                    # negative as our eastern boundary.
                    if wrs_ur[self.LON] > wrs_lr[self.LON]:
                        wrs_right_bound = wrs_ur[self.LON]
                    else:
                        wrs_right_bound = wrs_lr[self.LON]
                elif wrs_ur[self.LON] < -170.0:
                    # UR corner wraps around
                    wrs_right_bound = wrs_ur[self.LON]
                else:
                    # LR corner wraps around
                    wrs_right_bound = wrs_lr[self.LON]
            
            # Determine the distance between the UR - UL longitudes.  Then
            # compute the 4% drift that is possible for Landsat from the
            # nominal scene centers in the provided table.  If there is right
            # or left scene wrap, then the scene distance needs to be handled
            # a bit differently.  Look at the distance between the UL and the
            # 180.0 line and then the UR and the -180.0 line.  Remember right
            # or left wrap could be either the upper or the lower corners.
            if (right_wrap == True and wrs_ur[self.LON] < -170.0) or \
               (left_wrap == True and wrs_ul[self.LON] > 170.0):
                lon_drift = (abs(-180.0 - wrs_ur[self.LON]) +  \
                    (180.0 - wrs_ul[self.LON])) * 0.04;
            else:
                lon_drift = abs (wrs_ur[self.LON] - wrs_ul[self.LON]) * 0.04;
#            print 'DEBUG Longitudinal drift:', lon_drift
        
            # Determine the bounding lat/long coordinates for this path/row. If
            # there is a left or right wrap, that bounding coord has already
            # been determined.
            if left_wrap != True:
                if wrs_ul[self.LON] < wrs_ll[self.LON]:
                    wrs_left_bound = wrs_ul[self.LON]
                else:
                    wrs_left_bound = wrs_ll[self.LON]
        
            if right_wrap != True:
                if wrs_ur[self.LON] > wrs_lr[self.LON]:
                    wrs_right_bound = wrs_ur[self.LON]
                else:
                    wrs_right_bound = wrs_lr[self.LON]
        
            if wrs_ul[self.LAT] > wrs_ur[self.LAT]:
                wrs_upper_bound = wrs_ul[self.LAT]
            else:
                wrs_upper_bound = wrs_ur[self.LAT]
        
            if wrs_ll[self.LAT] < wrs_lr[self.LAT]:
                wrs_lower_bound = wrs_ll[self.LAT]
            else:
                wrs_lower_bound = wrs_lr[self.LAT]
            
#            print 'DEBUG WRS Left bound (before lon drift):', wrs_left_bound
#            print 'DEBUG WRS Right bound (before lon drift):', wrs_right_bound
            
            # Add the longitudinal drift to the bounding extents
            wrs_left_bound -= lon_drift
            wrs_right_bound += lon_drift
#            print 'DEBUG WRS Left bound:', wrs_left_bound
#            print 'DEBUG WRS Right bound:', wrs_right_bound
#            print 'DEBUG WRS Upper bound:', wrs_upper_bound
#            print 'DEBUG WRS Lower bound:', wrs_lower_bound
            
            # Handle the international dateline wrap-around
            if (wrs_left_bound < -180.0):
                wrap = wrs_left_bound + 180.0
                wrs_left_bound = 180.0 + wrap
            if (wrs_left_bound > 180.0):
                wrap = wrs_left_bound - 180.0
                wrs_left_bound = -180.0 + wrap
            if (wrs_right_bound < -180.0):
                wrap = wrs_right_bound + 180.0
                wrs_right_bound = 180.0 + wrap
            if (wrs_right_bound > 180.0):
                wrap = wrs_right_bound - 180.0
                wrs_right_bound = -180.0 + wrap
#            print 'DEBUG WRS Left bound (after wrap):', wrs_left_bound
#            print 'DEBUG WRS Right bound (after wrap):', wrs_right_bound
#            print 'DEBUG WRS Upper bound (after wrap):', wrs_upper_bound
#            print 'DEBUG WRS Lower bound (after wrap):', wrs_lower_bound
        
            # If any of the WRS corner lat/longs fall within the tile boundary,
            # then include this path/row in the list needed to cover the
            # specified tile.
            if ((modis_lower_bound <= wrs_upper_bound <= modis_upper_bound) and  \
                (modis_left_bound <= wrs_left_bound <= modis_right_bound)) or    \
               ((modis_lower_bound <= wrs_upper_bound <= modis_upper_bound) and  \
                (modis_left_bound <= wrs_right_bound <= modis_right_bound)) or   \
               ((modis_lower_bound <= wrs_lower_bound <= modis_upper_bound) and  \
                (modis_left_bound <= wrs_left_bound <= modis_right_bound)) or    \
               ((modis_lower_bound <= wrs_lower_bound <= modis_upper_bound) and  \
                (modis_left_bound <= wrs_right_bound <= modis_right_bound)):
                # Keep this path/row
#                print 'DEBUG keep path/row:', path, row
                path_list.append (path)
                row_list.append (row)
        
                # Calculate the distance of this nominal scene center to the
                # center of the bounding coords
                dist = tools.distance (wrs_center[self.LAT],   \
                    wrs_center[self.LON], tile_center[self.LAT],  \
                    tile_center[self.LON])
                dist_center.append (dist)
#                print 'DEBUG distance from center:', dist_center[npathrow]
        
                # Increment the path/row counter
                npathrow += 1
        
        # Close the WRS input file
        wrsFile.close()
        
        # Sort the scenes based on which scene center is closest to the center
        # of the MODIS tile
        for loop in range(npathrow):
            for i in range(loop, npathrow):
                if (dist_center[i] < dist_center[loop]):
                    mytemp = dist_center[loop]
                    dist_center[loop] = dist_center[i]
                    dist_center[i] = mytemp
                    mytemp = path_list[loop]
                    path_list[loop] = path_list[i]
                    path_list[i] = mytemp
                    mytemp = row_list[loop]
                    row_list[loop] = row_list[i]
                    row_list[i] = mytemp
     
        # Sort the scenes based on which scene center is closest to the
        # lat/long point
        results = [npathrow]
        for loop in range(npathrow):
            results.append (path_list[loop])
            results.append (row_list[loop])

        return results


    #########################################################################
    # Module: latlong_to_tile.py
    #
    # Description: This function will take an input latitude/longitude in
    #     decimal degrees and return the respective 10 degree MODIS tile(s)
    #     which cover that lat/long.
    #
    # Inputs:
    #     <lat> is the latitude in decimal degrees (float)
    #     <lon> is the longitude in decimal degrees (float)
    #
    # Returns:
    #     Array of integers.  The first value is the number of tiles.  The
    #     second value is the horizontal tile number, followed by the vertical
    #     tile number.  From there the remaining tile numbers will be staggered
    #     (horizontal, vertical) in the array.
    #
    # Developer History:
    #     Gail Schmidt    Original Development          May 2012
    #
    # Notes:
    # 1. When just using the MODIS bounding coordinates and comparing the
    #    specified lat/long to those coordinates, the algorithem usually ends
    #    up with more than one tile in which the point resides.  That's really
    #    not possible, but is part of the fact that the bounding coordinates
    #    are being used.  So the algorithm will list the tiles in order of
    #    which tile center is closest to the point.
    #########################################################################
    def latlong_to_tile(self, lat, lon):
        # Validate the lat/long values.
        if (lat < -90.0) or (lat > 90.0):
            print 'latlong_to_tile: Latitude argument is invalid: ', lat
            return 0
        if (lon < -180.0) or (lon > 180.0):
            print 'latlong_to_tile: Latitude argument is invalid: ', lat
            return 0

        # Open the MODIS 10 degree table of lat/longs.  This table is of the
        # form iv, ih, lon_min, lon_max, lat_min, lat_max.
        modisFile = open (self.MODIS_FILE, 'rb')
        modisReader = csv.reader (modisFile, delimiter= ',')
        
        # Instantiate the converter tools class for later use
        tools = Converter_Tools()

        # Determine which tile the lat/long point resides in
        horiz_tiles = []               # list of horizontal tiles
        vert_tiles = []                # list of vertical tiles
        dist_center = []               # distance from scene center
        nmodis_tiles = 0
        header_line = modisReader.next()   # skip the header
        for modis_line in modisReader:
            # Get the tile numbers
            vtile = int(modis_line[0])
            htile = int(modis_line[1])
#            print 'DEBUG horiz/vert:', htile, vtile
        
            # Store the bounding corner points for this tile.
            modis_left_bound = float(modis_line[2])
            modis_right_bound = float(modis_line[3])
            modis_lower_bound = float(modis_line[4])
            modis_upper_bound = float(modis_line[5])
#            print 'DEBUG MODIS upper bound:', modis_upper_bound
#            print 'DEBUG MODIS lower bound:', modis_lower_bound
#            print 'DEBUG MODIS left bound:', modis_left_bound
#            print 'DEBUG MODIS right bound:', modis_right_bound
        
            # The MODIS tile coordinates have fill values for lat/longs which
            # don't exist.  These tiles need to be skipped.
            if (modis_upper_bound == -99.0) or (modis_lower_bound == -99.0) or \
               (modis_left_bound == -999.0) or (modis_right_bound == -999.0):
                continue
        
            # If lat/long falls within the tile boundaries, then include this
            # tile in the list of MODIS tiles.
            if ((modis_lower_bound <= lat <= modis_upper_bound) and  \
                (modis_left_bound <= lon <= modis_right_bound)):
                # Keep this tile
                horiz_tiles.append (htile)
                vert_tiles.append (vtile)
        
                # Find the tile center
                center_lat = modis_lower_bound + \
                    ((modis_upper_bound - modis_lower_bound) * 0.5)
                center_lon = modis_left_bound + \
                    ((modis_right_bound - modis_left_bound) * 0.5)
#                print 'DEBUG center lat:', center_lat
#                print 'DEBUG center lon:', center_lon
        
                # Calculate the distance of this point to the center of tile
                dist = tools.distance (center_lat, center_lon, lat, lon)
                dist_center.append (dist)
#                print 'DEBUG distance from center:', dist_center[nmodis_tiles]
        
                # Increment the tile counter
                nmodis_tiles += 1
        
        # Close the MODIS input file
        modisFile.close()
         
        # Sort the tiles based on which tile center is closest to the point
        for loop in range(nmodis_tiles):
            for i in range(loop, nmodis_tiles):
                if (dist_center[i] < dist_center[loop]):
                    mytemp = dist_center[loop]
                    dist_center[loop] = dist_center[i]
                    dist_center[i] = mytemp
                    mytemp = horiz_tiles[loop]
                    horiz_tiles[loop] = horiz_tiles[i]
                    horiz_tiles[i] = mytemp
                    mytemp = vert_tiles[loop]
                    vert_tiles[loop] = vert_tiles[i]
                    vert_tiles[i] = mytemp
        
        # Sort the scenes based on which scene center is closest to the
        # lat/long point
        results = [nmodis_tiles]
        for loop in range(nmodis_tiles):
            results.append (horiz_tiles[loop])
            results.append (vert_tiles[loop])

        return results


    #########################################################################
    # Module: latlong_to_pathrow
    #
    # Description: This funcion will take an input latitude/longitude in
    #     decimal degrees and return the respective descending WRS-2
    #     path/row(s) which cover that lat/long.
    #
    # Inputs:
    #     <lat> is the latitude in decimal degrees (float)
    #     <lon> is the longitude in decimal degrees (float)
    #
    # Returns:
    #     Array of integers.  The first value is the number of path/rows.
    #     The second value is the first path, followed by the first row.  From
    #     there the remaining path/rows will be staggered in the array.
    #
    # Developer History:
    #     Gail Schmidt    Original Development          June 2012
    #
    # Notes:
    # 1. When just using the WRS-2 bounding coordinates and comparing the
    #    specified lat/long to those coordinates, the point can reside within
    #    multiple path/rows.  The algorithm will list the path/rows in order of
    #    which scene center is closest to the point.
    # 2. Some scenes cross the international dateline, so those coordinates
    #    need to be handled appropriately.
    # 3. This application currently skips over ascending rows (123 - 245). Rows
    #    1 through 121 descend to the southmost row, which is row 122.  Rows
    #    123 to 245 comprise the ascending portion of the orbit.  Row 246 is the
    #    the northernmost row.  And rows 247 and 248 begin the descending
    #    portion of the next orbit (path) leading to row 1.  If the ascending
    #    rows are processed, then the corner points will need to be flipped.
    #    UL switched with LR and UR switched with LL.
    #    UL -> LR, UR -> LL, LL -> UR, LR -> UL
    #########################################################################
    def latlong_to_pathrow(self, lat, lon):
        # Validate the lat/long values.
        if (lat < -90.0) or (lat > 90.0):
            print 'latlong_to_pathrow: Latitude argument is invalid: ', lat
            return 0
        if (lon < -180.0) or (lon > 180.0):
            print 'latlong_to_pathrow: Longitude argument is invalid: ', lon
            return 0
        
        # Open the WRS-2 table of lat/longs.  This table is of the form path,
        # row, ctr_lat, ctr_lon, ul_lat, ul_lon, ur_lat, ur_lon, ll_lat,
        # ll_lon, lr_lat, lr_lon
        wrsFile = open (self.WRS_FILE, 'rb')
        wrsReader = csv.reader (wrsFile, delimiter= ',')
        
        # Instantiate the converter tools class for later use
        tools = Converter_Tools()

        # Determine which path/row the lat/long point resides in
        path_list = []               # list of paths
        row_list = []                # list of rows
        dist_center = []             # distance from scene center
        npathrow = 0
        header_line = wrsReader.next()   # skip the header
        for wrs_line in wrsReader:
        #    print 'DEBUG path/row:', wrs_line[0], wrs_line[1]
            # Read the lat/long values for each center and corner point and
            # store as lat/long pairs.
            path = int(wrs_line[0])
            row = int(wrs_line[1])
            wrs_center = [float(wrs_line[2]), float(wrs_line[3])]
            wrs_ul = [float(wrs_line[4]), float(wrs_line[5])]
            wrs_ur = [float(wrs_line[6]), float(wrs_line[7])]
            wrs_ll = [float(wrs_line[8]), float(wrs_line[9])]
            wrs_lr = [float(wrs_line[10]), float(wrs_line[11])]
        
            # If this is an ascending row (123-245) which is nighttime data,
            # then skip to the next row.
            if 123 <= row <= 245:
                continue
        
            # If this is an ascending row (123-245) then the corners need to
            # be flipped around. UL -> LR, UR -> LL, LL -> UR, LR -> UL.
            # Basically switch UL with LR and LL with UR.  Row 122 is the
            # southern most row.  Row 246 is the northernmost row.
#            if 123 <= row <= 245:
#                mytemp = wrs_lr[self.LAT]
#                wrs_lr[self.LAT] = wrs_ul[self.LAT]
#                wrs_ul[self.LAT] = mytemp
#                mytemp = wrs_lr[LON]
#                wrs_lr[LON] = wrs_ul[LON]
#                wrs_ul[LON] = mytemp
#                mytemp = wrs_ll[self.LAT]
#                wrs_ll[self.LAT] = wrs_ur[self.LAT]
#                wrs_ur[self.LAT] = mytemp
#                mytemp = wrs_ll[self.LON]
#                wrs_ll[self.LON] = wrs_ur[self.LON]
#                wrs_urself.[LON] = mytemp
        
            # Determine if there is a left or right sided wrap-around for this
            # scene
            left_wrap = False
            if ((wrs_center[self.LON] < -170.0) and \
                ((wrs_ul[self.LON] > 170.0) or (wrs_ll[self.LON] > 170.0))):
                left_wrap = True
#                print 'DEBUG Left wrap'
        
                # Which corner is farthest west
                if ((wrs_ul[self.LON] > 170.0) and (wrs_ll[self.LON] > 170.0)):
                    # Both points wrap around the dateline
                    if wrs_ul[self.LON] < wrs_ll[self.LON]:
                        wrs_left_bound = wrs_ul[self.LON]
                    else:
                        wrs_left_bound = wrs_ll[self.LON]
                elif wrs_ul[self.LON] > 170.0:
                    # UL corner wraps around
                    wrs_left_bound = wrs_ul[self.LON]
                else:
                    # LL corner wraps around
                    wrs_left_bound = wrs_ll[self.LON]
        
            right_wrap = False
            if ((wrs_center[self.LON] > 170.0) and \
                ((wrs_ur[self.LON] < -170.0) or (wrs_lr[self.LON] < -170.0))):
                right_wrap = True
#                print 'DEBUG Right wrap'
        
                # Which corner is farthest east
                if ((wrs_ur[self.LON] < -170.0) and \
                    (wrs_lr[self.LON] < -170.0)):
                    # Both points wrap around the dateline. Find the least
                    # negative as our eastern boundary.
                    if wrs_ur[self.LON] > wrs_lr[self.LON]:
                        wrs_right_bound = wrs_ur[self.LON]
                    else:
                        wrs_right_bound = wrs_lr[self.LON]
                elif wrs_ur[self.LON] < -170.0:
                    # UR corner wraps around
                    wrs_right_bound = wrs_ur[self.LON]
                else:
                    # LR corner wraps around
                    wrs_right_bound = wrs_lr[self.LON]
        
            # Determine the distance between the UR - UL longitudes.  Then
            # compute the 4% drift that is possible for Landsat from the
            # nominal scene centers in the provided table.  If there is right
            # or left scene wrap, then the scene distance needs to be handled a
            # bit differently.  Look at the distance between the UL and the
            # 180.0 line and then the UR and the -180.0 line.  Remember right
            # or left wrap could be either the upper or the lower corners.
            if (right_wrap == True and wrs_ur[self.LON] < -170.0) or \
               (left_wrap == True and wrs_ul[self.LON] > 170.0):
                lon_drift = (abs(-180.0 - wrs_ur[self.LON]) + \
                (180.0 - wrs_ul[self.LON])) * 0.04;
            else:
                lon_drift = abs (wrs_ur[self.LON] - wrs_ul[self.LON]) * 0.04;
#            print 'DEBUG Longitudinal drift:', lon_drift
        
            # Determine the bounding lat/long coordinates for this path/row. If
            # there is a left or right wrap, that bounding coord has already
            # been determined.
            if left_wrap != True:
                if wrs_ul[self.LON] < wrs_ll[self.LON]:
                    wrs_left_bound = wrs_ul[self.LON]
                else:
                    wrs_left_bound = wrs_ll[self.LON]
        
            if right_wrap != True:
                if wrs_ur[self.LON] > wrs_lr[self.LON]:
                    wrs_right_bound = wrs_ur[self.LON]
                else:
                    wrs_right_bound = wrs_lr[self.LON]
        
            if wrs_ul[self.LAT] > wrs_ur[self.LAT]:
                wrs_upper_bound = wrs_ul[self.LAT]
            else:
                wrs_upper_bound = wrs_ur[self.LAT]
        
            if wrs_ll[self.LAT] < wrs_lr[self.LAT]:
                wrs_lower_bound = wrs_ll[self.LAT]
            else:
                wrs_lower_bound = wrs_lr[self.LAT]
        
#            print 'DEBUG WRS Left bound (before lon drift):', wrs_left_bound
#            print 'DEBUG WRS Right bound (before lon drift):', wrs_right_bound
        
            # Add the longitudinal drift to the bounding extents
            wrs_left_bound -= lon_drift
            wrs_right_bound += lon_drift
#            print 'DEBUG WRS Left bound:', wrs_left_bound
#            print 'DEBUG WRS Right bound:', wrs_right_bound
#            print 'DEBUG WRS Upper bound:', wrs_upper_bound
#            print 'DEBUG WRS Lower bound:', wrs_lower_bound
        
            # Handle the international dateline wrap-around
            if (wrs_left_bound < -180.0):
                wrap = wrs_left_bound + 180.0
                wrs_left_bound = 180.0 + wrap
            if (wrs_left_bound > 180.0):
                wrap = wrs_left_bound - 180.0
                wrs_left_bound = -180.0 + wrap
            if (wrs_right_bound < -180.0):
                wrap = wrs_right_bound + 180.0
                wrs_right_bound = 180.0 + wrap
            if (wrs_right_bound > 180.0):
                wrap = wrs_right_bound - 180.0
                wrs_right_bound = -180.0 + wrap
#            print 'DEBUG WRS Left bound (after wrap):', wrs_left_bound
#            print 'DEBUG WRS Right bound (after wrap):', wrs_right_bound
#            print 'DEBUG WRS Upper bound (after wrap):', wrs_upper_bound
#            print 'DEBUG WRS Lower bound (after wrap):', wrs_lower_bound
        
            # If lat/long falls within the path/row boundaries, then include
            # this path/row in the list.  Special handling is needed for scenes
            # which wrap around the international dateline.
            keep = False
            if (left_wrap == True) or (right_wrap == True):
                if ((wrs_lower_bound <= lat <= wrs_upper_bound) and  \
                    ((wrs_left_bound <= lon <= 180.0) or  \
                     (-180.0 <= lon <= wrs_right_bound))):
                    keep = True
            elif ((wrs_lower_bound <= lat <= wrs_upper_bound) and  \
                (wrs_left_bound <= lon <= wrs_right_bound)):
                keep = True
        
            # If this scene is a keeper, then store the path/row
            if keep == True:
#                print 'DEBUG path/row:', path, row
                # Keep this path/row
                path_list.append (path)
                row_list.append (row)
#                print 'DEBUG center lat:', wrs_center[self.LAT]
#                print 'DEBUG center lon:', wrs_center[self.LON]
        
                # Calculate the distance of this point to the nominal scene
                # center
                dist = tools.distance (wrs_center[self.LAT],
                    wrs_center[self.LON], lat, lon)
                dist_center.append (dist)
#                print 'DEBUG distance from center:', dist_center[npathrow]
        
                # Increment the path/row counter
                npathrow += 1
        
        # Close the WRS input file
        wrsFile.close()
        
        # Sort the scenes based on which scene center is closest to the
        # lat/long point
        for loop in range(npathrow):
            for i in range(loop, npathrow):
                if dist_center[i] < dist_center[loop]:
                    mytemp = dist_center[loop]
                    dist_center[loop] = dist_center[i]
                    dist_center[i] = mytemp
                    mytemp = path_list[loop]
                    path_list[loop] = path_list[i]
                    path_list[i] = mytemp
                    mytemp = row_list[loop]
                    row_list[loop] = row_list[i]
                    row_list[i] = mytemp

        # Sort the scenes based on which scene center is closest to the
        # lat/long point
        results = [npathrow]
        for loop in range(npathrow):
            results.append (path_list[loop])
            results.append (row_list[loop])

        return results
##### End LL2PR_Converter class

class LL2Zone_Converter:

    def __init__(self):
        # use pass statement since nothing needs to be done
        pass
 
    #########################################################################
    # Module: latlong_to_zone
    #
    # Description: This script will take an input latitude/longitude
    #     and compute the UTM zone in which that latitude/longitude resides.
    #
    # Inputs:
    #     <lat> is the latitude in decimal degrees (float, -90.0 to 90.0)
    #     <lon> is the longitude in decimal degrees (float, -180.0 to 180.0)
    #
    # Returns:
    #     <zone> is the integer UTM zone for the latitude/longitude.  If
    #     the zone is negative then it falls below the equator.  Valid values
    #     are from +-1 to +-60.  If the returned zone value is -99 then the
    #     input lat/long was invalid.
    #
    # Developer History:
    #     Gail Schmidt    Original Development          December 2012
    #
    # Notes:
    # The UTM system divides the surface of Earth between 80deg S and 84deg N
    # latitude into 60 zones, each 6deg of longitude in width. Zone 1 covers
    # longitude 180deg to 174deg W; zone numbering increases eastward to zone
    # 60 that covers longitude 174 to 180 East.
    #########################################################################
    def latlong_to_zone(self, lat, lon):
        # Validate the lat/long values.
        if (lat < -90.0) or (lat > 90.0):
            print 'latlong_to_zone: Latitude argument is invalid: ', lat
            return -99
        if (lon < -180.0) or (lon > 180.0):
            print 'latlong_to_zone: Longitude argument is invalid: ', lon
            return -99
        
        # Compute the zone from the longitude
        zone = int(math.floor((lon + 180.0)/6.0)) + 1

        # Make it negative if it falls below the equator
        if (lat < 0.0):
            zone = -zone

        return zone
##### End LL2Zone_Converter class
