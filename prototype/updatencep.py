#!/usr/bin/python

import os.path
import ftplib
import datetime
import os
import re
import time
import subprocess

############################################



def getNcepData(year):
    airFile = 'air.sig995.' + year + '.nc'
    pressureFile = 'pres.sfc.' + year + '.nc'
    waterFile = 'pr_wtr.eatm.' + year + '.nc'
    
    #try:
    downloadNcep(airFile, '/tmp/ncep')
    #except ftplib.all_errors:
	#print "Could not download airFile data"
    #    return

    #try:
    downloadNcep(pressureFile, '/tmp/ncep')
    #except ftplib.all_errors:
	#print "Could not download pressureFile data"
    #    return
        
    
    downloadNcep(waterFile, '/tmp/ncep')
    #except ftplib.all_errors:
    #    print "Could not download waterFile data"
    #    return


    cleanNcepTargetDir(year)

    airFileSource = '/tmp/ncep/' + airFile
    pressureFileSource = '/tmp/ncep/' + pressureFile
    waterFileSource = '/tmp/ncep/' + waterFile

    outputDest = ancdir + '/reanalysis/re_' + year

    if not os.path.exists(outputDest):
        os.makedirs(outputDest, 0775)

    executeNcep(airFileSource, outputDest, year)
    executeNcep(pressureFileSource, outputDest, year)
    executeNcep(waterFileSource, outputDest, year)

    print "Removing downloaded files"
    os.remove(airFileSource)
    os.remove(pressureFileSource)
    os.remove(waterFileSource)

    print "Data update complete"




def executeNcep(fullinputpath, outputdir, year):

    day_of_year = datetime.datetime.now().timetuple().tm_yday


    #for i in range(1, 366):
    for i in range(1, day_of_year + 1):
               
        if i < 10:
            dayofyear = '00' + str(i)
        elif 9 < i < 100:
            dayofyear = '0' + str(i)
        else:
            dayofyear = str(i)

        fulloutputpath = outputdir + "/REANALYSIS_" + year + dayofyear + '.hdf'
        cmd = 'ncep %s %s %s' % (fullinputpath, fulloutputpath, i)
        print "Executing %s" % cmd
        subprocess.call(cmd, shell=True)
        #os.system(cmd)

    print "Ncep run complete"




def cleanNcepTargetDir(year):
    dir = ancdir + '/reanalysis/re_' + year
    regex = re.compile('reanalysis_' + year + '*')
    if os.path.exists(dir):
        for each in os.listdir(dir):
            if regex.search(each):
                name = os.path.join(dir, each)
                try:
                    os.remove(name)
                    print "Removed %s" % name
                except:
                    print "Could not remove %s" % name


def downloadNcep(sourcefilename, destination):

    print "Retrieving %s to %s" % (sourcefilename,destination)
    url = 'ftp://ftp.cdc.noaa.gov/Datasets/ncep.reanalysis/surface/%s' % sourcefilename

    if not os.path.exists(destination):
        print "%s did not exist... creating" % destination
        os.makedirs(destination, 0775)

        print "Updating group ownership for %s" % destination
        changeGrpCmd = 'chgrp saicdev %s' % destination
        subprocess.call(changeGrpCmd, shell=True)

        print "Updating permissions %s" % destination
        changePermCmd = 'chmod 777 %s' % destination
        subprocess.call(changePermCmd, shell=True)

    cmd = 'wget %s' % url
    subprocess.call(cmd, shell=True, cwd=destination)
    


if __name__ == "__main__":
    global ancdir 
    ancdir = os.environ.get('ANCILLARY')
    if ancdir == None:
        print "ANCILLARY environment variable not set... exiting"
    else:
        print "Running ncep update now"
        getNcepData(str(2009))
	#for i in range(2002, 2010):
        #    getNcepData(str(i))
        #    #getNcepData('1980')
        #cleanTargetDir('1980')

    
    
