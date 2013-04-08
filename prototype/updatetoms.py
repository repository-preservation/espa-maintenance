#!/usr/bin/python

import os.path
import ftplib
import datetime
import os
import re
import time
import subprocess

__author__ = "soa"
__date__ = "$Aug 6, 2009 8:40:26 AM$"

################################################################################
# Toms section
################################################################################
def getTomsData(year):
    
    dloaddir = '/tmp/toms/%s' % str(year)

    #We should have 1 to n datasources now for the given year
    dsList = DatasourceResolver().resolve(year)

    #Sanity check to make sure our download directory exists
    if not os.path.exists(dloaddir):
        print "Creating download directory:%s" % dloaddir
        os.makedirs(dloaddir, 0775)
    else:
        #If this directory already exists and has files in it, they are old
        #and need to be cleaned up
        print "Cleaning download directory:%s" % dloaddir
        for each in os.listdir(dloaddir):
            name = os.path.join(dloaddir, each)
            os.remove(name)

    #Run our downloader, will scan the source dirs, pull down 1 years worth of
    #data
    #try:
        #print "Creating file list to download"
        #fileStatusList = generateFileList(year, dsList)
    #except ftplib.all_errors:
        #print "Could not generate a file listing to download"
        #return

    
     
    print "Downloading data for:%s to:%s" % (str(year),dloaddir)
    #downloadToms(dloaddir, year, fileStatusList)
    downloadToms(dloaddir,year,dsList)
         
            

    #Wipe out the existing ancillary data before we generate the new stuff
    print "Cleaning target directory"
    cleanTomsTargetDir(str(year))

    print "listing dload dir:%s" % dloaddir

    #We need to extract the year, month, and day from the source files we just
    #downloaded, then generate the day of year for that particular file so
    #we know where to put the ozone results at.  The starting files look like
    # L3_ozone_XXX_YYYYMMDD.txt where XXX = either ept (EARTHPROBE),
    #omi (OMI), n7t (NIMBUS7), or m3t (METEOR3).  The final output ozone data
    #needs to look like TOMS_YYYYDDD.hdf where DDD is day of year.

    for filename in os.listdir(dloaddir):
        print "Processing: %s" % filename
        parts = filename.split('_')
        p = parts[len(parts) - 1]
        fulldatepart = p.split('.')
        fulldate = fulldatepart[0]
        #print "OrigDate:" + fulldate
        strYear = fulldate[0:4]
        #print "strYear:%s" % strYear
        strMonth = fulldate[4:6]
        #print "strMonth:%s" % strMonth
        strDay = fulldate[6:8]
        #print "strDay:%s" % strDay

        #Check to see if the day or month start with a 0.  If they do then
        #substring out the second character, which is the actual day or month
        regex = re.compile('^0.')
        if regex.search(strMonth):
            intMonth = strMonth[1:2]
        else:
            intMonth = strMonth

        if regex.search(strDay):
            intDay = strDay[1:2]
        else:
            intDay = strDay


        #Construct a datetime object so we can generate the day of year
        d = datetime.date(int(strYear), int(intMonth), int(intDay))
        #print "Date: %s" % str(d)

        doy = d.strftime('%j')
        #print "Day of Year : %s" % str(doy)
        #print "Survey says:" + str(strYear) + ':' + str(intMonth) + ':' + str(intDay)

        #Now we determine the datasource for the file we're dealing with
        meteor3regex = re.compile('.*_m3t_.*')
        earthproberegex = re.compile('.*_epc_.*')
        nimbusregex = re.compile('.*_n7t_.*')
        omiregex = re.compile('.*_omi_.*')

        ozoneSource = None

        if meteor3regex.search(filename):
            ozoneSource = 'METEOR3'
        elif earthproberegex.search(filename):
            ozoneSource = 'EARTHPROBE'
        elif nimbusregex.search(filename):
            ozoneSource = 'NIMBUS7'
        elif omiregex.search(filename):
            ozoneSource = 'OMI'

        if ozoneSource == None:
            print "Error classifying the downloaded data for:%s ... unknown type" % filename
            return

        #Check to see if the daily file already exists, that means it was pulled
        #from an earlier iteration.  We don't want to wipe it out because we've
        #already cleansed this output directory earlier.  If there's a file here
        #then we must be on the 2nd of n datasources for this particular year.
        #Just skip over this one.
        outputDir = str(ancdir) + '/ep_toms/ozone_' + str(year)

        if not os.path.exists(outputDir):
            os.makedirs(outputDir, 0775)
            changeGrpCmd = '`chgrp saicdev ' + outputDir + '`'
            os.system(changeGrpCmd)

            changePermCmd = '`chmod 777 ' + outputDir + '`'
            os.system(changePermCmd)

        fullOutputFilePath = outputDir + '/TOMS_' + str(year) + str(doy) + '.hdf'
        fullInputPath = os.path.join(dloaddir, filename)

        if not os.path.exists(fullOutputFilePath):
            print "Did not detect existing ozone file for: %s, generating now..." % fullOutputFilePath
            #Ok, there wasn't an existing file so lets generate some ozone data
            cmd = 'convert_ozone %s %s %s' % (fullInputPath, fullOutputFilePath, ozoneSource)
            print "Executing %s" % cmd
            #os.system(cmd)
	    subprocess.call(cmd, shell=True)
            changeGrpCmd = '`chgrp saicdev ' + fullOutputFilePath + '`'
            #os.system(changeGrpCmd)
	    subprocess.call(changeGrpCmd, shell=True)
        else:
            print "Skipping: %s (already exists)" % fullOutputFilePath
        print "### End %s -> %s ###" % (fullInputPath, fullOutputFilePath)
        print " "

    #for loop ends here

    print "Removing downloaded files"
    for each in os.listdir(dloaddir):
        name = os.path.join(dloaddir, each)
        os.remove(name)
    print "Data update complete"


################################################################################
# Check to see if all the files in the list have succeeded
################################################################################
#def isDownloadDone(fileStatusList):
#    for fs in fileStatusList:
#        if not fs.downloaded:
#            return False

#    return True

################################################################################
#This will look for existing ozone data for the given year and wipe it out
################################################################################
def cleanTomsTargetDir(year):
    dir = ancdir + '/ep_toms/ozone_' + year
    regex = re.compile('TOMS_' + year + '*')
    print "Checking to see if %s exists" % dir
    if os.path.exists(dir):
        for each in os.listdir(dir):
            if regex.search(each):
                name = os.path.join(dir, each)
                try:
                    os.remove(name)
                    print "Removed %s" % name
                except:
                    print "Could not remove %s" % name

################################################################################
# Generate a list of files to be downloaded, return list of FileStatus objs
################################################################################
#def generateFileList(year, datasourceList):
#    print "Connecting to server for listing: %s" % datasourceList[0].url

#    returnList = []
#    try:
#        ftp = ftplib.FTP(datasourceList[0].url)
#        ftp.login('anonymous', 'anonymous')
#        ftp.makepasv()
#        for ds in datasourceList:
#            targetDirectory = ds.basepath + '/Y' + str(year)
#            ftp.cwd(targetDirectory)
#            filelist = ftp.nlst()

#            for f in filelist:
#                fs = FileStatus(f, ds, False)
#                returnList.append(fs)

#    except ftplib.all_errors:
#        ftp.quit()
#        print "Error occurred, retrying in 5 seconds..."
#        time.sleep(5)
#        print "Connecting to server: %s" % datasourceList[0].url
#        returnList = []
#        ftp = ftplib.FTP(datasourceList[0].url)
#        ftp.login('anonymous', 'anonymous')
#        ftp.makepasv()
#        for ds in datasourceList:
#            targetDirectory = ds.basepath + '/Y' + str(year)
#            ftp.cwd(targetDirectory)
#            filelist = ftp.nlst()

#            for f in filelist:
#                fs = FileStatus(f, False, ds)
#                returnList.append(fs)
#    ftp.quit()
#    return returnList


def downloadToms(dloadDir, year, datasourceList):
    print "Downloading TOMS data for year:%s" % year
    for ds in datasourceList:
        cmd = 'wget %s' % ds.url
        subprocess.call(cmd, shell=True, cwd=dloadDir)




################################################################################
#Just what it says. --this is old...
################################################################################
#def downloadTomsOld(destination, year, fsList):

#    print "Connecting to server for download: %s" % fsList[0].datasource.url

#    ftp = ftplib.FTP(fsList[0].datasource.url)
#    try:
#        ftp.login('anonymous', 'anonymous')
#        ftp.makepasv()
        
        #for ds in datasourceList:
#        for fs in fsList:
#            filename = fs.filename
#            downloaded = fs.downloaded
#            ds = fs.datasource

#            if not downloaded:
#                targetDirectory = ds.basepath + '/Y' + str(year)
        
#                if not os.path.exists(destination):
#                    os.makedirs(destination, 0775)
            
            
#                target = destination + '/' + filename
#                ftp.cwd('/')
#                ftp.cwd(targetDirectory)
#                cmd = 'RETR ' + filename
#                print "Downloading %s to %s" % (filename, target)
#                ftp.retrbinary(cmd, open(target, 'wb').write)
#                fs.downloaded = True

#        ftp.quit()
#        print "Download finished"
#    except ftplib.all_errors:
#        ftp.quit()
#        raise Exception
    

################################################################################
#Datasource transfer object
################################################################################
class Datasource:
    name = None
    url = None

    def __init__(self, name, url):
        self.name = name
        self.url = url
        

################################################################################
#FileStatus transfer object
################################################################################
#class FileStatus:
#    filename = None
#    downloaded = False
#    datasource = None

#    def __init__(self, filename, datasource, downloaded=False):
#        self.filename = filename
#        self.downloaded = downloaded
#        self.datasource = datasource

################################################################################
#DatasourceResolver
################################################################################
class DatasourceResolver:
    SERVER_URL = 'ftp://toms.gsfc.nasa.gov'

    NIMBUS = '/pub/nimbus7/data/ozone/Y'
    EARTHPROBE = '/pub/eptoms/data/ozone/Y'
    METEOR3 = '/pub/meteor3/data/ozone/Y'
    OMI = '/pub/omi/data/ozone/Y'

    
    def __init__(self):
        pass

    def resolve(self, year):
        dsList = []
        if year in range(1978, 1990):

            url = self.buildURL('NIMBUS', self.SERVER_URL, self.NIMBUS, year)

            if url is not None:
                ds = Datasource('NIMBUS', url)
                dsList.append(ds)
            else:
                print "Could not resolve a datasource for year:%s" % str(year)
                return None

        elif year in range(1991, 1994):

            url = self.buildURL('METEOR3', self.SERVER_URL, self.METEOR3, year)
                        
            if url is not None:
                ds = Datasource('METEOR3', url)
            else:
                print "Could not resolve a datasource for year:%s" % str(year)
                return None

            url2 = self.buildURL('NIMBUS', self.SERVER_URL, self.NIMBUS, year)

            if url2 is not None:
                ds2 = Datasource('NIMBUS', url2)
            else:
                print "Could not resolve a datasource for year:%s" % str(year)
                return None

            dsList.append(ds)
            dsList.append(ds2)

        elif year == 1994:

            url = self.buildURL('METEOR3', self.SERVER_URL, self.METEOR3, year)

            if url is not None:
                ds = Datasource('METEOR3', url)
                dsList.append(ds)
            else:
                print "Could not resolve a datasource for year:%s" % str(year)
                return None



        elif year in range(1996, 2004):

            url = self.buildURL('EARTHPROBE', self.SERVER_URL, self.EARTHPROBE, year)

            if url is not None:
                ds = Datasource('EARTHPROBE', url)
                dsList.append(ds)
            else:
                print "Could not resolve a datasource for year:%s" % str(year)
                return None


        elif year  in range(2004, 2005):

            url = self.buildURL('OMI', self.SERVER_URL, self.OMI, year)

            if url is not None:
                ds = Datasource('OMI', url)
                dsList.append(ds)
            else:
                print "Could not resolve a datasource for year:%s" % str(year)
                return None

            url2 = self.buildURL('EARTHPROBE', self.SERVER_URL, self.EARTHPROBE, year)

            if url2 is not None:
                ds2 = Datasource('EARTHPROBE', url)
                dsList.append(ds2)
            else:
                print "Could not resolve a datasource for year:%s" % str(year)
                return None


        elif year > 2005:

            url = self.buildURL('OMI', self.SERVER_URL, self.OMI, year)

            if url is not None:
                ds = Datasource('OMI', url)
                dsList.append(ds)
            else:
                print "Could not resolve a datasource for year:%s" % str(year)
                return None

        else:
            print "No datasource defined for year:%s" % str(year)
            return None

        return dsList

    def buildURL(self, type, serverUrl, basePath, year):
        if type == 'NIMBUS':
            regex = '*_n7t_*'
        elif type == 'EARTHPROBE':
            regex = '*_epc_*'
        elif type == 'METEOR3':
            regex = '*_m3t_*'
        elif type == 'OMI':
            regex = '*_omi_*'
        else:
            print "Could not categorize datasource for:%s" % type
            return None;

        url = serverUrl + basePath + str(year) + '/' + regex
        return url




################################################################################
# Main method
################################################################################
if __name__ == "__main__":
    print "Updating TOMS data";
    global ancdir
    ancdir = os.environ.get('ANCILLARY')
    if ancdir == None:
        print "ANCILLARY environment variable not set... exiting"
    else:
        print "Running toms update now"
        #for i in range(2002, 2010):
        getTomsData(2009)
	#getTomsData(2003)
    
    
