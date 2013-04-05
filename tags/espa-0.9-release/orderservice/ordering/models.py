from django.db import models
from django.contrib import admin
import paramiko
import os

class Scene(models.Model):

    def __unicode__(self):
        display = self.name
        display = display + ' (' + self.status + ')'
        return ('%s') % (display)

    _STATUS = (
        ('On Order', 'On Order'),
        ('On Cache', 'On Cache'),
        ('Queued', 'Queued'),
        ('Staging', 'Staging'),
        ('Processing', 'Processing'),
        ('Distributing', 'Distributing'),
        ('Complete', 'Complete'),
        ('Purged', 'Purged'),
        ('Unavailable','Unavailable')
    )

    
    
    tram_order_id = models.CharField(max_length=256, blank=True)
    
    #scene file name, with no suffix
    name = models.CharField(max_length=256)

    
    #scene system note, used to add message to users
    note = models.CharField(max_length=2048, blank=True, null=True)

    ###################################################################################
    #  Scene status flags.  The general status of the scene can be determined by the
    #  following flags.  If any path is populated then this means that the path
    #  exists and the file is present at that location
    ###################################################################################
    
    #full path including filename where this scene has been distributed to 
    #minus the host and port. signifies that this scene is distributed
    distribution_location = models.CharField(max_length=1024, blank=True)

    #full path to where this scene can be downloaded from on the distribution node
    download_url = models.CharField(max_length=1024, blank=True)

    source_l1t_distro_location = models.CharField(max_length=1024,blank=True)

    source_l1t_download_url = models.CharField(max_length=1024, blank=True)
    


    ###################################################################################
    # General status flags for this scene
    ###################################################################################
    #Status.... one of Submitted, Ready For Processing, Processing,
    #Processing Complete, Distributed, or Purged
    status = models.CharField(max_length=30, choices=_STATUS)

    #Where is this scene being processed at?  (which machine)
    processing_location = models.CharField(max_length=256, blank=True)

    #Time this scene was finished processing
    completion_date = models.DateTimeField('date completed', blank=True, null=True)

    #Final contents of log file... should be put added when scene is marked
    #complete.
    log_file_contents = models.TextField('log_file', blank=True, null=True)
    
    #Shouldn't this be on the Order itself?  Doesn't make sense
    #unless we are talking about when it was requested from the
    #archive
    order_date = models.DateTimeField('date ordered', blank=True, null=True)


    ###################################################################################
    # Scene metadata extraction routines.  All information is collected from the
    # actual scene name
    ###################################################################################
    
    #Cooresponding path for this scene
    def getPath(self):
        return self.name[3:6]

    #Corresponding row for this scene
    def getRow(self):
        return self.name[6:9]

    #Scene collection year
    def getYear(self):
        return self.name[9:13]

    #Scene collection julian date
    def getDate(self):
        return self.name[13:16]

    #Scene sensor (L5-TM,L7-SLC-ON,L7-SLC-OFF).  LT5 means Landsat 5,
    #Have to split L7 because of the date the SLC broke (May 30,2003 is SLC on
    # while May 31,2003 is SLC off)
    def getSensor(self):
        if self.name[0:3] =='LT5':
            return 'tm'
        elif self.name[0:3] == 'LE7':
            return 'etm'
            

    #returns the station this scene was acquired from
    def getStation(self):
        return self.name[16:21]

   

    ###################################################################################
    #  Online cache utilities
    ###################################################################################
    
    def isOnCache(self):
        '''Returns true if this scene is at the standard tm/etm branch of the online cache'''
    
        sftp = None
        transport = None
        
        try:
            #need just the directory on the cache, not full path for this op
            path = self.getOnlineCachePath().split(self.name)[0]        
            sftp,transport = self.getOnlineCacheClient()

            #Careful, this will blow up if the path does not exist... need error handling
            #but we'll put it in later.  The path should ALWAYS exist but if something is
            #wrong with the cache it may not be there
       
            print ("Checking for tm/etm scene at:%s") % (str(path))
            contents = sftp.listdir(str(path))
                    
            if contents is None or len(contents) <= 0:
                print ("No tm/etm scenes found at the requested path")
                return False        
            else:
                try:
                    #throws an exception if the value is not found               
                    contents.index(self.name + '.tar.gz')
                    print ("Located tm/etm scene:%s") % (self.name)
                    return True
                except:
                    return False
        finally:
            if sftp is not None:
                sftp.close()
            if transport is not None:
                transport.close()

    
    def isNlapScene(self):
        '''Returns true if this scene is present on the nlaps branch of the online cache'''
    
        path = self.getNLAPSOnlineCachePath()
    
        
        if path is None:#was not an nlap scene
            return False
        else:
            #still might not be an nlaps scene, need to go look for it first
            sftp = None
            transport = None
            try:
                #need just the directory on the cache, not full path for this op
                directory_path = path.split(self.name)[0]        
                sftp,transport = self.getOnlineCacheClient()

                #Careful, this will blow up if the path does not exist... need error handling
                #but we'll put it in later.  The path should ALWAYS exist but if something is
                #wrong with the cache it may not be there
       
                print ("Checking for nlaps scene at:%s") % (str(directory_path))
                try:
                    contents = sftp.listdir(str(directory_path))
                except:
                    #Directory path did not exist
                    return False
                    
                if contents is None or len(contents) <= 0:
                    print ("No nlaps scenes found at the requested path")
                    return False        
                else:
                    try:
                        #throws an exception if the value is not found               
                        contents.index(self.name + '.tar.gz')
                        print ("Located nlaps scene:%s") % (self.name)
                        return True
                    except:
                        #there was an nlaps directory path but the specific scene wasn't in there
                        return False
            finally:
                if sftp is not None:
                    sftp.close()
                if transport is not None:
                    transport.close()

                    
        
    #returns a possible path only, does not guarantee that there is anything there
    def getOnlineCachePath(self):
        base_dir = '/data'
        product_dir = '/standard_l1t'
        sensor = '/' + self.getSensor()
        
        _p = self.stripZeros(self.getPath())
        path = '/' + _p
        _r = self.stripZeros(self.getRow())
        
        row = '/' + _r
        year = '/' + self.getYear()
        name = '/' + self.name
        cachepath = ('%s%s%s%s%s%s%s.tar.gz') % (base_dir,product_dir,sensor,path,row,year,name)
        return cachepath

    #returns a possible path only, does not guarantee that there is anything there
    def getNLAPSOnlineCachePath(self):
        onlinepath = self.getOnlineCachePath()
        #look to see if this is a tm scene... if it is then replace tm with nlaps
        if onlinepath.find('tm') != -1:
            onlinepath = onlinepath.replace('tm', 'nlaps/tm')
        else:
            #this isn't a possible nlaps scene, return None
            onlinepath = None
        return onlinepath
        

    def stripZeros(self, value):
        returnval = None
        val = value
        #print ("Checking for starting 0 on:%s") %(val)
        if val.startswith('0'):
            returnval = val[1:len(val)]
            if returnval.startswith('0'):
                returnval = self.stripZeros(returnval)

        return returnval

    ###################################################################################
    # Path builder methods.  Only generates the expected paths, does not actually
    # construct paths or mean that the files are present in the returned paths.
    ###################################################################################
    
    #generates the path where we should store the scene on HDFS for input. This does not
    #denote that the path exists of the scene has been transferred
    #def buildHDFSInputPath(self):
    #    base_dir='/espa'
    #    order_base_dir = '/orders'
    #    order_dir = '/' + self.order.orderid
    #    return ('%s%s%s%s') % (base_dir,order_base_dir,order_dir,'/input')

    #generates the path where we should store the scene on HDFS for output. This
    #does not denote that the path exists or the scene is done processing.
    #def buildHDFSOutputPath(self):
    #    base_dir='/espa'
    #    order_base_dir = '/orders'
    #    order_dir = '/' + self.order.orderid
    #    return ('%s%s%s%s') % (base_dir,order_base_dir,order_dir,'/output')

    #generates the path where we should store the scene for distribution. This
    #does not denote that the path exists or the scene is transferred.
    def buildDistributionPath(self):
        base_dir='/data1'
        product_dir = '/espa'
        product_type_dir = '/user'
        order_dir = '/' + self.order.orderid
        return ('%s%s%s%s') % (base_dir,product_dir,product_type_dir,order_dir)

    ###################################################################################
    # File transfer utilities
    ###################################################################################
    #def stageToHDFS(self):

    #    sftp = self.getOnlineCacheClient()
        
        #path = '/data/standard_l1t/tm/1/58/2009/LT50010582009322CUB00.tar.gz'
    #    path = self.getPathOnCache()

        #configure this
    #    tmp_dir = '/tmp'
    #    localpath = tmp_dir + '/' + self.name #'/tmp/LT50010582009322CUB00.tar.gz'
    #    sftp.get(path, localpath)
    #    sftp.close()
        

        #now transfer from tmp to hdfs then clean up tmp
        #os.popen('/home/dhill/bin/hadoop/bin/hadoop dfs -copyFromLocal localpath order#/in
        #os.popen(('/home/dhill/bin/hadoop/bin/hadoop dfs -copyFromLocal %s %s') %(localpath, buildHDFSInputPath())
        
        #delete file from /tmp

    
    
    ###################################################################################
    # Miscellaneous utilities
    ###################################################################################

    #build a connection to the online cache and returns it
    def getOnlineCacheClient(self):
        #configure this
        #host = 'edclxs140.cr.usgs.gov'
        host = Configuration().getValue('online.cache.host')

        #configure this
        #port = 22
        port = Configuration().getValue('online.cache.port')
        
        transport = paramiko.Transport((host,int(port)))

        #configure this
        #password = '1qw2!QW@'
        password = Configuration().getValue('online.cache.password')

        #configure this
        #username = 'espa'
        username = Configuration().getValue('online.cache.username')

        #NEED TO RETURN BOTH OF THESE SO THE CALLER CAN WIPE OUT THE TRANSPORT TOO
        transport.connect(username = username, password = password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        return sftp,transport

        
    #Utility to remove .tar.gz from end of scene name if present
    def __strip_suffix(self):
        return self.name.split('.tar.gz')[0]



class Order(models.Model):

    def __unicode__(self):
        return self.orderid
    
    #orderid should be in the format email_MMDDYY_HHMMSS
    orderid = models.CharField(max_length=255, unique=True)
    email = models.CharField(max_length=256)
    scenes = models.ManyToManyField(Scene, through = 'SceneOrder')
    #scenes = models.ManyToManyField(Scene)

#many to many relational object
class SceneOrder(models.Model):
    scene = models.ForeignKey(Scene)
    order = models.ForeignKey(Order)

   

#Simple class to provide centralized configuration for the system
class Configuration(models.Model):
    key = models.CharField(max_length=255, unique=True)
    value = models.CharField(max_length=2048)

    def __unicode__(self):
        return ('%s : %s') % (self.key,self.value)

    def getValue(self, key):
        #print ("Getting config value for key:%s" % key)
        c = Configuration.objects.filter(key=key)
        #print ("returned value for config key is:%s" % c)
        if len(c) > 0:
            return str(c[0].value)
        else:
            return ''
        

