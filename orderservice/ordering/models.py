from django.db import models
from django.contrib import admin

__author__ = "David V. Hill"

    
class Order(models.Model):

    def __unicode__(self):
        return self.orderid

    DATASETS = (
        ('sr_ondemand', 'Surface Reflectance TM/ETM'),
        ('sr_gls2010', 'Surface Reflectance GLS 2010'),
        ('sr_gls2005', 'Surface Reflectance GLS 2005'),
        ('sr_gls2000', 'Surface Reflectance GLS 2000')
    )
    
    STATUS = (
        ('ordered', 'Ordered'),
        ('partial', 'Partially Filled'),
        ('complete', 'Complete')
        
    )

    ORDER_SOURCE = (
        ('espa', 'ESPA'),
        ('ee', 'EE')
    )
    
    #orderid should be in the format email_MMDDYY_HHMMSS
    orderid = models.CharField(max_length=255, unique=True, db_index=True)
    email = models.EmailField(db_index=True)
    #scenes = models.ManyToManyField(Scene, through = 'SceneOrder')
    #display scene count ordered, scene count completed in UI
    chain = models.CharField(max_length=50,choices=DATASETS,db_index=True)
    order_date = models.DateTimeField('date ordered', blank=True, db_index=True)
    completion_date = models.DateTimeField('date completed', blank=True, null=True, db_index=True)
    status = models.CharField(max_length=20, choices=STATUS,db_index=True)
    note = models.CharField(max_length=2048, blank=True, null=True)
    #json for all product options
    product_options = models.TextField(blank=False,null=False)

    order_source = models.CharField(max_length=10, choices=ORDER_SOURCE,db_index=True)
    ee_order_id = models.CharField(max_length=13, blank=True)

class Scene(models.Model):

    def __unicode__(self):
        display = self.name
        display = display + ' (' + self.status + ')'
        return ('%s') % (display)

    STATUS = (
        ('submitted', 'Submitted'),
        ('onorder', 'On Order'),
        ('oncache', 'On Cache'),
        ('queued', 'Queued'),
        ('staging', 'Staging'),
        ('processing', 'Processing'),
        ('distributing', 'Distributing'),
        ('complete', 'Complete'),
        ('purged', 'Purged'),
        ('unavailable','Unavailable'),
        ('error', 'Error')
    )
    
    #scene file name, with no suffix
    name = models.CharField(max_length=256,db_index=True)
    
    #scene system note, used to add message to users
    note = models.CharField(max_length=2048, blank=True, null=True)

    order = models.ForeignKey(Order)
    
    ###################################################################################
    #  Scene status flags.  The general status of the scene can be determined by the
    #  following flags.  If any path is populated then this means that the path
    #  exists and the file is present at that location
    ###################################################################################
    
    #full path including filename where this scene has been distributed to 
    #minus the host and port. signifies that this scene is distributed
    product_distro_location = models.CharField(max_length=1024, blank=True)
    #full path to where this scene can be downloaded from on the distribution node
    product_dload_url = models.CharField(max_length=1024, blank=True)
    cksum_distro_location = models.CharField(max_length=1024,blank=True)
    cksum_download_url = models.CharField(max_length=1024, blank=True)

    ###################################################################################
    # This will only be populated if the scene had to be placed on order through
    # EE to satisfy the request.
    ###################################################################################
    tram_order_id = models.CharField(max_length=13, blank=True, null=True)
     
    ###################################################################################
    # Flags for order origination.  These will only be populated if the scene request
    # came from EE.
    ###################################################################################
    
    ee_unit_id = models.IntegerField(max_length=11, blank=True, null=True)
    
    ###################################################################################
    # General status flags for this scene
    ###################################################################################
    #Status.... one of Submitted, Ready For Processing, Processing,
    #Processing Complete, Distributed, or Purged
    status = models.CharField(max_length=30, choices=STATUS,db_index=True)

    #Where is this scene being processed at?  (which machine)
    processing_location = models.CharField(max_length=256, blank=True)

    #Time this scene was finished processing
    completion_date = models.DateTimeField('date completed', blank=True, null=True, db_index=True)

    #Final contents of log file... should be put added when scene is marked
    #complete.
    log_file_contents = models.TextField('log_file', blank=True, null=True)
    
               
    
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



        

