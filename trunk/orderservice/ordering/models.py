from django.db import models
from django.contrib import admin
#import paramiko
#import os
#from abc import ABCMeta, abstractmethod, abstractproperty
#from espa.espa import *

    
class TramOrder(models.Model):
    
    def __unicode__(self):
        return self.order_id
    
    order_id = models.CharField(max_length=255)
    order_date = models.DateTimeField('order date', blank=True, null=True)
        
    #need to tie into the orderstatus service to get this.
    #order_status = models.CharField(max_length=255)
    #order_complete_date = models.DateTimeField('delivered date', blank=True, null=True)

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

    #These will be populated if the scene had to be ordered from TRAM.  Should be
    #migrated to a subclass at some point if we have to start handling MODIS data as
    #well.
    #tram_order_id = models.CharField(max_length=256, blank=True)
    #tram_order_date = models.DateTimeField('tram order date', blank=True, null=True)
    tram_order = models.ForeignKey(to=TramOrder, blank=True, null=True)
    
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
    source_distro_location = models.CharField(max_length=1024,blank=True)
    source_download_url = models.CharField(max_length=1024, blank=True)
    

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



        

