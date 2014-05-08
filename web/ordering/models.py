from django.db import models
from django.contrib.auth.models import User

__author__ = "David V. Hill"


class UserProfile (models.Model):
    '''Extends the information attached to ESPA users with a one-to-one relationship.
    The other options were to extend the actual Django User model or create an 
    entirely new User model.  This is the cleanest and recommended method per
    the Django docs.
    '''
    #reference to the User this Profile belongs to
    user = models.OneToOneField(User)
    
    #The EE contactid of this user
    contactid = models.CharField(max_length=10)
    
     
class Order(models.Model):
    '''Persistent object that models a user order for processing.'''

    def __unicode__(self):
        return self.orderid

    ORDER_TYPES = (
        ('level2_ondemand', 'Level 2 On Demand'),
        ('lpvs', 'Product Validation')
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
    
    #This field is in the User object now, but should actually be pulled from 
    #the EarthExplorer profile
    #the users email address    
    email = models.EmailField(db_index=True)
        
    #reference the user that placed this order    
    user = models.ForeignKey(User)
    
    #order_type describes the order characteristics so we can use logic to handle
    #multiple varieties of orders
    order_type = models.CharField(max_length=50,choices=ORDER_TYPES,db_index=True)
    
    #date the order was placed
    order_date = models.DateTimeField('date ordered', blank=True, db_index=True)
    
    #date the order was completed (all scenes either completed or marked unavailable)
    completion_date = models.DateTimeField('date completed', 
                                           blank=True, 
                                           null=True, 
                                           db_index=True)
    
    #one of order.STATUS
    status = models.CharField(max_length=20, choices=STATUS,db_index=True)
    
    #space for users to add notes to orders
    note = models.CharField(max_length=2048, blank=True, null=True)
    
    #json for all product options
    product_options = models.TextField(blank=False,null=False)

    #one of Order.ORDER_SOURCE
    order_source = models.CharField(max_length=10, choices=ORDER_SOURCE,db_index=True)
    
    #populated when the order is placed through EE vs ESPA
    ee_order_id = models.CharField(max_length=13, blank=True)
    

class Scene(models.Model):
    '''Persists a scene object as defined from the ordering and tracking perspective'''

    def __unicode__(self):
        return "%s (%s)" % (self.name, self.status)

    #enumeration of valid status flags a scene may have
    STATUS = (
        ('submitted', 'Submitted'),
        ('onorder', 'On Order'),
        ('oncache', 'On Cache'),
        ('queued', 'Queued'),
        ('processing', 'Processing'),
        ('complete', 'Complete'),
        ('purged', 'Purged'),
        ('unavailable','Unavailable'),
        ('error', 'Error')
    )
    
    #scene file name, with no suffix
    name = models.CharField(max_length=256,db_index=True)
    
    #scene system note, used to add message to users
    note = models.CharField(max_length=2048, blank=True, null=True)

    #Reference to the Order this Scene is associated with
    order = models.ForeignKey(Order)
    
    #full path including filename where this scene has been distributed to 
    #minus the host and port. signifies that this scene is distributed
    product_distro_location = models.CharField(max_length=1024, blank=True)
    
    #full path to where this scene can be downloaded from on the distribution node
    product_dload_url = models.CharField(max_length=1024, blank=True)
    
    #full path including filename of the scene checksum file on distribution filesystem
    cksum_distro_location = models.CharField(max_length=1024,blank=True)
    
    #full url this file can be downloaded from
    cksum_download_url = models.CharField(max_length=1024, blank=True)

    # This will only be populated if the scene had to be placed on order through
    # EE to satisfy the request.
    tram_order_id = models.CharField(max_length=13, blank=True, null=True)
         
    # Flags for order origination.  These will only be populated if the scene request
    # came from EE.    
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
    completion_date = models.DateTimeField('date completed', 
                                           blank=True, 
                                           null=True, 
                                           db_index=True)

    #Final contents of log file... should be put added when scene is marked
    #complete.
    log_file_contents = models.TextField('log_file', blank=True, null=True)
    
                  
class Configuration(models.Model):
    '''Implements a key/value datastore on top of a relational database
    '''
    key = models.CharField(max_length=255, unique=True)
    value = models.CharField(max_length=2048)

    def __unicode__(self):
        return ('%s : %s') % (self.key,self.value)

    def getValue(self, key):
       
        c = Configuration.objects.filter(key=key)
        
        if len(c) > 0:
            return str(c[0].value)
        else:
            return ''



        

