from ordering.models import Scene, Order, Configuration,TramOrder
from django.contrib import admin
      
class TramOrderAdmin(admin.ModelAdmin):
    fields = ['order_id', 'order_date']
    list_display = ('order_id', 'order_date')
    
         

#THESE DON"T WORK LIKE YOU"D EXPECT
class SceneInline(admin.StackedInline):
    model = Scene
    
      

###################################


class SceneAdmin(admin.ModelAdmin):
    fields = ['name',
              'status',
              'order',
              #'sourceDS',
              #'destDS',
              'completion_date',
              'note',
              'tram_order',
              'product_distro_location',
              'product_dload_url',
              'cksum_distro_location',
              'cksum_download_url',
              'processing_location',
              'log_file_contents']
    list_display = ('name',
                    'status',
                    'completion_date',
                    'order',
                    'tram_order',
                    #'sourceDS',
                    #'destDS')
                   )
    list_filter = ('status',
                   'completion_date',
                   'processing_location',
                   'order',
                   'tram_order',
                   #'sourceDS',
                   #'destDS')
                  )
    search_fields = ['name', 'status', 'processing_location','order__orderid','tram_order__order_id']
    
   
    #readonly_fields = ('order_date', 'completion_date')
    
              
   
    

class OrderAdmin(admin.ModelAdmin):
    fields = ['orderid', 'email','status','chain','order_date','completion_date','note', 'product_options']
    list_display = ('orderid', 'email','status', 'chain', 'order_date', 'completion_date', 'product_options')
    list_filter = ('orderid', 'email','status','chain','order_date','completion_date')
    search_fields = ['orderid', 'email', 'status','chain']
    
    inlines = [SceneInline,]
    #filter_horizontal = ('scenes',)
    
    #readonly_fields = ('completion_date')
    
 
#    exclude = ('scenes',)

class ConfigurationAdmin(admin.ModelAdmin):
    fields = ['key', 'value']
    list_display = ('key', 'value')
    list_filter = ('key', 'value')
    search_fields = ['key', 'value']
    

    
admin.site.register(Scene,SceneAdmin)
admin.site.register(Order,OrderAdmin)
admin.site.register(Configuration, ConfigurationAdmin)
admin.site.register(TramOrder, TramOrderAdmin)


