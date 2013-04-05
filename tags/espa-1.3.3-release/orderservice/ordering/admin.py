from ordering.models import Scene, Order, Configuration,DataSource,TramOrder,LedapsAncillary
from django.contrib import admin
  

class LedapsAncillaryAdmin(admin.ModelAdmin):
    fields = ['dayAndYear','day','year','last_updated','air_filename','water_filename','pres_filename','ozone_filename']
    list_display = ('dayAndYear','day','year','last_updated','air_filename','water_filename','pres_filename','ozone_filename')
    list_filter = ('day','year',)
    search_fields = ['dayAndYear', 'air_filename', 'water_filename', 'pres_filename', 'ozone_filename']
    readonly_fields = ('last_updated',)
    
class TramOrderAdmin(admin.ModelAdmin):
    fields = ['order_id', 'order_date']
    list_display = ('order_id', 'order_date')
    

class DataSourceAdmin(admin.ModelAdmin):
    fields = ['name','username','password','host','port']
    
    list_display = ('name', 'username', 'password', 'host', 'port')
         

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
              'source_distro_location',
              'source_download_url',
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
    fields = ['orderid', 'email','status','chain','order_date','completion_date','note']
    list_display = ('orderid', 'email','status', 'chain', 'order_date', 'completion_date')
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
admin.site.register(DataSource, DataSourceAdmin)
admin.site.register(LedapsAncillary, LedapsAncillaryAdmin)


