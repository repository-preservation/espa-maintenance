from ordering.models import Scene, Order, Configuration,SceneOrder
from django.contrib import admin

#These Admin classes are here to pretty up the admin interface... so we can see the many-to-many relationship.
#They are not used operationally.
    
class SceneOrderInline(admin.TabularInline):
    model = Order.scenes.through
    
    
class SceneOrderAdmin(admin.ModelAdmin):
    fields = ['scene', 'order']
    list_display = ('order', 'scene')
    list_filter = ('scene', 'order')
    search_fields = ['scene', 'order']
    #filter_horizontal = ('scene', 'order'),


class SceneAdmin(admin.ModelAdmin):
    fields = ['name',
              'status',
              'order_date',
              'completion_date',
              'note',
              'distribution_location',
              'tram_order_id',
              'download_url',
              'source_l1t_distro_location',
              'source_l1t_download_url',
              'processing_location',
              'log_file_contents']
    list_display = ('name',
                    'status',
                    'order_date',
                    'completion_date',
                    'processing_location',
                    'tram_order_id')
    list_filter = ('status',
                   'order_date',
                   'completion_date',
                   'processing_location')
    search_fields = ['name', 'status', 'processing_location']

    readonly_fields = ('order_date', 'completion_date')
    
              
    inlines = [
        SceneOrderInline,
    ]
   

class OrderAdmin(admin.ModelAdmin):
    fields = ['orderid', 'email']
    list_display = ('orderid', 'email')
    list_filter = ('orderid', 'email')
    search_fields = ['orderid', 'email']
    filter_horizontal = ('scenes',)
    
    inlines = [
        SceneOrderInline,
    ]
#    exclude = ('scenes',)

class ConfigurationAdmin(admin.ModelAdmin):
    fields = ['key', 'value']
    list_display = ('key', 'value')
    list_filter = ('key', 'value')
    search_fields = ['key', 'value']
    

admin.site.register(Scene,SceneAdmin)
admin.site.register(Order,OrderAdmin)
admin.site.register(Configuration, ConfigurationAdmin)


