from ordering.models import Scene
from ordering.models import Order
from ordering.models import Configuration
from ordering.models import UserProfile

from django.contrib import admin

__author__ = "David V. Hill"


#THESE DON"T WORK LIKE YOU"D EXPECT
class SceneInline(admin.StackedInline):
    model = Scene


class SceneAdmin(admin.ModelAdmin):
    fields = ['name',
              'order',
              'status',
              'completion_date',
              'tram_order_id',
              'ee_unit_id',
              'product_distro_location',
              'product_dload_url',
              'cksum_distro_location',
              'cksum_download_url',
              'processing_location',
              'note',
              'log_file_contents']

    #this should stop django from querying the Order in addition to the Scene
    list_select_related = ()

    readonly_fields = ('order', 'name', 'tram_order_id', 'ee_unit_id',
                       'product_distro_location', 'product_dload_url',
                       'cksum_distro_location', 'cksum_download_url',
                       'processing_location')

    list_display = ('name',
                    'status',
                    'completion_date',
                    'order',
                    )

    list_filter = ('status',
                   'completion_date',
                   'processing_location',
                   'order',
                   )

    search_fields = ['name',
                     'status',
                     'processing_location',
                     'order__orderid',
                     'order__user__email',
                     'order__user__first_name',
                     'order__user__last_name']


class OrderAdmin(admin.ModelAdmin):
    fields = ['user', 'orderid', 'order_source', 'status',
              'ee_order_id', 'order_type', ('order_date', 'completion_date'),
              'note', 'product_options', ]

    list_display = ('orderid', 'user',  'status', 'order_type',
                    'order_date', 'completion_date', 'ee_order_id',
                    'order_source', 'product_options')

    list_filter = ('order_date', 'completion_date', 'order_source', 'status',
                   'order_type', 'orderid',  'user', 'user__email',
                   'ee_order_id')

    search_fields = ['user__username',
                     'user__email',
                     'user__first_name',
                     'user__last_name',
                     'orderid',
                     'order_source',
                     'ee_order_id',
                     'status',
                     'order_type']

    readonly_fields = ('order_source', 'orderid', 'ee_order_id', 'order_type')

    inlines = [SceneInline, ]


class ConfigurationAdmin(admin.ModelAdmin):
    fields = ['key', 'value']
    list_display = ('key', 'value')
    list_filter = ('key', 'value')
    search_fields = ['key', 'value']


class UserProfileAdmin(admin.ModelAdmin):

    fields = ['user', 'contactid']

    list_display = ['user', 'contactid']

    list_filter = ['user', 'contactid']

    search_fields = ['user__username',
                     'user__email',
                     'user__first_name',
                     'user__last_name',
                     'contactid']

    readonly_fields = ('user',)


admin.site.register(Scene, SceneAdmin)
admin.site.register(Order, OrderAdmin)
admin.site.register(Configuration, ConfigurationAdmin)
admin.site.register(UserProfile, UserProfileAdmin)
