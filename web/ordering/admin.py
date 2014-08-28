from ordering.models import Scene
from ordering.models import Order
from ordering.models import Configuration
from ordering.models import UserProfile
from ordering.models import Download
from ordering.models import DownloadSection
from ordering.models import DataPoint
from ordering.models import Tag

from django.contrib import admin

__author__ = "David V. Hill"


#THESE DON"T WORK LIKE YOU"D EXPECT
class SceneInline(admin.StackedInline):
    model = Scene


class SceneAdmin(admin.ModelAdmin):
    fields = ['name',
              'sensor_type',
              'job_name',
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
                    'sensor_type',
                    'status',
                    'job_name',
                    'completion_date',
                    'order',
                    )

    list_filter = ('status',
                   'order__priority',
                   'completion_date',
                   'sensor_type',
                   'processing_location',                   
                   'order'
                   )

    search_fields = ['name',
                     'status',
                     'processing_location',
                     'sensor_type',
                     'job_name',
                     'order__orderid',
                     'order__priority',
                     'order__user__email',
                     'order__user__first_name',
                     'order__user__last_name']


class OrderAdmin(admin.ModelAdmin):
    fields = ['user', 'orderid', 'order_source', 'priority', 'status',
              'ee_order_id', 'order_type', ('order_date', 'completion_date'),
              'note', 'product_options',
              ('initial_email_sent', 'completion_email_sent') ]

    list_display = ('orderid', 'user',  'status', 'priority', 'order_type',
                    'order_date', 'completion_date', 'ee_order_id',
                    'order_source', 'product_options')

    list_filter = ('order_date', 'completion_date', 'order_source', 'status', 
                   'priority', 'order_type', 'orderid',  'user', 'user__email',
                   'ee_order_id')

    search_fields = ['user__username',
                     'user__email',
                     'user__first_name',
                     'user__last_name',
                     'orderid',
                     'priority',
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


class TagAdmin(admin.ModelAdmin):
    fields = ['tag', 'description', 'last_updated']
    list_display = ('tag', 'last_updated')
    list_filter = ('tag', 'last_updated')
    search_fields = ['tag', 'description']


class DatapointTagInline(admin.TabularInline):
    model = DataPoint.tags.through
    extra = 3


class DataPointAdmin(admin.ModelAdmin):
    fields = ['key', 'command', 'description', 'enable', 'last_updated']
    list_display = ('key', 'command', 'enable', 'last_updated')
    list_filter = ('enable', 'last_updated', 'tags__tag')
    search_fields = ['key', 'command', 'description', 'tags__tag']
    inlines = (DatapointTagInline,)


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


class DownloadAdmin(admin.ModelAdmin):
    fields = ['target_name',
              'target_url',
              'checksum_name',
              'checksum_url',
              'readme_text',
              'display_order',
              'visible']

    list_display = ['target_name',
                    'target_url',
                    'checksum_name',
                    'checksum_url',
                    'readme_text',
                    'display_order',
                    'visible']

    list_filter = ['visible']

    search_fields = ['target_name',
                     'target_url',
                     'checksum_name',
                     'checksum_url',
                     'readme_text',
                     'visible']


class DownloadInline(admin.StackedInline):
    model = Download


class DownloadSectionAdmin(admin.ModelAdmin):
    fields = ['title', 'text', 'display_order', 'visible']

    list_display = ['title', 'display_order', 'visible']

    list_filter = ['title', 'visible']

    search_fields = ['title', 'text']

    inlines = [DownloadInline, ]


admin.site.register(Scene, SceneAdmin)
admin.site.register(Order, OrderAdmin)
admin.site.register(Configuration, ConfigurationAdmin)
admin.site.register(UserProfile, UserProfileAdmin)
admin.site.register(Download, DownloadAdmin)
admin.site.register(DownloadSection, DownloadSectionAdmin)
admin.site.register(DataPoint, DataPointAdmin)
admin.site.register(Tag, TagAdmin)
