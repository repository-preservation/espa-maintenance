# Create your views here.
from django.http import HttpResponse
from SimpleXMLRPCServer import SimpleXMLRPCDispatcher
from django.views.decorators.csrf import csrf_exempt
from ordering import core
from ordering.models import Configuration
from ordering.models import DataPoint
from django.db import transaction

__author__ = "David V. Hill"


@csrf_exempt
@transaction.commit_on_success
def rpc_handler(request):
    """
    the actual handler:
    if you setup your urls.py properly, all calls to the xml-rpc service
    should be routed through here.
    If post data is defined, it assumes it's XML-RPC and tries to process
     as such. Empty post assumes you're viewing from a browser and tells you
     about the service.
    """

    d = SimpleXMLRPCDispatcher(allow_none=True, encoding=None)

    if len(request.body):
        d.register_function(_update_status, 'update_status')
        d.register_function(_set_product_error, 'set_scene_error')
        d.register_function(_set_product_unavailable, 'set_scene_unavailable')
        d.register_function(_mark_product_complete, 'mark_scene_complete')
        d.register_function(_handle_orders, 'handle_orders')
        d.register_function(_queue_products, 'queue_products')
        d.register_function(_get_configuration, 'get_configuration')
        d.register_function(_get_products_to_process, 'get_scenes_to_process')
        d.register_function(_get_data_points, 'get_data_points')

        response = HttpResponse(mimetype="application/xml")
        response.write(d._marshaled_dispatch(request.body))
    else:
        response = HttpResponse()
        response.write("<b>This is an XML-RPC Service.</b><br>")
        response.write("You need to invoke it using an XML-RPC Client!<br>")
        response.write("The following methods are available:<ul>")
        methods = d.system_listMethods()

        for method in methods:
            sig = d.system_methodSignature(method)

            # this just reads your docblock, so fill it in!
            help_msg = d.system_methodHelp(method)

            response.write("<li><b>%s</b>: [%s] %s" % (method, sig, help_msg))

        response.write("</ul>")

    response['Content-length'] = str(len(response.content))
    return response


def _update_status(name, orderid, processing_loc, status):
        return core.update_status(name, orderid, processing_loc, status)


def _set_product_error(name, orderid, processing_loc, error):
    return core.set_product_error(name, orderid, processing_loc, error)


def _set_product_unavailable(name, orderid, processing_loc, error, note):
    return core.set_product_unavailable(name,
                                        orderid,
                                        processing_loc,
                                        error,
                                        note)


def _queue_products(order_name_tuple_list, processing_location, job_name):

    return core.queue_products(order_name_tuple_list,
                               processing_location,
                               job_name)


def _mark_product_complete(name,
                           orderid,
                           processing_loc,
                           completed_scene_location,
                           cksum_file_location,
                           log_file_contents_binary):

    log_file_contents = None
    if type(log_file_contents_binary) is str:
        log_file_contents = log_file_contents_binary
    else:
        log_file_contents = log_file_contents_binary.data

    return core.mark_product_complete(name,
                                      orderid,
                                      processing_loc,
                                      completed_scene_location,
                                      cksum_file_location,
                                      log_file_contents)


def _handle_orders():
    return core.handle_orders()


#method to expose master configuration repository to the system
def _get_configuration(key):
    return Configuration().getValue(key)


def _get_products_to_process(limit=None,
                             for_user=None,
                             priority=None,
                             product_types=['landsat', 'modis']):
    return core.get_products_to_process(limit=limit,
                                        for_user=for_user,
                                        priority=priority,
                                        product_types=product_types)


def _get_data_points(tags=[]):
    return DataPoint.get_data_points(tags)
