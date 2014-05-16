# Create your views here.
from django.http import HttpResponse
from SimpleXMLRPCServer import SimpleXMLRPCDispatcher
from django.views.decorators.csrf import csrf_exempt
from ordering.core import *
from ordering.models import Configuration
from django.db import transaction

__author__ = "David V. Hill"

#Create a Dispatcher; this handles the calls and translates info to function maps
#dispatcher = SimpleXMLRPCDispatcher() # Python 2.4
#dispatcher = SimpleXMLRPCDispatcher(allow_none=False, encoding=None) # Python 2.5

#need this so Django's built in cross site request forgery middleware will
#allow the call to come thru to the rpc handler
@csrf_exempt
@transaction.commit_on_success
def rpc_handler(request):
	"""
	the actual handler:
	if you setup your urls.py properly, all calls to the xml-rpc service
	should be routed through here.
	If post data is defined, it assumes it's XML-RPC and tries to process as such
	Empty post assumes you're viewing from a browser and tells you about the service.
	"""
	#moving this here to see if it will fix the thread leaks
	dispatcher = SimpleXMLRPCDispatcher(allow_none=False, encoding=None) # Python 2.5

	if len(request.POST):
		dispatcher.register_function(_update_status, 'updateStatus')
		dispatcher.register_function(_set_scene_error, 'setSceneError')
		dispatcher.register_function(_set_scene_unavailable, 'setSceneUnavailable')
		dispatcher.register_function(_mark_scene_complete, 'markSceneComplete')
		dispatcher.register_function(_get_configuration, 'getConfiguration')
		dispatcher.register_function(_get_scenes_to_process, 'getScenesToProcess')
		dispatcher.register_function(_get_scenes_to_purge, 'getScenesToPurge')
		dispatcher.register_function(_get_scene_input_path, 'getSceneInputPath')

		#if our leak isn't fixed, try checking to see if we need to close the response here.
		response = HttpResponse(mimetype="application/xml")
		response.write(dispatcher._marshaled_dispatch(request.raw_post_data))
	else:
		response = HttpResponse()
		response.write("<b>This is an XML-RPC Service.</b><br>")
		response.write("You need to invoke it using an XML-RPC Client!<br>")
		response.write("The following methods are available:<ul>")
		methods = dispatcher.system_listMethods()

		for method in methods:
			# right now, my version of SimpleXMLRPCDispatcher always
			# returns "signatures not supported"... :(
			# but, in an ideal world it will tell users what args are expected
			sig = dispatcher.system_methodSignature(method)

			# this just reads your docblock, so fill it in!
			help =  dispatcher.system_methodHelp(method)

			response.write("<li><b>%s</b>: [%s] %s" % (method, sig, help))

		response.write("</ul>")
		response.write('<a href="http://www.djangoproject.com/"> <img src="http://media.djangoproject.com/img/badges/djangomade124x25_grey.gif" border="0" alt="Made with Django." title="Made with Django."></a>')

	response['Content-length'] = str(len(response.content))
	return response

def _update_status(name, orderid, processing_loc, status):
        return update_status(name, orderid, processing_loc, status)

def _set_scene_error(name, orderid, processing_loc, error):
	return set_scene_error(name, orderid, processing_loc, error)

def _set_scene_unavailable(name, orderid, processing_loc, error, note):
	return set_scene_unavailable(name, orderid, processing_loc, error, note)

def _mark_scene_complete(name,orderid,processing_loc,completed_scene_location,cksum_file_location,log_file_contents_binary):
        
        log_file_contents = None
        if type(log_file_contents_binary) is str:
            log_file_contents = log_file_contents_binary
        else:
            log_file_contents = log_file_contents_binary.data
        
        return mark_scene_complete(name,orderid,processing_loc,completed_scene_location,cksum_file_location,log_file_contents)

#method to expose master configuration repository to the system
def _get_configuration(key):
        return Configuration().getValue(key)

def _get_scenes_to_process():
     return get_scenes_to_process()   

def _get_scenes_to_purge():
        return get_scenes_to_purge()

def _get_scene_input_path(sceneid):
        return get_scene_input_path(sceneid)

	
	

