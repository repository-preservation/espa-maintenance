from suds.client import Client as SoapClient
from cStringIO import StringIO
import urllib2

class LtaServices(object):
    ''' '''
    
    order_delivery_url = None
    order_service_url = None
    order_update_url = None

    urls = {
        "dev" : {
            "orderservice":"http://edclxs151.cr.usgs.gov/OrderWrapperServicedevsys/resources/",
            "orderdelivery":"http://edclxs151.cr.usgs.gov/OrderDeliverydevsys/OrderDeliveryService?WSDL",
            "orderupdate":"http://edclxs151.cr.usgs.gov/OrderStatusServicedevsys/OrderStatusService?wsdl"
        },
        "tst" : {
            "orderservice":"http://eedevmast.cr.usgs.gov/OrderWrapperServicedevmast/resources/",
            "orderdelivery":"http://edclxs151.cr.usgs.gov/OrderDeliverydevmast/OrderDeliveryService?WSDL",
            "orderupdate":"http://edclxs151.cr.usgs.gov/OrderStatusServicedevmast/OrderStatusService?wsdl"
        },
        "ops" : {
            "orderservice":"http://edclxs152.cr.usgs.gov/OrderWrapperService/resources/",
            "orderdelivery":"http://edclxs152.cr.usgs.gov/OrderDeliveryService/OrderDeliveryService?WSDL",
            "orderupdate":"http://edclxs152/OrderStatusService/OrderStatusService?wsdl"
        }
    }
       

    def __init__(self,environment="dev"):
        self.environment = environment


    def get_url(self,service_name):
        return self.urls[self.environment][service_name]


    def get_xml_header(self):
        return "<?xml version ='1.0' encoding='UTF-8' ?>"


    def get_product_code(self,sceneid):
        ''' returns the proper product code (e.g. T273) given a scene id '''
        if sceneid.startswith('LT5') or sceneid.startswith('LT4'):
            return 'T273'
        elif sceneid.startswith('LE7'):
            if int(sceneid[9:13]) >= 2003 and int(sceneid[13:16]) >= 151:
                return 'T271'
            else:
                return 'T272'

            
    def get_sensor_name(self,sceneid):
        ''' returns the EE sensor name (e.g. 'LANDSAT_ETM') given a scene id '''
        sensor = None
        code = self.get_product_code(sceneid)
        if code == "T273":
            sensor = "LANDSAT_TM"
        elif code == "T272":
            sensor = "LANDSAT_ETM"
        elif code == "T271":
            sensor = "LANDSAT_ETM_SLC_OFF"
        else:
            raise Exception("Unknown product code for %s" % s)
        return sensor


    def get_available_sensors(self):
        ''' Returns all the available sensors.. not needed at this time but available through order delivery service '''
        pass


    def get_available_orders(self):
        ''' Returns all the orders that were submitted for ESPA through EE '''
        client = SoapClient(self.get_url("orderdelivery"))
        resp = client.factory.create("getAvailableOrdersResponse")
        
        try:
            resp = client.service.getAvailableOrders("ESPA")
        except Exception,e:
            print e
            raise e

        #return these to the caller.
        for u in resp.units.unit:
            yield {"order_num":u.orderNbr, "sceneid":u.orderingId, "unit_num":u.unitNbr}
            

    def verify_scenes(self, scene_list):
        ''' Checks to make sure the scene list is valid '''

        url = self.get_url("orderservice")
        operation = 'verifyScenes'
        request_url = url + operation
    
        sb = StringIO()
        sb.write(self.get_xml_header())
        sb.write("<sceneList xmlns='http://earthexplorer.usgs.gov/schema/sceneList' ")
        sb.write("xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' ")
        sb.write("xsi:schemaLocation='http://earthexplorer.usgs.gov/schema/sceneList ")
        sb.write("http://earthexplorer.usgs.gov/EE/sceneList.xsd'>")
        for s in scene_list:
            sb.write("<sceneId sensor='%s'>%s</sceneId>" % (self.get_sensor_name(s),s))
        sb.write("</sceneList>")
    
        request_body = sb.getvalue()

        headers = dict()
        headers['Content-Type'] = 'application/xml'
        headers['Content-Length'] = len(request_body)

        request = urllib2.Request(request_url, request_body, headers)

        h = urllib2.urlopen(request)
    
        code = h.getcode()
        response = None
        if code == 200:
            response = h.read()
        else:
            print code

        h.close()

        print ("Response code:%s" % str(code))
        print ("Response:")
        print response
        #parse, transform and return response

        
    def get_order_status(order_number, username, password):
        ''' Retuns the status of the supplied order number '''
        url = self.get_url("orderservice")
        pass
    

    def update_order(self):
        ''' Update the status of orders that ESPA is working on '''
        client = SoapClient(self.get_url("orderupdate"))
        pass


    def order_scenes(self):
        ''' Orders scenes through Order Service '''
        url = self.get_url("orderservice")
        operation = 'submitOrder'
        request_url = url + operation
        pass

    

    
