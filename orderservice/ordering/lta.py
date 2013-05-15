from suds.client import Client as SoapClient
from cStringIO import StringIO
import urllib2
import re
import xml.etree.ElementTree as xml

class LtaServices(object):
    ''' Client for all of LTA services from ESPA '''

    urls = {
        "dev" : {
            "orderservice":"http://edclxs151.cr.usgs.gov/OrderWrapperServicedevsys/resources",
            "orderdelivery":"http://edclxs151.cr.usgs.gov/OrderDeliverydevsys/OrderDeliveryService?WSDL",
            "orderupdate":"http://edclxs151.cr.usgs.gov/OrderStatusServicedevsys/OrderStatusService?wsdl",
            "massloader":"http://edclxs151.cr.usgs.gov/MassLoaderdevsys/MassLoader?wsdl"
        },
        "tst" : {
            "orderservice":"http://eedevmast.cr.usgs.gov/OrderWrapperServicedevmast/resources",
            "orderdelivery":"http://edclxs151.cr.usgs.gov/OrderDeliverydevmast/OrderDeliveryService?WSDL",
            "orderupdate":"http://edclxs151.cr.usgs.gov/OrderStatusServicedevmast/OrderStatusService?wsdl",
            "massloader":"http://edclxs151.cr.usgs.gov/MassLoaderdevmast/MassLoader?wsdl"
        },
        "ops" : {
            "orderservice":"http://edclxs152.cr.usgs.gov/OrderWrapperService/resources",
            "orderdelivery":"http://edclxs152.cr.usgs.gov/OrderDeliveryService/OrderDeliveryService?WSDL",
            "orderupdate":"http://edclxs152/OrderStatusService/OrderStatusService?wsdl",
            "massloader":"http://edclxs152.cr.usgs.gov/MassLoader/MassLoader?wsdl"
        }
    }
       

    def __init__(self,environment="dev"):
        self.environment = environment

    def get_url(self,service_name):
        return self.urls[self.environment][service_name]


    def get_xml_header(self):
        return "<?xml version ='1.0' encoding='UTF-8' ?>"

    def sceneid_is_sane(self, sceneid):
        ''' validates against a properly structure L7, L5 or L4 sceneid '''
        p = re.compile('L(E7|T4|T5)\d{3}\d{3}\d{4}\d{3}\w{3}\d{2}')
        if p.match(sceneid):
            return True
        else:
            return False

    def get_product_code(self,sceneid):
        if not self.sceneid_is_sane(sceneid):
            return ''
        
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
        sensor = 'Unknown'
        code = self.get_product_code(sceneid)
        if code == "T273":
            sensor = "LANDSAT_TM"
        elif code == "T272":
            sensor = "LANDSAT_ETM"
        elif code == "T271":
            sensor = "LANDSAT_ETM_SLC_OFF"
            
        return sensor


    #def get_available_sensors(self):
    #    ''' Returns all the available sensors.. not needed at this time but available through order delivery service '''
    #    pass


    def get_available_orders(self):
        ''' Returns all the orders that were submitted for ESPA through EE '''
        returnVal = list()
        client = SoapClient(self.get_url("orderdelivery"))
        resp = client.factory.create("getAvailableOrdersResponse")
        
        try:
            resp = client.service.getAvailableOrders("ESPA")
        except Exception,e:
            print e
            raise e

        #return these to the caller.
        for u in resp.units.unit:
            line = {"order_num":u.orderNbr, "sceneid":u.orderingId, "unit_num":u.unitNbr}
            returnVal.append(line)
        return returnVal
            

    def verify_scenes(self, scene_list):
        ''' Checks to make sure the scene list is valid '''

        url = self.get_url("orderservice")
        operation = 'verifyScenes'
        request_url = "%s/%s" % (url, operation)
        
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
        h = None
        try:
            h = urllib2.urlopen(request)
        except Exception,e:
            print e
            raise Exception("Error occurred verifying scene list:%s" % s)
    
        code = h.getcode()
        response = None
        if code == 200:
            response = h.read()
        else:
            print code

        h.close()

       
        #parse, transform and return response
        retval = dict()
        root = xml.fromstring(response)
        scenes = root.getchildren()
        for s in scenes:
            retval[s.text] = s.attrib['valid']
        return retval    

        
    def get_order_status(self, order_number):
        ''' Returns the status of the supplied order number '''
        #url = self.get_url("orderservice")
        #operation = 'orderStatus'
        #request_url = "%s/%s?orderNumber=%s&username=%s&password=%s" % (url, operation, order_number, self.username, self.password)
        #pass
        retval = dict()
        
        client = SoapClient(self.get_url("orderupdate"))
        resp = client.factory.create("getOrderStatusResponse")
        resp = client.service.getOrderStatus(order_number)
        if resp is None:
            return dict()

        retval['order_num'] = str(resp.order.orderNbr)
        retval['order_status'] = str(resp.order.orderStatus)
        retval['units'] = list()
        
        for u in resp.units.unit:
            unit = dict()
            unit['unit_num'] = int(u.unitNbr)
            unit['unit_status'] = str(u.unitStatus)
            unit['sceneid'] = str(u.orderingId)
            retval['units'].append(unit)

        return retval
        
    
    def update_order(self, order_number, unit_number, status):
        ''' Update the status of orders that ESPA is working on '''
        client = SoapClient(self.get_url("orderupdate"))
        resp = client.factory.create("StatusOrderReturn")
        try:
            resp = client.service.setOrderStatus(orderNumber = str(order_number),
                                                 systemId = "EXTERNAL",
                                                 newStatus = str(status),
                                                 unitRangeBegin = int(unit_number),
                                                unitRangeEnd = int(unit_number))
        except Exception, e:
            return (False,e)
        
        if resp.status == "Pass":
            return (True,)
        else:
            return (False,resp.message,resp.status)
        


    def order_scenes(self, scene_list):
        ''' Orders scenes from the massloader.  Be sure to call verifyscenes before allowing this to happen '''

        client = SoapClient(self.get_url("massloader"))
        tramorder = client.factory.create('order')
        tramscenes = client.factory.create('scenes')
        tramorder.scenes = tramscenes
        for scene in scenes:
            tramscene = client.factory.create('scene')
            tramscene.sceneId = scene.name
            tramscene.productName = self.get_product_code(scene.name)
            tramscene.recipeId = null()
            tramscene.unitComment = null()
            tramscene.parameters = null()
            tramorder.scenes.scene.append(tramscene)
        tramorder.externalRefNumber = '111111'
        tramorder.orderComment = null()
        tramorder.priority = 5
        tramorder.registrationId = '252380'
        tramorder.requestor = 'EE'
        tramorder.roleId = null()
    
        try:
            response = client.service.submitOrder(tramorder)
            return response
        except Exception, e:
            print ("An error occurred submitting the order to tram: %s" % (e))
            #log error
            return -1


    ########################################################
    #This is on hold until the order wrapper shit gets fixed
    ########################################################
    def order_scenes_from_wrapper(self, scene_list):
        ''' Orders scenes through Order Service '''
        url = self.get_url("orderservice")
        operation = 'submitOrder'
        request_url = "%s/%s" % (url, operation)

        sb = StringIO()
        sb.write(self.get_xml_header())
        sb.write("<orderParameters xmlns='http://earthexplorer.usgs.gov/schema/orderParameters' ")
        sb.write("xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' ")
        sb.write("xsi:schemaLocation='http://earthexplorer.usgs.gov/schema/orderParameters http://earthexplorer.usgs.gov/EE/orderParameters.xsd'>")
        sb.write("<username>%s</username>" % self.username)
        sb.write("<password>%s</password>" % self.password)
        sb.write("<requestor>EXTERNAL</requestor>")
        sb.write("<externalReferenceNumber>%s</externalReferenceNumber>" % 1111111)
        sb.write("<priority>5</priority>")
        for s in scene_list:
            sb.write("<scene>")
            sb.write("<sceneId>%s</sceneId>" % s.strip())
            sb.write("<prodCode>%s</prodCode>" % self.get_product_code(s))
            sb.write("<sensor>%s</sensor>" % self.get_sensor_name(s))
            sb.write("</scene>")
        sb.write("</orderParameters>")

        request_body = sb.getvalue()
        
        headers = dict()
        headers['Content-Type'] = 'application/xml'
        headers['Content-Length'] = len(request_body)

        #try/catch this stuff
        request = urllib2.Request(request_url, request_body, headers)
        h = urllib2.urlopen(request)
        
        response = None

        if h.getcode() == 200:
            response = h.read()
        else:
            print h.getcode()

        h.close()

        #print ("Response code:%s" % str(code))
        #print ("Response:")
        print response
        
        

    

    
