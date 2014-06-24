from cStringIO import StringIO
from django.conf import settings
from suds.client import Client as SoapClient
from ordering.models import Scene

import urllib2
import collections
import xml.etree.ElementTree as xml


__author__ = "David V. Hill"


class LTAService(object):
    ''' Abstract service client for all of LTA services '''

    def __init__(self):
        self.xml_header = "<?xml version ='1.0' encoding='UTF-8' ?>"

    def __repr__(self):
        return "LTAService:%s" % self.__dict__

    def get_url(self, service_name):
        ''' Service locator pattern.  Retrieves proper url from settings.py
        based on the environment

        Keyword args:
        service_name Name of a service as defined in settings.py
        SERVICE_LOCATOR dictionary

        Returns:
        A url to contact the desired service
        '''
        return settings.URL_FOR(service_name)

    def get_product_code(self, sceneid):
        ''' Returns the proper product code given the sensor code

        Keyword args:
        sceneid The scene name to find the product code for

        Returns:
        string 'T273' for LT4 & LT5
        string 'T272' for LE7 prior to day 151 of year 2003 (SLC ON)
        string 'T271' for LE7 after day 151 of year 2003 (SLC OFF)
        '''

        if not Scene.sceneid_is_sane(sceneid):
            return ''

        ''' returns the proper product code (e.g. T273) given a scene id '''
        if sceneid.startswith('LT5') or sceneid.startswith('LT4'):
            return 'T273'
        elif sceneid.startswith('LE7'):
            if int(sceneid[9:13]) >= 2003 and int(sceneid[13:16]) >= 151:
                return 'T271'
            else:
                return 'T272'


class RegistrationServiceClient(LTAService):

    def __init__(self, *args, **kwargs):
        super(RegistrationServiceClient, self).__init__(*args, **kwargs)
        self.url = self.get_url('registration')
        self.client = SoapClient(self.url)

    def login_user(self, username, password):
        '''Authenticates a username/password against the EE Registration
        Service

        Keyword args:
        username EE username
        password EE password

        Returns:
        EE contactId if login is successful
        Exception if unsuccessful with reason
        '''

        return repr(self.client.service.loginUser(username, password))

    def get_user_info(self, username, pw):
        '''Retrieves the email address on file for the supplied credentials

        Keyword args:
        username EE username
        pw       EE password

        Returns:
        Email address on file for the user.
        Exception if the username/password is invalid
        None if there is no email on file.
        '''

        # build a named tuple for the return value
        userinfo = collections.namedtuple('UserInfo',
                                          'email',
                                          'first_name',
                                          'last_name')

        info = self.client.service.getUserInfo(username, pw).contactAddress

        userinfo.email = info.email
        userinfo.first_name = info.firstName
        userinfo.last_name = info.lastName

        return userinfo

    def get_username(self, contactid):
        '''Retrieves the users EE username given their contactid

        Keyword args:
        contactid -- The EE contactid

        Return:
        The EE username
        '''

        return self.client.service.getUserName(contactid)


#TODO: Fix the error checking around the calls to this service.
class OrderWrapperServiceClient(LTAService):
    '''LTA's OrderWrapper Service is a business process service that handles
    populating demographics and interacting with the inventory properly when
    callers order data.  It is implemented as a REST style service that passes
    schema-bound XML as the payload.

    This is the preferred method for ordering data from the LTA (instead of
    calling TRAM services directly), as there are multiple service calls that
    must be performed when placing orders, and only the LTA team really know
    what those calls are.  Their services are largely undocumented.
    '''

    def __init__(self, *args, **kwargs):
        super(OrderWrapperServiceClient, self).__init__(*args, **kwargs)
        self.url = self.get_url("orderservice")

    def get_sensor_name(self, sceneid):
        ''' returns the EE sensor name (e.g. 'LANDSAT_ETM') given a scene id

        Keyword args:
        sceneid Name of the scene to find the EE sensor code for

        Returns:
        string 'LANDSAT_TM' for code LT4 & LT5
        string 'LANDSAT_ETM_PLUS' for LE7 with scan line corrector
        string 'LANDSAT_ETM_SLC_OFF' for LE7 with scan line corrector turned on
        None if the sensor cannot be determined
        '''

        sensor = ''

        code = self.get_product_code(sceneid)

        if code == "T273":
            sensor = "LANDSAT_TM"
        elif code == "T272":
            sensor = "LANDSAT_ETM_PLUS"
        elif code == "T271":
            sensor = "LANDSAT_ETM_SLC_OFF"
        else:
            sensor = None
        return sensor

    def verify_scenes(self, scene_list):
        ''' Checks to make sure the scene list is valid, where valid means
        the scene ids supplied exist in the Landsat inventory and are orderable

        Keyword args:
        scene_list A list of scenes to be verified

        Returns:
        A dictionary with keys matching the scene list and values are 'true'
        if valid, and 'false' if not.

        Return value example:
        dictionary = dict()
        dictionary['LT51490212007234IKR00'] = True
        dictionary['asdf'] = False
        ...
        ...
        ...

        '''

        #build the service + operation url
        request_url = "%s/%s" % (self.url, 'verifyScenes')

        #build the request body
        sb = StringIO()
        sb.write(self.xml_header)
        sb.write("<sceneList ")
        sb.write("xmlns='http://earthexplorer.usgs.gov/schema/sceneList' ")
        sb.write("xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' ")
        sb.write("xsi:schemaLocation='http://earthexplorer.usgs.gov/schema/sceneList ")
        sb.write("http://earthexplorer.usgs.gov/EE/sceneList.xsd'>")

        for s in scene_list:
            sb.write("<sceneId sensor='%s'>%s</sceneId>"
                     % (self.get_sensor_name(s), s))

        sb.write("</sceneList>")

        request_body = sb.getvalue()

        #set the required headers
        headers = dict()
        headers['Content-Type'] = 'application/xml'
        headers['Content-Length'] = len(request_body)

        #send the request and check return status
        request = urllib2.Request(request_url, request_body, headers)
        h = urllib2.urlopen(request)
        code = h.getcode()

        response = None
        if code == 200:
            response = h.read()
        else:
            msg = StringIO()
            msg.write("Error in lta.OrderWrapperServiceClient.verify_scenes\n")
            msg.write("Non 200 response code from service\n")
            msg.write("Response code was:%s" % code)
            # Return the code and reason as an exception
            raise Exception(msg.getvalue())

        h.close()

        #parse, transform and return response
        retval = dict()
        response = response.replace('&', '&amp;')
        response = response.replace('\n', '')

        root = xml.fromstring(response)
        scenes = root.getchildren()

        for s in scenes:
            if s.attrib['valid'] == 'true':
                status = True
            else:
                status = False
            retval[s.text] = status

        return retval

    def order_scenes(self, scene_list, contact_id, priority=5):
        ''' Orders scenes through OrderWrapperService

        Keyword args:
        scene_list A list of scene ids to order
        contactId  The EE user id that is ordering the data
        priority   The priority placed on the backend ordering system.
                   Landsat has asked us to set the priority to 5 for all ESPA
                   orders.

        Returns:
        A dictionary containing the lta_order_id and up to three lists of scene
        ids, organized by their status.  If there are no scenes in the
        ordered status, the ordered list and the lta_order_id will not be
        present.  If there are no scenes in either the invalid or available
        status, then those respective lists will not be present.

        Example 1 (Scenes in each status):
        {
            'lta_order_id': 'abc123456',
            'ordered': ['scene1', 'scene2', 'scene3'],
            'invalid': ['scene4', 'scene5', 'scene6'],
            'available': ['scene7', 'scene8', 'scene9']
         }

        Example 2 (No scenes ordered):
        {
            'invalid': ['scene1', 'scene2', 'scene3'],
            'available': ['scene4', 'scene5', 'scene6']
        }

        Example 3 (No scenes available):
        {
            'lta_order_id': 'abc123456',
            'ordered': ['scene1', 'scene2', 'scene3'],
            'invalid': ['scene4', 'scene5', 'scene6']
        }
        '''

        # build service url
        request_url = "%s/%s" % (self.url, 'submitOrder')

        # build the request body
        sb = StringIO()
        sb.write(self.xml_header)
        sb.write("<orderParameters ")
        sb.write("xmlns='http://earthexplorer.usgs.gov/schema/orderParameters' ")
        sb.write("xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' ")
        sb.write("xsi:schemaLocation='http://earthexplorer.usgs.gov/schema/orderParameters ")
        sb.write("http://earthexplorer.usgs.gov/EE/orderParameters.xsd'>")

        sb.write("<contactId>%s</contactId>" % contact_id)
        sb.write("<requestor>ESPA</requestor>")

        # 1111111 is a dummy value.
        sb.write("<externalReferenceNumber>%s</externalReferenceNumber>"
                 % 1111111)
        sb.write("<priority>%i</priority>" % priority)
        for s in scene_list:
            sb.write("<scene>")
            sb.write("<sceneId>%s</sceneId>" % s.strip())
            sb.write("<prodCode>%s</prodCode>" % self.get_product_code(s))
            sb.write("<sensor>%s</sensor>" % self.get_sensor_name(s))
            sb.write("</scene>")
        sb.write("</orderParameters>")

        request_body = sb.getvalue()

        if settings.DEBUG:
            print("Request body")
            print (request_body)
            print("")

        # set the required headers
        headers = dict()
        headers['Content-Type'] = 'application/xml'
        headers['Content-Length'] = len(request_body)

        # send the request and check response
        request = urllib2.Request(request_url, request_body, headers)
        h = urllib2.urlopen(request)

        response = None
        if h.getcode() == 200:
            response = h.read()
        else:
            msg = StringIO()
            msg.write("Error in lta.OrderWrapperServiceClient.order_scenes\n")
            msg.write("Non 200 response code from service\n")
            msg.write("Response code was:%s" % h.getcode())
            # Return the code and reason as an exception
            raise Exception(msg.getvalue())

        h.close()

        # parse the response
        '''
        Example response for scenes

        <?xml version="1.0" encoding="UTF-8"?>
        <orderStatus xmlns="http://earthexplorer.usgs.gov/schema/orderStatus"
                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                     xsi:schemaLocation="http://earthexplorer.usgs.gov/schema/orderStatus http://edclxs151.cr.usgs.gov/OrderWrapperServicedevsys/orderStatus.xsd">
            <scene>
                <sceneId>LT51490212007234IKR00</sceneId>
                <prodCode>T273</prodCode>
                <sensor>LANDSAT_TM</sensor>
                <status>ordered</status>
                <orderNumber>0621405213419</orderNumber>
            </scene>

            <scene>
                <sceneId>LE70290302013153EDC00</sceneId>
                <prodCode>T271</prodCode>
                <sensor>LANDSAT_ETM_SLC_OFF</sensor>
                <status>available</status>
                <downloadURL>http://edclpdsftp.cr.usgs.gov/lsatL1GT/29/30/2013/LE70290302013153EDC00.tar.gz?iid=LE70290302013153EDC00&did=80752&ver=</downloadURL>
            </scene>

            <scene>
                <sceneId>LE70290302003142EDC00</sceneId>
                <prodCode>T272</prodCode>
                <sensor>LANDSAT_ETM_PLUS</sensor>
                <status>invalid</status>
            </scene>

        </orderStatus>
        '''

        if settings.DEBUG:
            print(response)

        # since the xml is namespaced there is a namespace prefix for every
        # element we are looking for.  Build those values to make the code
        # a little more sane
        response_namespace = 'http://earthexplorer.usgs.gov/schema/orderStatus'
        ns_prefix = ''.join(['{', response_namespace, '}'])
        status_elem = ''.join([ns_prefix, 'status'])
        sceneid_elem = ''.join([ns_prefix, 'sceneId'])
        order_number_elem = ''.join([ns_prefix, 'orderNumber'])
        # leave this here for now.  We aren't using it yet but will when EE
        # straightens out their urls + internal dowloading capability
        #dload_url_elem = ''.join([ns_prefix, 'downloadURL'])

        # escape the ampersands and get rid of newlines if they exist
        # was having problems with the sax escape() function
        response = response.replace('&', '&amp;').replace('\n', '')

        #this will get us the list of <scene></scene> elements
        scene_elements = xml.fromstring(response).getchildren()

        # the dictionary we will return as the response
        # contains the lta_order_id at the top (if anything is ordered)
        # and possibly three lists of scenes, one for each status
        # retval['available'] = list()
        # retval['invalid'] = list()
        # retval['ordered'] = list()
        retval = dict()

        for scene in scene_elements:

            name = scene.find(sceneid_elem).text
            status = scene.find(status_elem).text

            if status == 'available':
                if not 'available' in retval:
                    retval['available'] = [name]
                else:
                    retval['available'].append(name)
            elif status == 'invalid':
                if not 'invalid' in retval:
                    retval['invalid'] = [name]
                else:
                    retval['invalid'].append(name)
            elif status == 'ordered':
                if not 'ordered' in retval:
                    retval['ordered'] = [name]
                else:
                    retval['ordered'].append(name)

                if not 'lta_order_id' in retval:
                    retval['lta_order_id'] = scene.find(order_number_elem).text

        return retval


class OrderUpdateServiceClient(LTAService):

    def __init__(self, *args, **kwargs):
        super(OrderUpdateServiceClient, self).__init__(*args, **kwargs)
        self.url = self.get_url('orderupdate')
        self.client = SoapClient(self.url)

    #TODO - Migrate this call to the OrderWrapperService
    def get_order_status(self, order_number):
        ''' Returns the status of the supplied order number

        Keyword args:
        order_number The EE order number to check status on

        Returns:
        A list of dictionaries containing unit_num, unit_status & sceneid
        '''

        retval = dict()

        resp = self.client.factory.create("getOrderStatusResponse")
        resp = self.client.service.getOrderStatus(order_number)

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
        ''' Update the status of orders that ESPA is working on

        Keyword args:
        order_number The EE order number to update
        unit_number  The unit within the order to update
        status The EE defined status to set the unit to
               'F' for failed
               'C' for complete
               'R' for rejected

        Returns:
        On success, a tuple (True, None, None)
        On failure, a tuple (False, failure message, failure status)
        '''

        returnval = collections.namedtuple('UpdateOrderResponse',
                                           ['success', 'message', 'status'])

        resp = self.client.factory.create('StatusOrderReturn')

        try:
            unit_number = int(unit_number)
            status = str(status)
            resp = self.client.service.setOrderStatus(
                orderNumber=str(order_number),
                systemId='EXTERNAL',
                newStatus=status,
                unitRangeBegin=unit_number,
                unitRangeEnd=unit_number)
        except Exception, e:
            raise e

        if resp.status == 'Pass':
            return returnval(success=True, message=None, status=None)
        else:
            return returnval(success=False,
                             message=resp.message,
                             status=resp.status)


#TODO:Stop using this once the OrderWrapperService is fixed by EE
#TODO: Don't delete this though, just stop using it.
#class MassLoaderServiceClient(LTAService):

#    def order_scenes(self, scene_list):
#        ''' Orders scenes from the TRAM massloader.
#        Be sure to call verifyscenes before allowing this to happen
#
#        Keyword args:
#        scene_list A list of scene ids to be ordered
#
#        Returns:
#        A TRAM orderid
#        Raises exception on error
#        '''
#        client = SoapClient(self.get_url("massloader"))
#        tramorder = client.factory.create('order')
#        tramscenes = client.factory.create('scenes')
#        tramorder.scenes = tramscenes
#        for scene in scene_list:
#            tramscene = client.factory.create('scene')
#            tramscene.sceneId = scene.name
#            tramscene.productName = self.get_product_code(scene.name)
#            tramscene.recipeId = null()
#            tramscene.unitComment = null()
#            tramscene.parameters = null()
#            tramorder.scenes.scene.append(tramscene)
#        tramorder.externalRefNumber = '111111'
#        tramorder.orderComment = null()
#        tramorder.priority = 5
#        tramorder.registrationId = self.tram_id
#        tramorder.requestor = 'EE'
#        tramorder.roleId = null()
#
#        try:
#            response = client.service.submitOrder(tramorder)
#            return response
#        except Exception, e:
#            raise e


class OrderDeliveryServiceClient(LTAService):
    '''EE SOAP Service client to find orders for ESPA which originated in EE'''

    def __init__(self, *args, **kwargs):
        super(OrderDeliveryServiceClient, self).__init__(*args, **kwargs)
        self.url = self.get_url('orderdelivery')
        self.client = SoapClient(self.url)

    def get_available_orders(self):
        ''' Returns all the orders that were submitted for ESPA through EE

        Returns:
        A dictionary of lists that contain dictionaries

        response[ordernumber, email, contactid] = [
            {'sceneid':orderingId, 'unit_num':unitNbr},
            {...}
        ]
        '''
        rtn = dict()
        resp = self.client.factory.create("getAvailableOrdersResponse")

        try:
            resp = self.client.service.getAvailableOrders("ESPA")
        except Exception, e:
            raise e

        #if there were none just return
        if len(resp.units) == 0:
            return rtn

        #return these to the caller.
        for u in resp.units.unit:

            #ignore anything that is not for us
            if str(u.productCode).lower() not in ('sr01', 'sr02'):

                print ("%s is not an ESPA product.  \
                       Order[%s] Unit[%s] Product code[%s]: ignoring"
                       % (u.orderingId, u.orderNbr, u.unitNbr, u.productCode))
                continue

            # get the processing parameters
            pp = u.processingParam

            try:
                email = pp[pp.index("<email>") + 7:pp.index("</email>")]
            except:
                print ("Could not find an email address for \
                order[%s] unit[%s]: rejecting" % (u.orderNbr, u.unitNbr))

                # we didn't get an email... fail the order
                resp = OrderUpdateServiceClient().update_order(u.orderNbr,
                                                               u.unitNbr,
                                                               "F")
                # we didn't get a response from the service
                if not resp.success:
                    raise Exception("Could not update order[%s] unit[%s] \
                    to status:'F'. Error message:%s Error status code:%s"
                                    % (u.orderNbr,
                                       u.unitNbr,
                                       resp.message,
                                       resp.status))
                else:
                    continue

            try:
                # get the contact id
                cid = pp[pp.index("<contactid>") + 11:pp.index("</contactid>")]
            except:
                print ("Could not find a contactid for \
                order[%s] unit[%s]: rejecting" % (u.orderNbr, u.unitNbr))

                # didn't get an email... fail the order
                resp = OrderUpdateServiceClient().update_order(u.orderNbr,
                                                               u.unitNbr,
                                                               "F")
                # didn't get a response from the service
                if not resp.success:
                    raise Exception("Could not update order[%s] unit[%s] \
                    to status:'F'. Error message:%s Error status code:%s"
                                    % (u.orderNbr,
                                       u.unitNbr,
                                       resp.message,
                                       resp.status))
                else:
                    continue

            # This is a dictionary that contains a list of dictionaries
            key = (str(u.orderNbr), str(email), str(cid))

            if not key in rtn:
                rtn[key] = list()

            rtn[key].append({'sceneid': str(u.orderingId),
                             'unit_num': int(u.unitNbr)}
                            )

        return rtn
