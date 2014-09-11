import settings
import re
import os
import utilities
import httplib
import xmlrpclib


class ProductNotImplemented(NotImplementedError):
    '''Exception to be thrown when trying to instantiate an unsupported
    product'''

    def __init__(self, product_id, *args, **kwargs):
        '''Constructor for the product not implemented

        Keyword args:
        product_id -- The product id of that is not implemented

        Return:
        None
        '''
        self.product_id = product_id
        super(ProductNotImplemented, self).__init__(*args, **kwargs)


class SensorProduct(object):
    '''Base class for all sensor products'''

    # full path to the file containing the input product
    input_file_path = None

    # full path where the output file should be placed
    #output_file_path = None

    # http, ftp, scp, file, etc
    #input_scheme = None
    #input_host = None
    #input_port = None
    input_file_name = None
    #input_user = None
    #input_pw = None
    #input_url = None

    # http, ftp, scp, file, etc
    #output_scheme = None
    #output_host = None
    #output_port = None
    #output_file_name = None
    #output_user = None
    #output_pw = None
    #output_url = None

    # landsat sceneid, modis tile name, aster granule id, etc.
    product_id = None

    # lt5, le7, mod, myd, etc
    sensor_code = None

    # tm, etm, terra, aqua, etc
    sensor_name = None

    # four digits
    year = None

    # three digits
    doy = None

    # last 5 for LANDSAT, collection # for MODIS
    version = None

    # this is a dictionary
    default_pixel_size = {}

    def __init__(self, product_id):
        '''Constructor for the SensorProduct base class

        Keyword args:
        product_id -- The product id for the requested product
                      (e.g. Landsat is scene id, Modis is tilename, minus
                      file extension)

        Return:
        None
        '''
        self.product_id = product_id
        self.sensor_code = product_id[0:3]
        self.sensor_name = settings.SENSOR_NAMES[self.sensor_code.upper()]

    # subclasses should override, construct and return True/False
    def input_exists(self):
        ''' '''
        raise NotImplementedError()

    # subclasses should override, construct and return True/False
    #def output_exists(self):
    #    raise NotImplementedError()

    #def get_input_product(self, target_directory):
    #    raise NotImplementedError()


class Modis(SensorProduct):
    version = None
    short_name = None
    horizontal = None
    vertical = None
    date_acquired = None
    date_produced = None

    def __init__(self, product_id):

        super(Modis, self).__init__(product_id)

        input_file_name = ''.join([product_id,
                                   settings.MODIS_INPUT_FILENAME_EXTENSION])

        self.input_file_name = input_file_name

        parts = product_id.strip().split('.')

        self.short_name = parts[0]
        self.date_acquired = parts[1][1:]
        self.year = self.date_acquired[0:4]
        self.doy = self.date_acquired[4:8]

        hv = parts[2]
        self.horizontal = hv[1:3]
        self.vertical = hv[4:6]
        self.version = parts[3]
        self.date_produced = parts[4]

        # set the default pixel sizes

        # this comes out to 09A1, 09GA, 13A1, etc
        _product_code = self.short_name.split(self.sensor_code)[1]

        _meters = settings.DEFAULT_PIXEL_SIZE['meters'][_product_code]

        _dd = settings.DEFAULT_PIXEL_SIZE['dd'][_product_code]

        self.default_pixel_size = {'meters': _meters, 'dd': _dd}

    def _build_input_file_path(self, base_source_path):

        date = utilities.date_from_doy(self.year, self.doy)

        path_date = "%s.%s.%s" % (date.year,
                                  str(date.month).zfill(2),
                                  str(date.day).zfill(2))

        input_file_extension = settings.MODIS_INPUT_FILENAME_EXTENSION

        input_file_name = "%s.A%s%s.h%sv%s.%s.%s%s" % (self.short_name,
                                                       self.year,
                                                       self.doy,
                                                       self.horizontal,
                                                       self.vertical,
                                                       self.version,
                                                       self.date_produced,
                                                       input_file_extension)

        self.input_file_path = os.path.join(
            base_source_path,
            '.'.join([self.short_name.upper(), self.version.upper()]),
            path_date.upper(),
            input_file_name)

    def input_exists(self):

        host = settings.MODIS_INPUT_CHECK_HOST
        port = settings.MODIS_INPUT_CHECK_PORT

        conn = None

        try:
            conn = httplib.HTTPConnection(host, port)

            conn.request("HEAD", self.input_file_path)

            resp = conn.getresponse()

            if resp.status == 200:
                return True
            else:
                return False
        except Exception, e:
            print ("Exception checking inputs:%s" % e)
            return False
        finally:
            conn.close()
            conn = None


class Terra(Modis):
    def __init__(self, product_id):
        super(Terra, self).__init__(product_id)
        self._build_input_file_path(settings.TERRA_BASE_SOURCE_PATH)


class Aqua(Modis):
    def __init__(self, product_id):
        super(Aqua, self).__init__(product_id)
        self._build_input_file_path(settings.AQUA_BASE_SOURCE_PATH)


class ModisTerra09A1(Terra):
    def __init__(self, product_id):
        super(ModisTerra09A1, self).__init__(product_id)


class ModisTerra09GA(Terra):
    def __init__(self, product_id):
        super(ModisTerra09GA, self).__init__(product_id)


class ModisTerra09GQ(Terra):
    def __init__(self, product_id):
        super(ModisTerra09GQ, self).__init__(product_id)


class ModisTerra09Q1(Terra):
    def __init__(self, product_id):
        super(ModisTerra09Q1, self).__init__(product_id)


class ModisTerra13A1(Terra):
    def __init__(self, product_id):
        super(ModisTerra13A1, self).__init__(product_id)


class ModisTerra13A2(Terra):
    def __init__(self, product_id):
        super(ModisTerra13A2, self).__init__(product_id)


class ModisTerra13A3(Terra):
    def __init__(self, product_id):
        super(ModisTerra13A3, self).__init__(product_id)


class ModisTerra13Q1(Terra):
    def __init__(self, product_id):
        super(ModisTerra13Q1, self).__init__(product_id)


class ModisAqua09A1(Aqua):
    def __init__(self, product_id):
        super(ModisAqua09A1, self).__init__(product_id)


class ModisAqua09GA(Aqua):
    def __init__(self, product_id):
        super(ModisAqua09GA, self).__init__(product_id)


class ModisAqua09GQ(Aqua):
    def __init__(self, product_id):
        super(ModisAqua09GQ, self).__init__(product_id)


class ModisAqua09Q1(Aqua):
    def __init__(self, product_id):
        super(ModisAqua09Q1, self).__init__(product_id)


class ModisAqua13A1(Aqua):
    def __init__(self, product_id):
        super(ModisAqua13A1, self).__init__(product_id)


class ModisAqua13A2(Aqua):
    def __init__(self, product_id):
        super(ModisAqua13A2, self).__init__(product_id)


class ModisAqua13A3(Aqua):
    def __init__(self, product_id):
        super(ModisAqua13A3, self).__init__(product_id)


class ModisAqua13Q1(Aqua):
    def __init__(self, product_id):
        super(ModisAqua13Q1, self).__init__(product_id)


class Landsat(SensorProduct):
    path = None
    row = None
    station = None

    def __init__(self, product_id):

        product_id = product_id.strip()

        super(Landsat, self).__init__(product_id)

        input_file_name = ''.join([product_id,
                                   settings.LANDSAT_INPUT_FILENAME_EXTENSION])

        self.input_file_name = input_file_name

        self.path = utilities.strip_zeros(product_id[3:6])
        self.row = utilities.strip_zeros(product_id[6:9])
        self.year = product_id[9:13]
        self.doy = product_id[13:16]
        self.station = product_id[16:19]
        self.version = product_id[19:21]

        self.input_file_path = os.path.join(
            settings.LANDSAT_BASE_SOURCE_PATH,
            self.sensor_name,
            self.path,
            self.row,
            self.year,
            self.input_file_name)

        #set the default pixel sizes
        _pixels = settings.DEFAULT_PIXEL_SIZE

        _meters = _pixels['meters'][self.sensor_code.upper()]

        _dd = _pixels['dd'][self.sensor_code.upper()]

        self.default_pixel_size = {'meters': _meters, 'dd': _dd}

    def input_exists(self):
        ''' Checks the existence of a landsat tm/etm+ scene on the online
        cache via call to the ESPA scene cache'''

        host = settings.LANDSAT_INPUT_CHECK_HOST
        port = settings.LANDSAT_INPUT_CHECK_PORT
        base_url = settings.LANDSAT_INPUT_CHECK_BASE_PATH

        url = ''.join(["http://", host, ":", str(port), base_url])
        server = xmlrpclib.ServerProxy(url)

        result = server.scenes_exist([self.product_id])
        nlaps = server.is_nlaps([self.product_id])

        if self.product_id in result and not self.product_id in nlaps:
            return True
        else:
            return False


class LandsatTM(Landsat):
    def __init__(self, product_id):
        super(LandsatTM, self).__init__(product_id)


class LandsatETM(Landsat):
    def __init__(self, product_id):
        super(LandsatETM, self).__init__(product_id)


def instance(product_id):
    '''
    Supported MODIS products
    MOD09A1 MOD09GA MOD09GQ MOD09Q1 MYD09A1 MYD09GA MYD09GQ MYD09Q1
    MOD13A1 MOD13A2 MOD13A3 MOD13Q1 MYD13A1 MYD13A2 MYD13A3 MYD13Q1

    MODIS FORMAT:   MOD09GQ.A2000072.h02v09.005.2008237032813

    Supported LANDSAT products
    LT4 LT5 LE7

    LANDSAT FORMAT: LE72181092013069PFS00

    '''

    #remove known file extensions before comparison
    #do not alter the case of the actual product_id!
    if product_id.lower().endswith(settings.MODIS_INPUT_FILENAME_EXTENSION):
        index = product_id.lower().index(settings.MODIS_INPUT_FILENAME_EXTENSION)
        #leave original case intact
        product_id = product_id[0:index] 
    elif product_id.lower().endswith(settings.LANDSAT_INPUT_FILENAME_EXTENSION):
        index = product_id.lower().index(settings.LANDSAT_INPUT_FILENAME_EXTENSION)
        #leave original case intact
        product_id = product_id[0:index]

    #ok to modify case here for comparison in regex
    _id = product_id.lower().strip()


    instances = {
        'tm': (r'^lt[4|5]\d{3}\d{3}\d{4}\d{3}[a-z]{3}[a-z0-9]{2}$',
               LandsatTM),

        'etm': (r'^le7\d{3}\d{3}\d{4}\d{3}\w{3}.{2}$',
                LandsatETM),

        'mod09a1': (r'^mod09a1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisTerra09A1),

        'mod09ga': (r'^mod09ga\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisTerra09GA),

        'mod09gq': (r'^mod09gq\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisTerra09GQ),

        'mod09q1': (r'^mod09q1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisTerra09Q1),

        'mod13a1': (r'^mod13a1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisTerra13A1),

        'mod13a2': (r'^mod13a2\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisTerra13A2),

        'mod13a3': (r'^mod13a3\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisTerra13A3),

        'mod13q1': (r'^mod13q1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisTerra13Q1),

        'myd09a1': (r'^myd09a1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisAqua09A1),

        'myd09ga': (r'^myd09ga\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisAqua09GA),

        'myd09gq': (r'^myd09gq\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisAqua09GQ),

        'myd09q1': (r'^myd09q1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisAqua09Q1),

        'myd13a1': (r'^myd13a1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisAqua13A1),

        'myd13a2': (r'^myd13a2\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisAqua13A2),

        'myd13a3': (r'^myd13a3\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisAqua13A3),

        'myd13q1': (r'^myd13q1\.a\d{7}\.h\d{2}v\d{2}\.005\.\d{13}$',
                    ModisAqua13Q1)
    }

    for key in instances.iterkeys():
        if re.match(instances[key][0], _id):
            return instances[key][1](product_id.strip())

    msg = "[%s] is not a supported sensor product" % product_id
    raise ProductNotImplemented(product_id, msg)
