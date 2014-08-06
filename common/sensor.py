import settings
import re
import os
import util

class SensorProduct(object):

    # full path to the file containing the input product
    input_file_path = None
    
    # full path where the output file should be placed
    output_file_path = None
    
    # http, ftp, scp, file, etc
    input_scheme = None    
    input_host = None
    input_port = None
    input_file_name = None
    input_user = None
    input_pw = None
    input_url = None
    
    # http, ftp, scp, file, etc
    output_scheme = None
    output_host = None    
    output_port = None
    output_file_name = None
    output_user = None
    output_pw = None
    output_url = None
  
    # landsat sceneid, modis tile name, aster granule id, etc.
    product_id = None
    
    # lt5, le7, mod, myd, etc
    sensor_code = None
    
    # tm, etm, terra, aqua, etc
    sensor_name = None
    
    year = None
    
    doy = None
    
    version = None
       
    def __init__(self, product_id):
        self.product_id = product_id
        self.sensor_code = product_id[0:3]
        self.sensor_name = settings.SENSOR_NAMES[self.sensor_code.upper()]
        
    # subclasses should override, construct and return string    
    def input_exists(self):
        raise NotImplementedError()
        
    # subclasses should override, construct and return string
    def output_exists(self):
        raise NotImplementedError()
    
    def get_input_product(self, target_directory):
        raise NotImplementedError()
        
         
class Modis(SensorProduct):
    version = None
    short_id = None
    horizontal = None
    vertical = None
    archive_date = None
    
    def __init__(self, product_id):
        super(Modis, self).__init__(product_id)
    
    
class ModisTerra(Modis):
    def __init__(self, product_id):
        super(ModisTerra, self).__init__(product_id)
        
            
class ModisAqua(Modis):
    def __init__(self, product_id):
        super(ModisAqua, self).__init__(product_id)
        
        
class ModisSR(Modis):
    def __init__(self, product_id):
        super(ModisSR, self).__init__(product_id)


class ModisNDVI(Modis):
    def __init__(self, product_id):
        super(ModisNDVI, self).__init__(product_id)


class ModisTerraSR(ModisTerra, ModisSR):
    def __init__(self, product_id):
        super(ModisTerraSR, self).__init__(product_id)


class ModisTerraNDVI(ModisTerra, ModisNDVI):
    def __init__(self, product_id):
        super(ModisTerraNDVI, self).__init__(product_id)


class ModisAquaSR(ModisAqua, ModisSR):
    def __init__(self, product_id):
        super(ModisAquaSR, self).__init__(product_id)

        
class ModisAquaNDVI(ModisAqua, ModisSR):
    def __init__(self, product_id):
        super(ModisAquaNDVI, self).__init__(product_id)
        
        
class Landsat(SensorProduct):
    path = None
    row = None
    station = None
    
    def __init__(self, product_id):

        product_id = product_id.strip()
        
        super(Landsat, self).__init__(product_id)

        self.base_input_path = os.path.join(settings.LANDSAT_BASE_SOURCE_PATH,
                                            self.sensor_name)
        self.path = util.strip_zeros(product_id[3:6])
        self.row = util.strip_zeros(product_id[6:9])
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
            self.product_id + '.tar.gz')
        

    
class LandsatTM(Landsat):
    def __init__(self, product_id):
        super(LandsatTM, self).__init__(product_id)
               
        
class LandsatETM(Landsat):
    def __init__(self, product_id):
        super(LandsatETM, self).__init__(product_id)
        

def instance(product_id):
    
    _id = product_id.upper().strip()
    
    instances = {
        'tm': ('LT[4|5]\d{3}\d{3}\d{4}\d{3}\w{3}.{2}', LandsatTM),
        'etm': ('LE7\d{3}\d{3}\d{4}\d{3}\w{3}.{2}', LandsatETM),
        'modsr': ('MOD.{3}', ModisTerraSR),
        'modndvi': ('MOD.{3}', ModisTerraNDVI),
        'mydsr': ('MOD.{3}', ModisAquaSR),
        'mydndvi': ('MOD.{3}', ModisAquaNDVI)
    }
            
    for key in instances.iterkeys():
        if re.match(instances[key][0], _id):
            return instances[key][1](_id)
            
    msg = "[%s] is not a supported sensor" % product_id
    raise NotImplementedError(msg)
    