from espa_common import settings
from espa_common import sensor
from espa_common import utilities
import requests
import os


class LPDAACService(object):

    def __init__(self):
        self.host = settings.MODIS_INPUT_CHECK_HOST
        self.port = settings.MODIS_INPUT_CHECK_PORT

    def verify_products(self, products):
        response = {}

        if isinstance(products, str):
            products = [products]

        for product in products:

            if isinstance(product, str):
                product = sensor.instance(product)

            response[product.product_id] = self.input_exists(product)

        return response

    def input_exists(self, product):
        '''Determines if a LPDAAC product is available for download

        Keyword args:
        product - The name of the product

        Returns:
        True/False
        '''
        result = False

        try:
            url = self.get_download_url(product)
            if url:
                response = None

                try:
                    response = requests.head(url)
                    if response.ok:
                        result = True
                except Exception, e:
                    print ("Exception checking inputs:%s" % e)
                    return False
                finally:
                    if response is not None:
                        response.close()
                        response = None

        except sensor.ProductNotImplemented:
            pass

        return result

    def get_download_urls(self, products):

        urls = {}

        #be nice and accept a string
        if isinstance(products, str):
            products = sensor.instance(products)

        #also be nice and accept a sensor.Modis object
        if isinstance(products, sensor.Modis):
            products = [products]

        for product in products:
            path = self._build_modis_input_file_path(product)
            url = ''.join([self.host, ":", str(self.port), path])

            if not url.lower().startswith("http"):
                url = ''.join(['http://', url])

            urls[product.product_id] = url

        return urls

    def _build_modis_input_file_path(self, product):

        if isinstance(product, str):
            product = sensor.instance(product)

        if isinstance(product, sensor.Aqua):
            base_path = settings.AQUA_BASE_SOURCE_PATH
        elif isinstance(product, sensor.Terra):
            base_path = settings.TERRA_BASE_SOURCE_PATH
        else:
            msg = "Cant build input file path for unknown LPDAAC product:%s"
            raise Exception(msg % product.product_id)

        date = utilities.date_from_doy(product.year, product.doy)

        path_date = "%s.%s.%s" % (date.year,
                                  str(date.month).zfill(2),
                                  str(date.day).zfill(2))

        input_file_extension = settings.MODIS_INPUT_FILENAME_EXTENSION

        input_file_name = "%s.%s" % (product.product_id, input_file_extension)

        path = os.path.join(base_path,
                            '.'.join([product.short_name.upper(),
                                      product.version.upper()]),
                            path_date.upper(), input_file_name)

        return path


def input_exists(product):
    return LPDAACService().input_exists(product)


def verify_products(products):
    return LPDAACService().verify_products(products)


def get_download_urls(products):
    return LPDAACService().get_download_urls(products)
