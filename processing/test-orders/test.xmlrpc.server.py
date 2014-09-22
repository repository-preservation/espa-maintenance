#! /usr/bin/env python

import os
import sys
import signal
import logging
import SocketServer
from argparse import ArgumentParser
from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler


# imports from espa/espa_common
from espa_common import settings, utilities, sensor


class LPCS_XMLRPCServer(SimpleXMLRPCServer):
    '''
    Description:
      TODO TODO TODO
    '''

    done = False

    def register_signal(self, signum):
        signal.signal(signum, self.signal_handler)

    def signal_handler(self, signum, frame):
        print "Recieved signal", signum
        self.shutdown()

    def shutdown(self):
        self.done = True
        return 0

    def serve_forever(self):
        while not self.done:
            try:
                self.handle_request()
            except Exception, e:
                pass
# END - LPCS_XMLRPCServer


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RDD')


# ============================================================================
# Called from the crons
def get_configuration(config_item):

    logger = logging.getLogger(__name__)

    logger.info("%s was called" % __name__)
    if config_item == 'landsatds.username':
        return 'dev_username'
    elif config_item == 'landsatds.password':
        return 'dev_password'
    elif config_item == 'landsatds.host':
        return 'localhost'
    elif config_item == 'ondemand_enabled':
        return 'true'

    return ''


# ============================================================================
# Called from the crons
def get_scenes_to_process(limit, user, priority, product_types):

    logger = logging.getLogger(__name__)

    logger.info("%s was called" % __name__)

    # Grab environment variables
    env_vars = dict()
    env_vars = {'dev_data_dir': {'name': 'DEV_DATA_DIRECTORY',
                                 'value': None},
                'dev_cache_dir': {'name': 'DEV_CACHE_DIRECTORY',
                                  'value': None},
                'espa_work_dir': {'name': 'ESPA_WORK_DIR',
                                  'value': None}}

    missing_environment_variable = False
    for var in env_vars:
        env_vars[var]['value'] = os.environ.get(env_vars[var]['name'])

        if env_vars[var]['value'] is None:
            logger.error("Missing environment variable [%s]"
                         % env_vars[var]['name'])
            return []

    job_orders = list()

    order_requests = ['LPCS-UTM-2']

#    status = True
#    error_msg = ''

    for order_id in order_requests:
        products_file = '.'.join([order_id, 'products'])
        logger.info("Processing Products File [%s]" % products_file)

        with open(products_file, 'r') as scenes_fd:
            while (1):
                product_name = scenes_fd.readline().strip()

                if not product_name:
                    break
                if product_name.startswith('#'):
                    break
                logger.info("Processing Product Name [%s]" % product_name)

                request_file = '.'.join([order_id, 'json'])
                logger.info("Using Request File [%s]" % request_file)

                with open(request_file, 'r') as order_fd:
                    order_contents = order_fd.read()
                    if not order_contents:
                        logger.error("Order file [%s] is empty" % request_file)

                    tmp_line = order_contents

                    # Update the order for the developer
                    tmp = product_name[:3]
                    source_host = 'localhost'
                    is_modis = False
                    if tmp == 'MOD' or tmp == 'MYD':
                        is_modis = True
                        source_host = settings.MODIS_INPUT_HOSTNAME

                    if not is_modis:
                        product_path = ('%s/%s%s'
                                        % (env_vars['dev_data_dir']['value'],
                                           product_name, '.tar.gz'))

                        if not os.path.isfile(product_path):
                            logger.error("Missing product data (%s)"
                                         % product_path)
                            break

                        source_directory = env_vars['dev_data_dir']['value']

                    else:
                        if tmp == 'MOD':
                            base_source_path = settings.TERRA_BASE_SOURCE_PATH
                        else:
                            base_source_path = settings.AQUA_BASE_SOURCE_PATH

                        short_name = sensor.instance(product_name).short_name
                        version = sensor.instance(product_name).version
                        archive_date = utilities.date_from_doy(
                            sensor.instance(product_name).year,
                            sensor.instance(product_name).doy)
                        xxx = '%s.%s.%s' % (str(archive_date.year).zfill(4),
                                            str(archive_date.month).zfill(2),
                                            str(archive_date.day).zfill(2))

                        source_directory = ('%s/%s.%s/%s'
                                            % (base_source_path,
                                               short_name,
                                               version,
                                               xxx))

                    tmp_line = tmp_line.replace('\n', '')
                    tmp_line = tmp_line.replace("ORDER_ID", order_id)
                    tmp_line = tmp_line.replace("SCENE_ID", product_name)
                    tmp_line = tmp_line.replace("SRC_HOST", source_host)
                    tmp_line = \
                        tmp_line.replace("DEV_DATA_DIRECTORY",
                                         source_directory)
                    tmp_line = \
                        tmp_line.replace("DEV_CACHE_DIRECTORY",
                                         env_vars['dev_cache_dir']['value'])

                # END - with request_file

                job_orders.extend([tmp_line])

            # END - while (1)
        # END - with products_file
    # END - for order_id

    return job_orders


# ============================================================================
# Called from the crons
def queue_products(product_list, processing_location, job_name):

    logger = logging.getLogger(__name__)

    logger.info("%s was called" % __name__)
    return True


# ============================================================================
# Called from the mappers
def update_status(sceneid, orderid, module, status):

    logger = logging.getLogger(__name__)

    logger.info("%s was called" % __name__)
    with open("xmlrpc_server.log", "a") as fd:
        fd.write("Scene [%s] Order [%s] Received [%s] From [%s]"
                 % (sceneid, orderid, status, module))
    return True


# ============================================================================
# Called from the mappers
def set_scene_error(product_type,
                    orderid,
                    processing_location,
                    logged_contents):

    logger = logging.getLogger(__name__)

    logger.info("%s was called" % __name__)
    with open("xmlrpc_server.log", "a") as fd:
        fd.write("Product Type [%s] Order ID [%s]"
                 " Processing Location [%s] Logged Contents [%s]"
                 % (product_type, orderid, processing_location,
                    logged_contents))
    return True


# ============================================================================
def build_argument_parser():
    '''
    Description:
      Build the command line argument parser
    '''

    # Create a command line argument parser
    description = "A test XMLRPC server"
    parser = ArgumentParser(description=description)

    parser.add_argument('--hostname',
                        action='store', dest='hostname', default='localhost',
                        help="specify the hostname")

    parser.add_argument('--port',
                        action='store', dest='port', default=55801,
                        help="specify the port to listen on")

    return parser
# END - build_argument_parser


# ============================================================================
if __name__ == '__main__':

    logging.basicConfig(format=('%(asctime)s.%(msecs)03d %(process)d'
                                ' %(levelname)-8s'
                                ' %(filename)s:%(lineno)d:%(funcName)s'
                                ' -- %(message)s'),
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO,
                        filename="/tmp/test.xmlrpc.server.log",
                        filemode='a')

    logger = logging.getLogger(__name__)

    # Build the command line argument parser
    parser = build_argument_parser()

    # Parse the command line arguments
    args = parser.parse_args()

    logger.info("Starting LPCS_XMLRPCServer on PID [%d]" % os.getpid())

    server = LPCS_XMLRPCServer((args.hostname, int(args.port)),
                               requestHandler=RequestHandler)

    # Add this in to allow shutdown from the service
    # server.register_function(server.shutdown)

    server.register_function(get_configuration, 'get_configuration')
    server.register_function(get_scenes_to_process, 'get_scenes_to_process')
    server.register_function(queue_products, 'queue_products')
    server.register_function(update_status, 'update_status')
    server.register_function(set_scene_error, 'set_scene_error')

    server.register_introspection_functions()

    server.register_signal(signal.SIGHUP)
    server.register_signal(signal.SIGINT)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Stopped LPCS_XMLRPCServer")
