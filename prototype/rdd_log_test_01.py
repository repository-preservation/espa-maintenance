#! /usr/bin/env python

import os
import sys
import logging
import threading

import rdd_logging
import rdd_log_test_02


def test_routine():

    # Get the processing logger
    logger = logging.getLogger('processing')

    # We want some debugging
    logger.setLevel(logging.INFO)

    logger.debug("In Method")
    logger.info("In Method")
    logger.warning("In Method")

    # Call a module routine
    rdd_log_test_02.test_02_routine_error()

    # Set back to info
    logger.setLevel(logging.WARNING)


if __name__ == '__main__':

    # Setup a basic logger until we define our own
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger()

    # Define our own logging
    try:
        rdd_logging.configure_log_handler(handler_name='processing',
                                           filename='processing.rdd')

        rdd_logging.configure_logger(logger_name='processing', level='DEBUG')

        rdd_logging.configure_log_handler(handler_name='web',
                                           filename='web.rdd')

        rdd_logging.configure_logger(logger_name='web', level='DEBUG')

        rdd_logging.configure_loggers()
    except Exception, e:
        logger.exception("Failed to configure logger objects")
        sys.exit(1)

    # Get and write to the processing logger
    logger = logging.getLogger('processing')
    logger.debug("In Main 1")
    logger.info("In Main 1")
    logger.warning("In Main 1")
    logger.error("In Main 1")
    logger.critical("In Main 1")

    try:
        # Call a local routine
        test_routine()
    except Exception, e:
        logger.exception("Error from an exception")
        sys.exit(1)

    # Write some more to the processing logger
    logger.debug("In Main 2")
    logger.info("In Main 2")
    logger.warning("In Main 2")
    logger.error("In Main 2")
    logger.critical("In Main 2")

    sys.exit(0)
