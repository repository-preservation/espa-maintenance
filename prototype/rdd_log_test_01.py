#! /usr/bin/env python

import os
import sys
import logging

import rdd_settings
import rdd_logging
import rdd_log_test_02


def test_routine():

    # Get the processing logger
    logger = (rdd_logging.EspaLogger.
              getLogger(rdd_settings.LOGGER_ALIAS['PROCESSING']))

    # We want some debugging
    logger.setLevel(logging.WARNING)

    logger.debug("In Method")
    logger.info("In Method")
    logger.warning("In Method")

    # Set back to info
    logger.setLevel(logging.INFO)

    # Call a module routine
    rdd_log_test_02.test_02_routine_error()


if __name__ == '__main__':

    # Configure logging for this application
    logger = rdd_logging.EspaLogger.configure('order', 'scene')

    # Get and write to the processing logger
    logger = (rdd_logging.EspaLogger.
              getLogger(rdd_settings.LOGGER_ALIAS['PROCESSING']))
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
