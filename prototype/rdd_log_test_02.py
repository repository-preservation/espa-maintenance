
import logging
import threading


def test_02_routine_error():
    logger1 = logging.getLogger('processing')
    logger2 = logging.getLogger('web')

    logger1.info("Incredible")
    logger2.info("Wow")

    try:
        open('/this/path/does/not/exist', 'r')
    except Exception, e:
        logger1.error("Failed to open file", exc_info=True)
        raise
