
import rdd_settings
import rdd_logging


def test_02_routine_error():
    logger1 = (rdd_logging.EspaLogger.
               getLogger(rdd_settings.LOGGER_ALIAS['PROCESSING']))
    logger2 = (rdd_logging.EspaLogger.
               getLogger(rdd_settings.LOGGER_ALIAS['WEB']))
    logger3 = (rdd_logging.EspaLogger.
               getLogger(rdd_settings.LOGGER_ALIAS['CRON']))

    logger1.info("PPPRRROOOCCCEEESSSSSSIIINNNGGG")
    logger2.info("WWWEEEBBB")
    logger3.info("CCCRRROOONNN")

    try:
        open('/this/path/does/not/exist', 'r')
    except Exception, e:
        logger1.warning("Failed to open file")
        raise
