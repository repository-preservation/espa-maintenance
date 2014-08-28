
import rdd_settings
import rdd_logging


def test_02_routine_error():

    logger1 = rdd_logging.EspaLogger.get_logger('espa.processing')
    logger2 = rdd_logging.EspaLogger.get_logger('espa.web')
    logger3 = rdd_logging.EspaLogger.get_logger('espa.cron.all')
    logger4 = rdd_logging.EspaLogger.get_logger('espa.cron.low')
    logger5 = rdd_logging.EspaLogger.get_logger('espa.cron.normal')
    #logger6 = rdd_logging.EspaLogger.get_logger('espa.cron.high')

    logger1.info("PPPRRROOOCCCEEESSSSSSIIINNNGGG")
    logger2.info("WWWEEEBBB")
    logger3.info("CCCRRROOONNN")
    logger4.info("CCCRRROOONNN-----LOW-----LOW")
    logger5.info("CCCRRROOONNN-----NORMAL-----NORMAL")
    #logger6.info("CCCRRROOONNN-----HIGH-----HIGH")

    try:
        open('/this/path/does/not/exist', 'r')
    except Exception, e:
        logger1.warning("Failed to open file")
        raise
