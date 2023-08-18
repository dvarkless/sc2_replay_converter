import logging
import logging.handlers


def get_handler():
    # create rotating file handler with 3 files, each limited to 1 MB
    handler = logging.handlers.RotatingFileHandler(
        "log.log", maxBytes=1 * 1024 * 1024, backupCount=3
    )
    handler.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter(
        "%(asctime)s in %(name)s: %(levelname)s MESSAGE:'%(message)s"
    )

    # add formatter to handler
    handler.setFormatter(formatter)

    return handler


def get_logger(name):
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)
        logger.addHandler(get_handler())
        logger.handler_set = True
    return logger
