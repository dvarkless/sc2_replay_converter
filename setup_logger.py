import logging
import logging.handlers


def get_handler():
    """
        Return handler for a logger.
        Change this function to reconfigure logs in every class
        Returns:
            handler: logging.Formatter
    """
    # create rotating file handler with 3 files, each limited to 1 MB
    handler = logging.handlers.RotatingFileHandler(
        "log.log", maxBytes=1 * 1024 * 1024, backupCount=3
    )
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s in %(name)s: %(levelname)s MESSAGE:'%(message)s"
    )

    handler.setFormatter(formatter)

    return handler


def get_logger(name):
    """
        Returns the logger for the provided name.
        Usually called as `get_logger(__name__)`
        Args:
            name: str - name of the class
        Returns:
            logger: logging.Logger
    """
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)
        logger.addHandler(get_handler())
        logger.handler_set = True
    return logger
