import logging
import multiprocessing


class MultiprocessingLogHandler(logging.Handler):
    """A log handler that forwards messages to the multiprocessing logger.

    This is used because we cannot change's flask logger, so instead we set
    this handler.
    """
    @classmethod
    def setup(cls, handler=None, level=None, format_str=None, reset=True):
        """Convenience method to setup the multiprocessing logger with a handler and log level.

        **Note that this sets it globally**

        @param handler: Optional logging.Handler object
        @param log_level: Optional Log level for the multiprocessing logger & handler
        @param format_str: Optional format string for the logger
        @param reset: If true remove all other handlers (default)
        """
        logger = multiprocessing.get_logger()
        if reset:
            while len(logger.handlers) > 0:
                logger.removeHandler(logger.handlers[0])
        if level:
            logger.setLevel(level)
            if handler:
                handler.setLevel(level)
        if format_str:
            handler.setFormatter(logging.Formatter(format_str))
        if handler:
            logger.addHandler(handler)

    def emit(self, record):
        """Emit the given record to the multiprocessing logger"""
        logger = multiprocessing.get_logger()
        logger.handle(record)
