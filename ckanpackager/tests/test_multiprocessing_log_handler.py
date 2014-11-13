import logging
import multiprocessing

from nose.tools import assert_equals, assert_greater, assert_is
from ckanpackager.lib.multiprocessing_log_handler import MultiprocessingLogHandler


class DummyHandler(logging.Handler):
    """Use this to test that the logging handler is called as expected"""
    def __init__(self):
        logging.Handler.__init__(self)
        self.records = []

    def emit(self, record):
        print record.getMessage()
        self.records.append((record.levelname, record.msg))


class TestMultiprocessingLogHandler:

    def test_setup_reset(self):
        """Ensures that setup resets existing handlers"""
        logger = multiprocessing.get_logger()
        logger.addHandler(logging.NullHandler())
        assert_greater(len(logger.handlers), 0)
        MultiprocessingLogHandler.setup()
        assert_equals(len(logger.handlers), 0)

    def test_setup_no_reset(self):
        """Ensures that setup doesn't reset handlers if specified"""
        logger = multiprocessing.get_logger()
        logger.addHandler(logging.NullHandler())
        assert_greater(len(logger.handlers), 0)
        MultiprocessingLogHandler.setup(reset=False)
        assert_greater(len(logger.handlers), 0)

    def test_setup_handler(self):
        """Ensures that setup adds the given handler"""
        logger = multiprocessing.get_logger()
        handler = logging.StreamHandler()
        MultiprocessingLogHandler.setup(handler=handler)
        assert_equals(len(logger.handlers), 1)
        assert_is(logger.handlers[0], handler)

    def test_setup_format_str(self):
        """Ensures that setup sets the format string"""
        logger = multiprocessing.get_logger()
        handler = logging.StreamHandler()
        MultiprocessingLogHandler.setup(handler=handler, level=logging.ERROR, format_str="hello world")
        record = logging.LogRecord('', logging.ERROR, '', '', '', '', '')
        assert_equals(handler.formatter.format(record), "hello world")

    def test_setup_level(self):
        """Ensures that setup sets the level properly on logger and handler"""
        logger = multiprocessing.get_logger()
        handler = logging.StreamHandler()
        logger.setLevel(logging.INFO)
        handler.setLevel(logging.INFO)
        assert_equals(logger.level, logging.INFO)
        assert_equals(handler.level, logging.INFO)
        MultiprocessingLogHandler.setup(level=logging.DEBUG, handler=handler)
        assert_equals(logger.level, logging.DEBUG)
        assert_equals(handler.level, logging.DEBUG)

    def test_log_passthrough(self):
        """Ensures that the logger passes messages to the given handler"""
        logger = multiprocessing.get_logger()
        handler = DummyHandler()
        MultiprocessingLogHandler.setup(level=logging.DEBUG, handler=handler)
        logger.error('test error')
        assert_equals(handler.records, [('ERROR', 'test error')])

    def test_log_passthrough_levels(self):
        """Ensures that the logger passes the right messages to the given handler"""
        logger = multiprocessing.get_logger()
        handler = DummyHandler()
        MultiprocessingLogHandler.setup(level=logging.INFO, handler=handler)
        logger.debug('test debug')
        logger.info('test info')
        logger.error('test error')
        assert_equals(handler.records, [('INFO', 'test info'), ('ERROR', 'test error')])

