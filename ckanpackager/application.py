"""Ckanpackager application
"""
import sys
import logging

from flask import Flask, g
from ckanpackager.lib.queue import TaskQueue
from ckanpackager.lib.multiprocessing_log_handler import MultiprocessingLogHandler
from ckanpackager.controllers.errors import errors
from ckanpackager.controllers.main import main

# Create the application
app = Flask(__name__)

# Setup logging. We want to replace all handlers with our own multiprocessing aware handler.
handler = logging.StreamHandler(sys.stderr)
MultiprocessingLogHandler.setup(
    handler=handler,
    level=logging.INFO,
    format_str='%(asctime)s %(levelname)s: %(message)s'
)
while len(app.logger.handlers) > 0:
    app.logger.removeHandler(app.logger.handlers[0])
app.logger.setLevel(logging.INFO)
app.logger.addHandler(MultiprocessingLogHandler())

# Read configuration
app.config.from_object('ckanpackager.config_defaults')
app.config.from_envvar('CKANPACKAGER_CONFIG')

# Setup our worker queue, and ensure it's available to requests. Note that requests are served by a single thread
# on a single process - so sharing the object is not a problem. The packaging itself happens in a separate
# process, so the request is very fast.
queue = TaskQueue(app.config['WORKERS'], app.config['REQUESTS_PER_WORKER'])
@app.before_request
def before_request():
    g.queue = queue


# Register our blueprints
app.register_blueprint(errors)
app.register_blueprint(main)

# Start server (debug mode)
if __name__ == '__main__':
    app.run(
        host=app.config['HOST'],
        port=app.config['PORT'],
        threaded=False,
        processes=1,
        debug=True,
        use_reloader=False
    )
