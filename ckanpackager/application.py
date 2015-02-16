"""Ckanpackager application
"""
import os
import sys
import logging

from flask import Flask, g
from ckanpackager.lib.queue import TaskQueue
from ckanpackager.lib.multiprocessing_log_handler import MultiprocessingLogHandler
from ckanpackager.controllers.status import status
from ckanpackager.controllers.actions import actions
from ckanpackager.controllers.packagers import packagers
from ckanpackager.controllers.error_handlers import error_handlers

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

# Create folders if required
if not os.path.exists(app.config['TEMP_DIRECTORY']):
    os.makedirs(app.config['TEMP_DIRECTORY'])
if not os.path.exists(app.config['STORE_DIRECTORY']):
    os.makedirs(app.config['STORE_DIRECTORY'])

# Setup our worker queue, and ensure it's available to requests. Note that requests are served by a single thread
# on a single process - so sharing the object is not a problem. The packaging itself happens in a separate
# process, so the request is very fast.
queue = TaskQueue(app.config['WORKERS'], app.config['REQUESTS_PER_WORKER'])
@app.before_request
def before_request():
    g.queue_task = queue.add


# Register our blueprints
app.register_blueprint(status)
app.register_blueprint(actions)
app.register_blueprint(packagers)
app.register_blueprint(error_handlers)

def run():
    """ Start the server """
    global app
    app.run(
        host=app.config['HOST'],
        port=app.config['PORT'],
        threaded=False,
        processes=1,
        debug=True,
        use_reloader=False
    )

# Start server (debug mode)
if __name__ == '__main__':
    run()
