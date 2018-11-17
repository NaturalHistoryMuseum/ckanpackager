"""Ckanpackager service

Run a web service to access statistics, perform actions and queue tasks
"""
import os

from flask import Flask, g
from ckanpackager.controllers.status import status
from ckanpackager.controllers.actions import actions
from ckanpackager.controllers.packagers import packagers
from ckanpackager.controllers.error_handlers import error_handlers

# Create the application
app = Flask(__name__)

# Read configuration
app.config.from_object('ckanpackager.config_defaults')
app.config.from_envvar('CKANPACKAGER_CONFIG')

# Create folders if required
if not os.path.exists(app.config['TEMP_DIRECTORY']):
    os.makedirs(app.config['TEMP_DIRECTORY'])
if not os.path.exists(app.config['STORE_DIRECTORY']):
    os.makedirs(app.config['STORE_DIRECTORY'])

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
        port=int(app.config['PORT']),
        threaded=False,
        processes=1,
        debug=True,
        use_reloader=False
    )

# Start server (debug mode)
if __name__ == '__main__':
    run()
