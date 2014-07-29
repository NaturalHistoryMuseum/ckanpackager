from flask import request, Blueprint, current_app, g
from flask.json import jsonify
from ckanpackager import logic
from ckanpackager.tasks.datastore_package_task import DatastorePackageTask
from ckanpackager.tasks.url_package_task import UrlPackageTask

main = Blueprint('main', __name__)


# Status page
@main.route('/')
@main.route('/index')
@main.route('/index.html')
def status():
    return jsonify(
        worker_count=current_app.config['WORKERS'],
        queue_length=g.queue.length(),
        processed_requests=g.queue.processed()
    )


# Package datastore task
@main.route('/package_datastore', methods=['POST'])
def package_datastore():
    logic.authorize_request(request.form)
    task = DatastorePackageTask(request.form, current_app.config)
    g.queue.add(task)
    return jsonify(
        status='success',
        message=current_app.config['SUCCESS_MESSAGE']
    )


# Package url task
@main.route('/package_url', methods=['POST'])
def package_url():
    logic.authorize_request(request.form)
    task = UrlPackageTask(request.form, current_app.config)
    g.queue.add(task)
    return jsonify(
        status='success',
        message=current_app.config['SUCCESS_MESSAGE']
    )
