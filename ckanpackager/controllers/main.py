import os
import glob
from flask import request, Blueprint, current_app, g
from flask.json import jsonify
from ckanpackager import logic
from ckanpackager.tasks.datastore_package_task import DatastorePackageTask
from ckanpackager.tasks.dwc_archive_package_task import DwcArchivePackageTask
from ckanpackager.tasks.url_package_task import UrlPackageTask

main = Blueprint('main', __name__)


# Status page
@main.route('/')
@main.route('/index')
@main.route('/index.html', methods=['POST'])
def status():
    logic.authorize_request(request.form)
    return jsonify(
        worker_count=current_app.config['WORKERS'],
        queue_length=g.queue.length(),
        processed_requests=g.queue.processed()
    )

# Clear caches
@main.route('/clear_caches', methods=['POST'])
def clear_caches():
    logic.authorize_request(request.form)
    matching_files = os.path.join(
        current_app.config['STORE_DIRECTORY'],
        '*.zip'
    )
    for file in glob.glob(matching_files):
        os.remove(file)
    return jsonify(
        status='success',
        message='Done.'
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


# Package dwc archive task
@main.route('/package_dwc_archive', methods=['POST'])
def package_dwc_archive():
    logic.authorize_request(request.form)
    task = DwcArchivePackageTask(request.form, current_app.config)
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
