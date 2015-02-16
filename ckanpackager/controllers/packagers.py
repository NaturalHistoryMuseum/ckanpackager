from flask import request, Blueprint, current_app, g
from flask.json import jsonify
from ckanpackager import logic
from ckanpackager.tasks.datastore_package_task import DatastorePackageTask
from ckanpackager.tasks.dwc_archive_package_task import DwcArchivePackageTask
from ckanpackager.tasks.url_package_task import UrlPackageTask

packagers = Blueprint('packager', __name__)


@packagers.route('/package_datastore', methods=['POST'])
def package_datastore():
    logic.authorize_request(request.form)
    task = DatastorePackageTask(request.form, current_app.config)
    g.queue_task(task)
    return jsonify(
        status='success',
        message=current_app.config['SUCCESS_MESSAGE']
    )


@packagers.route('/package_dwc_archive', methods=['POST'])
def package_dwc_archive():
    logic.authorize_request(request.form)
    task = DwcArchivePackageTask(request.form, current_app.config)
    g.queue_task(task)
    return jsonify(
        status='success',
        message=current_app.config['SUCCESS_MESSAGE']
    )


@packagers.route('/package_url', methods=['POST'])
def package_url():
    logic.authorize_request(request.form)
    task = UrlPackageTask(request.form, current_app.config)
    g.queue_task(task)
    return jsonify(
        status='success',
        message=current_app.config['SUCCESS_MESSAGE']
    )