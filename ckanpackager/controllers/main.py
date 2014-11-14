import os
import glob
from flask import request, Blueprint, current_app, g
from flask.json import jsonify
from ckanpackager import logic
from ckanpackager.lib.utils import BadRequestError, NotAuthorizedError
from ckanpackager.lib.statistics import statistics
from ckanpackager.tasks.datastore_package_task import DatastorePackageTask
from ckanpackager.tasks.dwc_archive_package_task import DwcArchivePackageTask
from ckanpackager.tasks.url_package_task import UrlPackageTask

main = Blueprint('main', __name__)


# Status page
@main.route('/', methods=['POST'])
@main.route('/status', methods=['POST'])
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

# Get statistics
@main.route('/statistics', methods=['POST'])
@main.route('/statistics/<stype>', methods=['POST'])
def application_statistics(stype=None):
    logic.authorize_request(request.form)
    if stype is None:
        return jsonify(
            status='success',
            totals=statistics(current_app.config['STATS_DB']).get_totals()
        )
    elif stype in ['requests', 'errors']:
        start = request.form.get('offset', 0)
        count = request.form.get('limit', 100)
        conditions = {}
        if 'resource_id' in request.form:
            conditions['resource_id'] = request.form.get('resource_id')
        if 'email' in request.form:
            conditions['email'] = request.form.get('email')
        if stype == 'requests':
            return jsonify(
                status='success',
                requests=statistics(current_app.config['STATS_DB']).get_requests(
                    start, count, **conditions
                )
            )
        else:
            return jsonify(
                status='success',
                errors=statistics(current_app.config['STATS_DB']).get_errors(
                    start, count, **conditions
                )
            )
    else:
        raise BadRequestError('Unknown statistics request {}'.format(stype))


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


# Handle BadRequestError
@main.errorhandler(BadRequestError)
def handle_bad_request(err):
    response = jsonify({
        'status': 'failed',
        'error': 'BadRequestError',
        'message': str(err)
    })
    response.status_code = 400
    return response


# Handle NotAuthorizedError
@main.errorhandler(NotAuthorizedError)
def handle_not_authorized(err):
    response = jsonify({
        'status': 'failed',
        'error': 'NotAuthorizedError',
        'message': str(err)
    })
    response.status_code = 401
    return response
