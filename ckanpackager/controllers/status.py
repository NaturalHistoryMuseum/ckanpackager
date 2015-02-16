from flask import request, Blueprint, current_app, g
from flask.json import jsonify
from ckanpackager import logic
from ckanpackager.lib.utils import BadRequestError
from ckanpackager.lib.statistics import statistics

status = Blueprint('status', __name__)


@status.route('/', methods=['POST'])
@status.route('/status', methods=['POST'])
def ckanpackager_status():
    logic.authorize_request(request.form)
    return jsonify(
        worker_count=current_app.config['WORKERS']
    )


@status.route('/statistics', methods=['POST'])
@status.route('/statistics/<stype>', methods=['POST'])
def application_statistics(stype=None):
    logic.authorize_request(request.form)
    if stype is None:
        conditions = {}
        if 'resource_id' in request.form:
            conditions['resource_id'] = request.form.get('resource_id')
        return jsonify(
            status=True,
            totals=statistics(current_app.config['STATS_DB']).get_totals(
                **conditions
            )
        )
    elif stype in ['requests', 'errors']:
        start = int(request.form.get('offset', 0))
        count = int(request.form.get('limit', 100))
        conditions = {}
        if 'resource_id' in request.form:
            conditions['resource_id'] = request.form.get('resource_id')
        if 'email' in request.form:
            conditions['email'] = request.form.get('email')
        if stype == 'requests':
            return jsonify(
                success=True,
                requests=statistics(current_app.config['STATS_DB']).get_requests(
                    start, count, **conditions
                )
            )
        else:
            return jsonify(
                success=True,
                errors=statistics(current_app.config['STATS_DB']).get_errors(
                    start, count, **conditions
                )
            )
    else:
        raise BadRequestError('Unknown statistics request {}'.format(stype))
