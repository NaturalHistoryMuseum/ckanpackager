from flask import request, Blueprint, current_app
from flask.json import jsonify

from ckanpackager import logic
from ckanpackager.lib.statistics import statistics
from ckanpackager.lib.utils import BadRequestError

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

    # create a stats object for database access
    stats = statistics(current_app.config['STATS_DB'], current_app.config.get(u'ANONYMIZE_EMAILS'))

    if stype is None:
        conditions = {}
        if 'resource_id' in request.form:
            conditions['resource_id'] = request.form.get('resource_id')
        return jsonify(
            status=True,
            totals=stats.get_totals(**conditions)
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
                requests=stats.get_requests(start, count, **conditions)
            )
        else:
            return jsonify(
                success=True,
                errors=stats.get_errors(start, count, **conditions)
            )
    else:
        raise BadRequestError('Unknown statistics request {}'.format(stype))
