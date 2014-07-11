from flask import request, Blueprint, current_app, g
from flask.json import jsonify
from ckanpackager import logic
from ckanpackager.lib.package_task import PackageTask

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


# Job page
@main.route('/package', methods=['POST'])
def package():
    logic.authorize_request(request.form)
    task = PackageTask(request.form, current_app.config)
    g.queue.add(task)
    return jsonify(
        status='success',
        message=current_app.config['SUCCESS_MESSAGE']
    )
