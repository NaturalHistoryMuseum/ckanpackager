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
    g.queue.add(PackageTask(request.form))

@main.route('/package_test', methods=['GET'])
def package_test():
    logic.authorize_request(request.args)
    g.queue.add(PackageTask(request.args))
    return jsonify(
        status='success'
    )