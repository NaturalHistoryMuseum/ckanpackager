import os
import glob
from flask import request, Blueprint, current_app
from flask.json import jsonify
from ckanpackager import logic

actions = Blueprint('actions', __name__)


@actions.route('/clear_caches', methods=['POST'])
def clear_caches():
    logic.authorize_request(request.form)
    matching_files = os.path.join(
        current_app.config['STORE_DIRECTORY'],
        '*.zip'
    )
    for file_name in glob.glob(matching_files):
        os.remove(file_name)
    return jsonify(
        status='success',
        message='Done.'
    )