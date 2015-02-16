from flask import Blueprint
from flask.json import jsonify
from ckanpackager.lib.utils import BadRequestError, NotAuthorizedError

error_handlers = Blueprint('error_handlers', __name__)


@error_handlers.app_errorhandler(BadRequestError)
def handle_bad_request(err):
    response = jsonify({
        'status': 'failed',
        'error': 'BadRequestError',
        'message': str(err)
    })
    response.status_code = 400
    return response


@error_handlers.app_errorhandler(NotAuthorizedError)
def handle_not_authorized(err):
    print "CARROT CAKE"
    response = jsonify({
        'status': 'failed',
        'error': 'NotAuthorizedError',
        'message': str(err)
    })
    response.status_code = 401
    return response