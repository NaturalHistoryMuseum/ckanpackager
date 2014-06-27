"""Provides exception classes and matching Flask error handling"""
from flask import Blueprint, make_response
from ckanpackager.lib.utils import BadRequestError, NotAuthorizedError

errors = Blueprint('errors', __name__)


@errors.errorhandler(BadRequestError)
def handle_bad_request(err):
    return make_response(str(err), 400)


@errors.errorhandler(NotAuthorizedError)
def handle_not_authorized(err):
    return make_response(str(err), 401)
