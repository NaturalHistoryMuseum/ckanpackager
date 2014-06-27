"""Provides snippets of application logic such as authorization functions"""
from flask import current_app
from ckanpackager.lib.utils import NotAuthorizedError


def authorize_request(params=None):
    """Checks whether the request is authorized

    Raises NotAuthorized if the request is not authorized, and does nothing otherwise.

    @param params: A dictionary representing the current request
    """
    if not params or 'secret' not in params or params['secret'] != current_app.config['SECRET']:
        raise NotAuthorizedError('Wrong/missing secret')