from flask import Blueprint

routes = Blueprint('routes', __name__, url_prefix='/handler')

from . import api_routes
