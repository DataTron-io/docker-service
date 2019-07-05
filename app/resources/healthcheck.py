from flask_restful import Resource
from flask import make_response
from app.settings import settings
import json


class Healthcheck(Resource):
    def get(self):
        service_info = dict(status={}, service_meta={})
        service_info['service_meta']['publisher_slug'] = settings.PUBLISHER_SLUG
        service_info['service_meta']['deployment_id'] = settings.DEPLOYMENT_ID

        status_code = 200
        status_msg = 'ServiceHealthcheckSuccess'

        service_info['status']['status_code'] = status_code
        service_info['status']['status_msg'] = status_msg

        return make_response(json.dumps(service_info), status_code)
