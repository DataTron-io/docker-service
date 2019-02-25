from flask_restful import Resource
from flask import make_response
from app.settings import settings
from app.ml_model import predictor
from datatron.common.ml_parser.exceptions import ModelBinaryLoadError
from datatron.common.ml_parser.loader.constant import EXCEPTIONS_HTTP_STATUS_CODE

import json
import logging

class PublisherHealthcheck(Resource):
    def get(self):
        service_info = dict(status={}, service_meta={})
        service_info['service_meta']['publisher_slug'] = predictor.publisher_slug
        service_info['service_meta']['deployment_id'] = predictor.deploy_id

        try:
            status_code = 200
            status_msg = 'ServiceHealthcheckSuccess'
        except ModelBinaryLoadError as e:
            logging.info('Failed model load on healthcheck as : {}'.format(str(e)))
            status_code = EXCEPTIONS_HTTP_STATUS_CODE['ModelBinaryLoadError']
            status_msg = "ModelBinaryLoadError"
        finally:
            service_info['status']['status_code'] = status_code
            service_info['status']['status_msg'] = status_msg

        return make_response(json.dumps(service_info), status_code)
