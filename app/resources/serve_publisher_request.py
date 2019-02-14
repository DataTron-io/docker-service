import json
import logging
from flask_restful import Resource
from flask import request, make_response
# from app.ml_model.publisher_model import PublisherModel
from app.ml_model import predictor
from app.monitoring import shiva_statsd
from app.settings import settings
from datatron.common.ml_parser.loader.constant import EXCEPTIONS_HTTP_STATUS_CODE


statsd_client = shiva_statsd.get_client()

# TODO: Refactor statsd code as decorator to remove redundancy


class ServePublisherRequest(Resource):
    def post(self):
        request_data = request.get_json()
        logging.info(
            'Received request data as: {} for publisher: {}'.format(str(request_data), predictor.publisher_slug))

        # TODO: Update getting meta with predict call to avoid inconsistency issue
        result = dict(prediction_meta={}, prediction={}, status={})

        try:
            input_features = request_data['data']
            prediction = predictor.predict(input_features)
            result['prediction'] = prediction
            logging.info('Successfully fetched the model prediction result')
            status_msg = "PublisherPredictionSuccess"
            status_code = 200
        except Exception as e:
            excp_type = e.__class__.__name__
            status_code = 500
            status_msg = "PublisherPredictionFailed"
            if excp_type in EXCEPTIONS_HTTP_STATUS_CODE:
                status_code = EXCEPTIONS_HTTP_STATUS_CODE[excp_type]
                status_msg = excp_type
            logging.info('{}: {}, while inferring the prediction for publisher: {}'
                         .format(status_msg, str(e), predictor.publisher_slug))
            logging.error(status_msg)
        finally:
            result['status']["status_code"] = status_code
            result['status']["status_msg"] = status_msg

        return make_response(json.dumps(result), status_code)
