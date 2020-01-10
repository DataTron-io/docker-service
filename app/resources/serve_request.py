import json
import logging
import numpy as np
from flask_restful import Resource
from flask import request, make_response
from app.settings import settings
from app.ml_model import predictor


class ServePredictRequest(Resource):
    def post(self):
        request_data = request.get_json()
        logging.info(
            'Received request data as: {} for publisher: {}'.format(str(request_data), settings.PUBLISHER_SLUG))

        result = dict(prediction_meta={}, prediction={}, status={})

        try:
            result['prediction_meta'] = {
                'model_type': settings.MODEL_TYPE,
                'model_learn_type': settings.LEARN_TYPE,
                'model_version': settings.MODEL_VERSION,
                'model_name': settings.MODEL_NAME,
                'model_slug': settings.MODEL_SLUG,
                'publisher_slug': settings.PUBLISHER_SLUG,
                'model_version_slug': settings.MODEL_VERSION_SLUG
            }

            x_dict = request_data['data']
            feature_list = predictor.feature_list()
            x = np.array([[x_dict[feature_name] for feature_name in feature_list]])
            y = predictor.predict(x)
            AuxResults=y.iloc[0].to_dict()
            for item in AuxResults:
                AuxResults[item]=str(AuxResults[item])
            result['prediction_meta'].update( AuxResults )
            result['prediction'] = {'outputs': int(y["MODEL_CLASS"].values[0])}

            logging.info('Successfully fetched the model prediction result')
            status_msg = "PublisherPredictionSuccess"
            status_code = 200
        except Exception as e:
            status_code = 500
            status_msg = "PublisherPredictionFailed"
            logging.info('{}: {}, while inferring the prediction for publisher: {}'
                         .format(status_msg, str(e), settings.PUBLISHER_SLUG))
            logging.error(status_msg)
        finally:
            result['status']["status_code"] = status_code
            result['status']["status_msg"] = status_msg

        return make_response(json.dumps(result), status_code)


class ServePredictProbaRequest(Resource):
    def post(self):
        request_data = request.get_json()
        logging.info('Received request for to get Predict proba for publisher: {}'
                     .format(settings.PUBLISHER_SLUG))

        result = dict(predict_proba={}, status={})

        try:
            x_dict = request_data['data']
            feature_list = predictor.feature_list()
            x = np.array([[x_dict[feature_name] for feature_name in feature_list]])
            result['predict_proba'] = predictor.predict_proba(x)

            logging.info('Successfully fetched the model predict_proba result')
            status_msg = "PublisherPredictionProbaSuccess"
            status_code = 200
        except Exception as e:
            status_code = 500
            status_msg = "PublisherPredictionProbaFailed"
            logging.info('{}: {}, while inferring the predict_proba for publisher: {}'
                         .format(status_msg, str(e), settings.PUBLISHER_SLUG))
            logging.error(status_msg)
        finally:
            result['status']["status_code"] = status_code
            result['status']["status_msg"] = status_msg

        return make_response(json.dumps(result), status_code)