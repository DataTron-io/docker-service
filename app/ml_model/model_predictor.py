import json
import logging
import pickle
import pandas as pd
from app.settings import settings
import requests
from datatron.common.discovery import DatatronDiscovery


class ModelPredictor(object):

    def __init__(self):
        pass

    def _get_service_discovery_client(self):
        dsd_discovery_client = DatatronDiscovery(discovery_type=settings.DISCOVERY_TYPE,
                                                 services_type='infrastructure',
                                                 hosts=settings.SHIVA_ZOOKEEPER_HOSTS,
                                                 caching=False)
        return dsd_discovery_client

    def predict(self, json_data, proba=False):
        dsd_client = self._get_service_discovery_client()
        dictator_url = dsd_client.get_single_instance(service_path='dictator', pick_random=True)
        full_url = dictator_url + '/api/publishers/deployments/{}'.format(settings.DEPLOYMENT_ID)
        deployment_response = requests.get(url=full_url)
        dsd_client.stop()
        deploy_data = deployment_response.json()
        features = deploy_data['result']['model']['features']

        validated_features = {}
        for feature in features:
            if feature in json_data:
                validated_features[feature] = json_data[feature]

        logging.info('Validated Features: {}'.format(validated_features))

        endpoint = settings.PROBA_ENDPOINT if proba else settings.PREDICT_ENDPOINT
        port = settings.APIPORT
        if endpoint[0] != '/':
            endpoint = '/' + endpoint
        response = requests.post("http://localhost:" + port + endpoint, json=validated_features)
        return response

