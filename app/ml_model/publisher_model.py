import json
import logging
import requests
from app.settings import settings
import datatron.common.transfer as dt
from datatron.common.ml_parser import get_model
from datatron.common.ml_parser.exceptions import ModelBinaryLoadError
from datatron.common.discovery import DatatronDiscovery
from keras.models import model_from_json

class PublisherModelPredictor(object):
    def __init__(self):
        self.publisher_slug = settings.PUBLISHER_SLUG
        self.workspace_slug = settings.WORKSPACE_SLUG
        self.deploy_id = settings.DEPLOYMENT_ID

        return

    def predict(self, input_features):
        # Model reconstruction from JSON file
        with open('model.json', 'r') as f:
            model = model_from_json(f.read())

        # Load weights into the new model
        model.load_weights('model.h5')
        return model.predict(input_features)


