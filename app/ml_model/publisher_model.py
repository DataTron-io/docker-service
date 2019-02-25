import json
import logging
import requests
from app.settings import settings
import datatron.common.transfer as dt
from datatron.common.ml_parser import get_model
from datatron.common.ml_parser.exceptions import ModelBinaryLoadError
from datatron.common.discovery import DatatronDiscovery


class PublisherModelPredictor(object):
    def __init__(self):
        self.publisher_slug = settings.PUBLISHER_SLUG
        self.workspace_slug = settings.WORKSPACE_SLUG
        self.deploy_id = settings.DEPLOYMENT_ID

        return

    def predict(self, input_features):
        #Prediction code here

        return 0.5


