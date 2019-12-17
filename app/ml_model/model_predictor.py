import json
import logging
import pickle
import pandas as pd
from app.settings import settings
import requests


class ModelPredictor(object):

    def __init__(self):
        pass

    def predict(self, json_data, proba=False):
        endpoint = settings.PROBA_ENDPOINT if proba else settings.PREDICT_ENDPOINT
        port = settings.PORT
        if endpoint[0] != '/':
            endpoint = '/' + endpoint
        response = requests.post("localhost:" + str(port) + endpoint, json=json_data)
        return response
