import json
import logging
import pickle

import os
os.environ["DRIVERLESS_AI_LICENSE_KEY"] = """czZvHO12DaielA8Bbv0KzZaYoeteUNaWxqHhzKGI67Uy7GzUpWf5ymGaBCqSrKq3SIfocIeeYjp7HHmW6D8GrQo6wCq332C0maCvQf269Qqodr_racx7ttX9S23v3IeHrIe1ZT8jGupEfEH5vOl7oJptACTU_ebTltVV7ywbUI3CDixWS2nrDmz1hckGX5QMxG8CmdqLCEj8HsMnkOYBXLki1gK2PDnzXtm7Wp060YIcRZfljEKtFEGyHkF7ANTtgaV2msJo5i8jURO7VerOovmZB5w-0N9dVUx3clWqnb2_Q-2d9O1bFAHGQ-VzHjsAF152u5MUXMwyOS7MeQph0mxpY2Vuc2VfdmVyc2lvbjoxCnNlcmlhbF9udW1iZXI6NDMxMDUKbGljZW5zZWVfb3JnYW5pemF0aW9uOlppcHdpcmUKbGljZW5zZWVfZW1haWw6eXdiMTkxNTRAdW5pLnN0cmF0aC5hYy51awpsaWNlbnNlZV91c2VyX2lkOjQzMTA1CmlzX2gyb19pbnRlcm5hbF91c2U6ZmFsc2UKY3JlYXRlZF9ieV9lbWFpbDprYXlAaDJvLmFpCmNyZWF0aW9uX2RhdGU6MjAyMC8wMi8yNgpwcm9kdWN0OkRyaXZlcmxlc3NBSQpsaWNlbnNlX3R5cGU6dHJpYWwKZXhwaXJhdGlvbl9kYXRlOjIwMjAvMDMvMTgK"""
import pandas as pd
import numpy as np
from numpy import nan
from scipy.special._ufuncs import expit
from scoring_h2oai_experiment_47323476_e6fa_11e9_afcc_0242ac110002 import Scorer

class ModelPredictor(object):
    """
    This class is modified by the user to upload the model into the Datatron platform.
    """
    def __init__(self):
        pass

    def predict(self, x):
        """
        Required for online and offline predictions

        :param: x : A list or list of list of input vector
        :return: single prediction

        Example (Copy and paste the 3 lines to test it out):
        Step 1. Load the model into python. Model artifacts are stored in "models" folder
        model = pickle.load(open("models/xgboost_birth_model.pkl", "rb"))

        Step 2. Prepare the data to be predicted. This needs to be modified based on how the data was sent in the
        request
        x= pd.DataFrame(x, columns = self.feature_list())

        Step 3. Use the uploaded model to predict/ infer from the input data
        return model.predict(x)

        Note: Make sure all the needed packages are mentioned in requirements.txt
        """
        print ("x1",x)
        print ("x2",x[0])
        scorer = Scorer()

        #y = scorer.score(['5','9',  '2010-02-05',  '1.0' ]);
        y = scorer.score(x[0]);
        return (y)

        pass

    def predict_proba(self, x):
        """
        This function is implemented if a probability is required instead of the class value for classification.

        :param: x: A list or list of list of input vector
        :return: A dict of predict probability of the model for
         each feature in the features list
        """
        pass

    def feature_list(self):
        """
        Required for online and offline predictions. This function binds the request data to the actual feature names.

        :param: None
        :return: A list of features
        """
        return ['Store','Dept','Date','IsHoliday']

