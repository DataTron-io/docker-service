import json
import logging
import pickle
import pandas as pd


class ModelPredictor(object):
    """
    This class is modified by the user to upload the model into the Datatron platform.
    """
    def __init__(self):
        self.model =  pickle.load(open("models/model.pkl", "rb"))

    def predict(self, x):
        """
        Required for online and offline predictions

        :param: x : A list or list of list of input vector
        :return: single prediction
        
        """

        x = pd.DataFrame(x, columns=self.feature_list())
        return model.predict(x)

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
        return ["Married", "Credit_score", "Age", "Total_assets", "Number_years", "Income", "Savings", "Ed_level"]
    

