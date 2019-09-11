import json
import logging
import pickle
import pandas as pd


class ModelPredictor(object):
    def __init__(self):
        pass

    def predict(self, x):
        """
        Required for online and offline predictions

        :param: x : A list or list of list of input vector
        :return: single prediction
        """

        model = pickle.load(open("models/xgboost_birth_model.pkl", "rb"))
        x= pd.DataFrame(x, columns = self.feature_list())
        return model.predict(x)

    def predict_proba(self, x):
        """
        Required if using Datatron's Explanability feature

        :param: x: A list or list of list of input vector
        :return: A dict of predict probability of the model for
         each feature in the features list
        """
        pass

    def feature_list(self):
        """
        Required for online and offline predictions

        :param: None
        :return: A list of features
        """
        return ['Black','Married','Boy','MomAge','MomSmoke','CigsPerDay','MomWtGain','Visit','MomEdLevel']

