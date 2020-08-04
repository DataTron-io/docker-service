import json
import logging
import pickle
import pandas as pd


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
        model = pickle.load(open("models/xgboost_birth_model.pkl", "rb"))
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
        return ['Black','Married','Boy','MomAge','MomSmoke','CigsPerDay','MomWtGain','Visit','MomEdLevel']

