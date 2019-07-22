import json
import logging
#from keras import backend as K
#from keras.models import model_from_json
import pickle


class ModelPredictor(object):
    def __init__(self):
        pass

    def predict(self, x):
        """
        Required for online and offline predictions

        :param: x : A list or list of list of input vector
        :return: single prediction
        """
        model = pickle.load(open("xgboost.pkl", "rb"))

        #K.clear_session()

        # Example of predict function using Keras model

        # Model reconstruction from JSON file
        #with open('models/model.json', 'r') as f:
        #    model = model_from_json(f.read())

        # Load weights into the new model
        #model.load_weights('models/model.h5')
        return model.predict(x).tolist()

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
        return ['Pregnancies', 'Glucose', 'BloodPressure',
                'SkinThickness', 'Insulin', 'BMI',
                'DiabetesPedigreeFunction', 'Age']

