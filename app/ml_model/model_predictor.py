import json
import logging
import pickle
import pandas as pd
from .scraping_bing import searchSingle
import gensim
from app.utils.helper import MeanEmbeddingVectorizer
from keras.models import load_model
from keras.models import model_from_json
import os
from sklearn.preprocessing import LabelEncoder
import numpy as np
from keras.optimizers import SGD
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import Dropout
from keras.constraints import maxnorm
import tensorflow as tf


graph = tf.get_default_graph()
cwd = os.getcwd()


class ModelPredictor(object):
    """
    This class is modified by the user to upload the model into the Datatron platform.
    """
    def __init__(self):
        import spacy
        import en_core_web_sm
        from gensim.models.word2vec import Word2Vec
        self.nlp = en_core_web_sm.load()
        self.stop_words = spacy.lang.en.stop_words.STOP_WORDS
        self.word_model = Word2Vec.load(cwd + "/app/ml_model/word2vec.model")
        self.model = load_model(cwd + "/app/ml_model/model2.h5", compile=False)
        self.encoder = LabelEncoder()
        self.encoder.classes_ = np.load(cwd + "/app/ml_model/classes.npy").tolist()
        #json_file = open(cwd + "/app/ml_model/model.json", 'r')
        #model = json_file.read()
        #json_file.close()
        #self.model = model_from_json(model)
        # load weights into new model
        #self.model.load_weights(cwd + "/app/ml_model/model1.h5")

    def preprocess(self, query):
        query = query.replace('\W', ' ')
        query = gensim.utils.simple_preprocess(query, deacc=True)
        query = " ".join(query)

        def lemmatize(simple_docs):
            doc = self.nlp(simple_docs)
            tokens = [token.lemma_ for token in doc if (token.text not in self.stop_words)]
            return tokens
        query = lemmatize(query)
        #query = [self.word_model.wv[word] for word in query]
        mean_vec = MeanEmbeddingVectorizer(self.word_model)
        query = mean_vec.transform(query)
        return query

    def predict(self, x):
        """
        Required for online and offline predictions

        :param: x : dict
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
        query = x['query']
        shipper = x['shipper']
        bing_query = searchSingle(query)
        if not bing_query:
            return
        bing_query = self.preprocess(bing_query)
        bing_query = pd.DataFrame([bing_query])
        with graph.as_default():
            result = self.model.predict(bing_query.loc[:0, :], batch_size=1, verbose=1).tolist()
        proba = [(i, j) for i, j in zip(self.encoder.classes_, result[0])]
        return list(sorted(proba, key=lambda x: x[1], reverse=True)[:6])



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

