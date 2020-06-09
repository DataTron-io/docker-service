from app.ml_model.model_predictor import ModelPredictor
import pytest
import numpy as np
import pandas as pd
import logging
import unittest
import json
import logging

@pytest.mark.incremental
class TestModelPredictor():
    @pytest.fixture(autouse=True)
    def _predictorclass(self):
        self.predictor = ModelPredictor()
        self.numofrows = 10
        self.file="/Users/ngzhiyong/Desktop/birth_weight.csv"

    def read_csvrows(self):
        try:
            dataDB= pd.read_csv(filepath_or_buffer=self.file, nrows=self.numofrows, delimiter=",")
            return dataDB
        except Exception as e:
            logging.error("An exception occured during reading of csv file: {}".format(str(e)))


    @pytest.mark.dependency()
    def test_feature_list(self):
        assert isinstance(self.predictor.feature_list(), list) , "return value of feature_list should be type list"

    @pytest.mark.dependency(depends=['TestModelPredictor::test_feature_list'])
    def test_predict_scoring(self):
        row=self.read_csvrows()
        feature_list = self.predictor.feature_list()
        x_list = row[feature_list].values
        prediction=self.predictor.predict(x_list)

        assert isinstance(prediction, np.ndarray) , "[ERROR] return values of predict should be of type numpy array"
        assert len(prediction) != 0 , "[ERROR] return list cannot be empty"
        assert len(prediction) == self.numofrows , "[ERROR] number of predictions needs to be same as number of chunks"

        logging.info("Integration test for scoring is successful!")
        logging.info("The prediction generated is: {} and will be stored into a csv or zip format via Datatron Platform into HDFS storage".format(prediction))
        logging.warning("The features declared in feature_list function, will be the features passed into the model, hence ensure that feature declared are the ones used to train the model")

    @pytest.mark.skip(reason="not current under used")
    def test_predict_proba(self, x):
        assert isinstance(self.predict_proba(x), dict)