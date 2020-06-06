from app.ml_model.model_predictor import ModelPredictor
import pytest
import numpy as np
import pandas as pd
import logging
import unittest

@pytest.mark.incremental
class TestModelPredictor():
    @pytest.fixture(autouse=True)
    def _predictorclass(self):
        self.predictor = ModelPredictor()
        self.numofrows = 10
        self.file="/Users/ngzhiyong/Desktop/birth_weight.csv"

    #@pytest.fixture()
    #def name(self, pytestconfig):
    #    return pytestconfig.getoption("name")

    def read_csvrows(self):
        dataDB= pd.read_csv(filepath_or_buffer=self.file, nrows=self.numofrows, delimiter=",")
        return dataDB

    def test_feature_list(self):
        assert isinstance(self.predictor.feature_list(), list) , "return value of feature_list should be type list"

    #@pytest.mark.parametrize("xdata", [{'Black': [0,1], 'Married': [1,1], 'Boy': [0,1], 'MomAge': [3,5],'MomSmoke': [0,1], 'CigsPerDay': [0,0], 'MomWtGain': [2,1], 'Visit': [3,1], 'MomEdLevel': [2,1]}])
    def test_predict(self):
        row=self.read_csvrows()
        feature_list = self.predictor.feature_list()
        x_list = row[feature_list].values
        prediction=self.predictor.predict(x_list)

        assert isinstance(prediction, np.ndarray) , "return value of predict should be of type numpy array"
        assert len(prediction) != 0 , "return list cannot be empty"
        assert len(prediction) == self.numofrows , "number of prediction needs to be same as number of chunks"
        

    @pytest.mark.skip(reason="not current under used")
    def test_predict_proba(self, x):
        assert isinstance(self.predict_proba(x), dict)

   