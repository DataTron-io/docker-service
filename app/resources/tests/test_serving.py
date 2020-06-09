from app.ml_model.model_predictor import ModelPredictor
import json
import pathlib
import pytest
import numpy as np
import logging
import os

@pytest.mark.incremental
class TestModelPredictor():
    #Pytest Fixtures
    @pytest.fixture(autouse=True)
    def _predictorclass(self):
        self.predictor = ModelPredictor()

    @pytest.fixture
    def json_config_input(self, request):
        try:
            file = pathlib.Path(request.node.fspath.strpath)
            config = file.with_name('challenger_endpoint.json')
            with config.open() as fp:
                return json.load(fp)
        except Exception as e:
            logging.error("An exception occured when loading challenger_endpoint.json: {}".format(str(e)))


    @pytest.fixture
    def json_config_output(self, request):
        try:
            file = pathlib.Path(request.node.fspath.strpath)
            config = file.with_name('challenger_output.json')
            with config.open() as fp:
                return json.load(fp)
        except Exception as e:
            logging.error("An exception occured when loading challenger_output.json: {}".format(str(e)))

    def write_json(self, sample_output):
        try:
            with open('app/resources/tests/challenger_output.json', 'w') as outfile:
                json.dump(sample_output, outfile, indent=4)
        except Exception as e:
            logging.error("An exception occured when writing into challenger_output.json: {}".format(str(e)))

    #integration tests
    @pytest.mark.dependency()
    def test_feature_list(self):
        assert isinstance(self.predictor.feature_list(), list) , "return value of feature_list should be type list"

    @pytest.mark.dependency(depends=['TestModelPredictor::test_feature_list'])
    def test_predict_serving(self, json_config_input, json_config_output):
        x_dict=json_config_input["data"]
        logging.info("Retrieving model's feature list...")
        feature_list = self.predictor.feature_list()
        x = np.array([[x_dict[feature_name] for feature_name in feature_list]])
        logging.info("Getting challenger prediction...")
        y= self.predictor.predict(x)
        
        assert isinstance(y, np.ndarray) , "[ERROR] return value of predict should be of type numpy array"
        assert len(y) != 0 , "[ERROR] return numpy array cannot be empty"
        assert len(y) == 1 , "[ERROR] number of predictions must be one for serving"
        
        logging.info("Writing challenger_endpoint output into json format")
        sample_output = json_config_output
        sample_output['results']['primary']['prediction'] = {'outputs': y[0].item()}
        logging.info("Challenger Endpoint output: {}".format(sample_output))
        logging.warning("The features declared in feature_list function, will be the features passed into the model, hence ensure that feature declared are the ones used to train the model")
        
        self.write_json(sample_output)
        
    @pytest.mark.skip(reason="not current under used")
    def test_predict_proba(self, x):
        assert isinstance(self.predict_proba(x), dict)

   