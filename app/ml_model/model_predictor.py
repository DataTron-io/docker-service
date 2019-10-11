from __future__ import absolute_import, division, print_function
import json
import logging
import pickle
import pandas as pd
import numpy as np
import shlex
import subprocess
import wave
from deepspeech import Model
from timeit import default_timer as timer

try:
    from shhlex import quote
except ImportError:
    from pipes import quote


BEAM_WIDTH = 500

LM_ALPHA = 0.75

LM_BETA = 1.85

N_FEATURES = 26

N_CONTEXT = 9





def metadata_to_string(metadata):
    return ''.join(item.character for item in metadata.items)


class ModelPredictor(object):
    def __init__(self):
        pass

    def predict(self, x):
        """
        Required for online and offline predictions

        :param: x : A list or list of list of input vector
        :return: single prediction
        """

        ds = Model('/Users/rohankhade/docker-service/app/ml_model/deepspeech-0.5.1-models/output_graph.pbmm', N_FEATURES, N_CONTEXT, '/Users/rohankhade/docker-service/app/ml_model/deepspeech-0.5.1-models/alphabet.txt', BEAM_WIDTH)
        ds.enableDecoderWithLM('/Users/rohankhade/docker-service/app/ml_model/deepspeech-0.5.1-models/alphabet.txt', '/Users/rohankhade/docker-service/app/ml_model/deepspeech-0.5.1-models/lm.binary', '/Users/rohankhade/docker-service/app/ml_model/deepspeech-0.5.1-models/trie', LM_ALPHA, LM_BETA)
        audio = x[0][0].astype(np.int16)
        return ds.stt(audio, 16000)

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
        return ['V1']

