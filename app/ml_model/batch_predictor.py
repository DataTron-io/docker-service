import json
import logging
import pickle
import pandas as pd


class BatchPredictor(object):
    """
    This class is modified by the user to upload the model into the Datatron platform.
    """
    def __init__(self):
        pass

    def transform(self, x):
        """
        Required for offline predictions

        :param: x : Individual line of data from the input file
        :return: Data Object which is used by the model to make a prediction

        Examples:

        1. Input : 43,56,67,4,Honda
           Output: [43, 56, 67, 4, "Honda"]
        
        2. Input : Dhruv,81,Male,321,546,Test
           Output: 
                {
                    "name": "Dhruv",
                    "Age": 81,
                    "Test": {
                        "Nested_Object": "Test"
                    }
                    "Sex": "Male",
                    "Height": 546,
                    "Weight": 321,
                }

        3. Input : {"preview":true,"offset":0,"result":{"Name":"mamycita","_geo":"7.13,120.193"}}
           Ouput : 

        """

        pass
    def output_format(self, datatron_request_id, x):
        """
        Optional: Generate format of output for the request string
        Args:
            datatron_request_id ([type]): [description]
            x ([type]): [description]
        """





