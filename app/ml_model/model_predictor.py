import json
import logging
import pickle
import pandas as pd
import numpy as np
import pandas as pd
import gender_guesser.detector as gender
import requests
import re


def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError

class ModelPredictor(object):
    def __init__(self):
        pass


    def bias_detection(self,firstname,surname,street,city,state,zip):
        output={}
        if firstname.strip():
            name=firstname.title().strip()
            global gender
            d = gender.Detector()
            genderType=d.get_gender(name)
            if genderType=="andy":
                genderType="Equal chance of male and female"
            output["first-name-analysis"]={'gender':genderType}
        else:
            output["first-name-analysis"]={'gender':"a first name is required to derive gender!"}
        if surname.strip():
            surnamesDF=pd.read_csv("./models/census_surnames_lower.csv")
            surname=surname.lower().strip()
            prediction=surnamesDF[surnamesDF.name==surname];
            if prediction.shape[0]==0:
                output['surname-analysis']={"U.S. Census Bureau provides no ethnicity information for this surname!"}
            else:
                output['surname-analysis']={"White":round(prediction['pctwhite'].iloc[0],2),
                        "Black":round(prediction['pctblack'].iloc[0],2),
                        "Hispanic":round(prediction['pcthispanic'].iloc[0],2),
                        "Asian, Hawaiian, Pacific Islander":round(prediction['pctapi'].iloc[0],2),
                        "American Indian and Alaska Native":round(prediction['pctaian'].iloc[0],2),
                        "Two or More Races":round(prediction['pct2prace'].iloc[0],2)
                       }
        else:
            output['surname-analysis']={"A surname is required to derive ethnicity based on surname"}
        
        if street.strip() and city.strip() and state.strip() and zip.strip():
            path="""
    https://geocoding.geo.census.gov/geocoder/geographies/address?street={}&city={}&state={}&zip={}
    &benchmark=4&vintage=4""".format(street,city,state,zip);
            x = requests.get(path)
            try:
                geoid = re.findall("GEOID: [0-9]{10,11}", x.text)[0].replace("GEOID: ","").strip()
                tractsDF=pd.read_csv("./models/tract_attr_over18.csv")
                geoid=int(geoid)
                prediction=tractsDF[tractsDF.GeoInd==geoid]
                if prediction.shape[0]==0:
                    output["location-analysis"]={"U.S. Census Bureau provides no ethnicity information for this location!"}
                else:
                    output["location-analysis"]={"White":round(prediction['geo_pr_white'].iloc[0],2),
                    "Black":round(prediction['geo_pr_black'].iloc[0],2),
                    "Hispanic":round(prediction['geo_pr_hispanic'].iloc[0],2),
                    "Asian, Hawaiian, Pacific Islander":round(prediction['geo_pr_api'].iloc[0],2),
                    "American Indian and Alaska Native":round(prediction['geo_pr_aian'].iloc[0],2),
                    "Two or More Races":round(prediction['geo_pr_mult_other'].iloc[0],2)
                    }
            except:
                output["location-analysis"]={"U.S. Census Bureau provides no ethnicity information for this location"}
        else:
            output['location-analysis']={"Street, city, state and zip code are required to derive ethnicity based on location"}
        return output



    def feature_list(self):
        """
        Required for online and offline predictions

        :param: None
        :return: A list of features
        """
        return ['firstname','surname','street','city','state','zip']

