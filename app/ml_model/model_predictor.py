import json
import logging
import pickle
import pandas as pd
from keras.models import load_model
import numpy as np

class ModelPredictor(object):
    def __init__(self):
        pass

    def predict(self, x):
        """
        Required for online and offline predictions

        :param: x : A list or list of list of input vector
        :return: single prediction
        """
		#define initial structure
        class Net(nn.Module):
            def __init__(self):
                super(Net, self).__init__()
                self.fc1 = nn.Linear(29, 6)
                self.fc2 = nn.Linear(6, 2)
            def forward(self, x):
                x = F.relu(self.fc1(x))
                x = self.fc2(x)
                return torch.sigmoid(x)
        net = Net()
        #read the model
        model = torch.load("models/Torch_Model.pt") 
        x= pd.DataFrame(x, columns = self.feature_list()).values()
        x=torch.tensor(x.astype(np.float32))
        #Make predictions
        out = model(row)
        _,predicted = torch.max(out.data,1)
        #convert prediction from tensor to numpy array
        prediction=predicted.numpy()
        return prediction

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
        return ['V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'V10', 'V11','V12', 'V13', 'V14', 'V15', 'V16', 'V17', 'V18', 'V19', 'V20', 'V21','V22', 'V23', 'V24', 'V25', 'V26', 'V27', 'V28', 'Amount']

