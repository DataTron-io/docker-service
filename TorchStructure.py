import torch.nn as nn
import torch
import pandas as pd
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
import numpy as np
from torch.utils.data import DataLoader
from torch.utils.data.sampler import SubsetRandomSampler
import torch.nn.functional as F
import torch.nn as nn
import torch
import torch.utils
from torch.autograd import Variable




#Torch Model Structure
class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.fc1 = nn.Linear(29, 6)
        self.fc2 = nn.Linear(6, 2)
    def forward(self, x):
        x = F.relu(self.fc1(x))
        x = self.fc2(x)
        return torch.sigmoid(x)


def save():
	#separate X and Y
	X=data.iloc[:,0:-1].values
	Y=data.iloc[:,-1].values

	#Normalize data
	min_max_scaler=preprocessing.MinMaxScaler()
	X=min_max_scaler.fit_transform(X)


	#Separate train/Test data
	X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=230)

	batch_size=30
	epochs=2
	learning_rate=0.001

	#Training data set tensors
	train_target = torch.tensor(y_train.astype(np.long))
	train = torch.tensor(X_train.astype(np.float32)) 
	train_tensor = torch.utils.data.TensorDataset(train, train_target) 
	train_loader = torch.utils.data.DataLoader(dataset = train_tensor, batch_size = 16, shuffle = True)

	#test dataset tensors
	test_target = torch.tensor(y_test.astype(np.long))
	test = torch.tensor(X_test.astype(np.float32)) 
	test_tensor = torch.utils.data.TensorDataset(train, train_target) 
	test_loader = torch.utils.data.DataLoader(dataset = train_tensor, batch_size = 16, shuffle = True)

	net = Net()    

	# create a stochastic gradient descent optimizer
	optimizer = torch.optim.SGD(net.parameters(), lr=learning_rate, momentum=0.9)
	# create a loss function
	criterion = nn.CrossEntropyLoss()

	# run the main training loop
	for epoch in range(epochs):
	    for batch_idx, (Xdata, target) in enumerate(train_loader):
	        Xdata, target = Variable(Xdata), Variable(target)
	        optimizer.zero_grad()
	        net_out = net(Xdata)
	        loss = criterion(net_out, target)
	        loss.backward()
	        optimizer.step()
	    out = net(test)
	    _,predicted = torch.max(out.data,1)
	    total = test_target.size(0)
	    correct = (predicted==test_target).sum().item()
	    print('Epoch #',epoch+1,"  ",'Accuracy: {} %'.format(100 * correct / total))
	    print ("---------------------")

	#Save Model
	torch.save(net, "../SavedModels/Torch_Model.pt")
