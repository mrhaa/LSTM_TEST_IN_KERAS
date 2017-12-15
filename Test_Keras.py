'''
import pandas
import matplotlib.pyplot as plt
dataset = pandas.read_csv('a.csv', usecols=[1], engine='python', skipfooter=0)
print(dataset)
plt.plot(dataset)
plt.show()
'''


# convert an array of values into a dataset matrix
def create_dataset(dataset, look_back=1, in_from=0, in_to=1, out_from=1, out_to=2):
	dataX, dataY = [], []
	for i in range(len(dataset)-look_back-1):
		dataX.append(dataset[i:(i+look_back), in_from:in_to])
		dataY.append(dataset[i + look_back, out_from:out_to])
	return numpy.array(dataX), numpy.array(dataY)


import numpy
import matplotlib.pyplot as plt
import pandas
import math
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error

# fix random seed for reproducibility
numpy.random.seed(7)

# load the dataset
#dataframe = pandas.read_csv('a.csv', names=['x','y'], engine='python', skipfooter=0, skiprows=1)
#dataframe = pandas.read_excel('major_indice.xlsx', skipfooter=0)
dataframe = pandas.read_excel('major_indice.xlsx', sheetname='indice_week')
dataset = dataframe.values
dataset = dataset.astype('float32')
#print(dataset)

# normalize the dataset
#scaler = MinMaxScaler(feature_range=(0, 1))
#dataset = scaler.fit_transform(dataset)
#print(dataset)

# split into train and test sets
train_size = int(len(dataset) * 0.9)
test_size = len(dataset) - train_size
train, test = dataset[0:train_size,:], dataset[train_size:len(dataset),:]

# reshape into X=t and Y=t+1
look_back = 4
in_from = 0
in_to = 3
out_from = 3
out_to = 5
trainX, trainY = create_dataset(train, look_back, in_from, in_to, out_from, out_to)
#print(trainX.shape, trainY.shape)
#print(trainX)
#print(trainY)
testX, testY = create_dataset(test, look_back, in_from, in_to, out_from, out_to)
#print(trainX)
#print(trainY)
# reshape input to be [samples, time steps, features]
#print(trainX.shape)
trainX = numpy.reshape(trainX, (trainX.shape[0], trainX.shape[1], trainX.shape[2]))
trainY = numpy.reshape(trainY, (trainY.shape[0], trainY.shape[1]))
print(trainX.shape, trainY.shape)
print("trainX:\n", trainX)
print("trainY:\n", trainY)
testX = numpy.reshape(testX, (testX.shape[0], testX.shape[1], testX.shape[2]))
testY = numpy.reshape(testY, (testY.shape[0], testY.shape[1]))
print(testX.shape, testY.shape)
print("testX:\n", testX)
print("testY:\n", testY)

# create and fit the LSTM network
model = Sequential()
#model.add(LSTM(4, input_dim=look_back))
model.add(LSTM(output_dim=6, input_dim=trainX.shape[2], input_length=trainX.shape[1]))
model.add(Dense(2))
model.compile(loss='mean_squared_error', optimizer='adam')
for loop in range(10):
	print("--------------- Loop # %d ---------------" % loop)
	#del model
	#model = load_model('my_model.h5')
	#model.fit(trainX, trainY, nb_epoch=1000, batch_size=1, verbose=2)
	model.fit(trainX, trainY, nb_epoch=1000, batch_size=1, verbose=2)
	#model.save('my_model.h5')  # creates a HDF5 file 'my_model.h5'
	#del model  # deletes the existing model

	# make predictions
	trainPredict = model.predict(trainX)
	print("trainPredict:\n")
	success = 0
	fail = 0
	for idx in range(len(trainX)):
		err_0 = abs(trainY[idx][0] - trainPredict[idx][0])
		err_1 = abs(trainY[idx][1] - trainPredict[idx][1])
		if (trainY[idx][0] == 1 and trainPredict[idx][0] > trainPredict[idx][1]) or (trainY[idx][1] == 1 and trainPredict[idx][0] < trainPredict[idx][1]):
			success += 1
			print(trainY[idx][0], '\t', trainPredict[idx][0], '\t', trainY[idx][1], '\t', trainPredict[idx][1], '\t', err_0, '\t', err_1, True)
		else:
			fail += 1
			print(trainY[idx][0], '\t', trainPredict[idx][0], '\t', trainY[idx][1], '\t', trainPredict[idx][1], '\t', err_0, '\t', err_1, False)
	print(success/float(success+fail))

	testPredict = model.predict(testX)
	print("testPredict:\n")
	success = 0
	fail = 0
	for idx in range(len(testX)):
		err_0 = abs(testY[idx][0] - testPredict[idx][0])
		err_1 = abs(testY[idx][1] - testPredict[idx][1])
		if (testY[idx][0] == 1 and testPredict[idx][0] > testPredict[idx][1]) or (testY[idx][1] == 1 and testPredict[idx][0] < testPredict[idx][1]):
			success += 1
			print(testY[idx][0], '\t', testPredict[idx][0], '\t', testY[idx][1], '\t', testPredict[idx][1], '\t', err_0, '\t', err_1, True)
		else:
			fail += 1
			print(testY[idx][0], '\t', testPredict[idx][0], '\t', testY[idx][1], '\t', testPredict[idx][1], '\t', err_0, '\t', err_1, False)
	print(success/float(success+fail))

'''
# invert predictions
trainPredict = scaler.inverse_transform(trainPredict)
trainY = scaler.inverse_transform([trainY])
testPredict = scaler.inverse_transform(testPredict)
testY = scaler.inverse_transform([testY])
# calculate root mean squared error
trainScore = math.sqrt(mean_squared_error(trainY[0], trainPredict[:,0]))
print('Train Score: %.2f RMSE' % (trainScore))
testScore = math.sqrt(mean_squared_error(testY[0], testPredict[:,0]))
print('Test Score: %.2f RMSE' % (testScore))
'''

'''
# shift train predictions for plotting
trainPredictPlot = numpy.empty_like(dataset)
trainPredictPlot[:, :] = numpy.nan
trainPredictPlot[look_back:len(trainPredict)+look_back, :] = trainPredict
# shift test predictions for plotting
testPredictPlot = numpy.empty_like(dataset)
testPredictPlot[:, :] = numpy.nan
testPredictPlot[len(trainPredict)+(look_back*2)+1:len(dataset)-1, :] = testPredict
# plot baseline and predictions
#plt.plot(scaler.inverse_transform(dataset))
#plt.plot(trainPredictPlot)
#plt.plot(testPredictPlot)
#plt.show()
'''