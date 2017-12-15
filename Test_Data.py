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
import datetime
import math
import matplotlib.pyplot as plt

# fix random seed for reproducibility
numpy.random.seed(7)

# load the dataset
raw_dataframe = pandas.read_excel('major_indice.xlsx', sheetname='indice_day')
#raw_dataframe = pandas.read_excel('major_indice.xlsx', sheetname='indice_week_1')
#print(raw_dataframe)
# 인덱스인 날짜의 포맷 변경
for column_name in raw_dataframe.columns:
    if 'date' in column_name:
        #print(raw_dataframe[column_name])
        dates = []
        for serial_date in raw_dataframe[column_name]:
            if not math.isnan(serial_date):
                serial_date = int(serial_date)
                base_date = datetime.datetime(1899, 12, 30)
                delta = datetime.timedelta(days=serial_date)
                #print(serial_date, str(base_date + delta)[0:11])
                dates.append(str(base_date + delta)[0:11])
            else:
                dates.append('')
        #print(column_name)
        raw_dataframe[column_name] = dates
        #print(raw_dataframe[column_name])
#print('*' * 100)
#print(raw_dataframe)

dataframes = []
for idx, column_name in enumerate(raw_dataframe.columns):
    #print(idx)
    if 'date' in column_name:
        #print(column_name)
        dataframes.append(pandas.DataFrame(index=raw_dataframe.T.values[idx], data=raw_dataframe.T.values[idx+2]*100, columns=[column_name.split('_')[0]]))
        #print(dataframes[0])

# 첫번째 컬럼은 데이터 누락이 없는 FX 데이터
# FX 데이터에 Outer join
dataframe = dataframes[0]
#print(pandas.isnull(dataframe).any(1).nonzero()[0])
dataframe = dataframe.drop(dataframe.index[pandas.isnull(dataframe).any(1).nonzero()[0]])
#print(pandas.isnull(dataframe).any(1).nonzero()[0])
#print(dataframe)
for idx, tmp_dataframe in enumerate(dataframes):
    #print(dataframe)
    if idx != 0:
        dataframe = dataframe.join(dataframes[idx])
print(dataframe.columns)
del dataframe['natural']
del dataframe['wti']
print(dataframe.columns)

target_idx = 0
input_num = len(dataframe.columns)
output_num = 2
for idx, column in enumerate(dataframe.columns):
    if 'gold' == column:
        target_idx = idx
print(target_idx, input_num, output_num)

#print(pandas.isnull(dataframe).any(1).nonzero()[0])

# 빈 데이터의 경우 전일자 값 사용
dataframe = dataframe.sort_index()
for row in pandas.isnull(dataframe).any(1).nonzero()[0]:
    #print(idx, dataframe.values[row])
    for column, value in enumerate(dataframe.values[row]):
        #print(column, value)
        if math.isnan(value):
            #print(dataframe.values[row][column])
            dataframe.values[row, column] = 0
            #print(dataframe.values[row][column])
#print(dataframe)

# Supervised learning의 결과 컬럼 추가
dataframe_average = dataframe.copy()
#print(dataframe_average)
result1 = []
result2 = []
for idx, row in enumerate(dataframe.iterrows()):
    pl1 = 0
    pl2 = 1
    for column, value in enumerate(dataframe.values[idx]):

        if column == target_idx and idx < len(dataframe.index) - 1:
            #print(dataframe.values[idx + 1, column])
            pl1 = 1 if dataframe.values[idx, column] > 0 else 0
            pl2 = 0 if dataframe.values[idx, column] > 0 else 1

        # 5영업일 평균을 사용
        if idx >= 4:
            dataframe_average.values[idx, column] = sum(dataframe.values[idx-4:idx+1, column]) / len(dataframe.values[idx-4:idx+1, column])

            if 1 and idx >= 30:
                mean = numpy.mean(dataframe_average.values[idx-30:idx+1, column])
                std = numpy.std(dataframe_average.values[idx-30:idx+1, column])
                if dataframe_average.values[idx, column] > mean + 3 * std:
                    dataframe_average.values[idx, column] = mean + 3 * std
                if dataframe_average.values[idx, column] < mean - 3 * std:
                    dataframe_average.values[idx, column] = mean - 3 * std

    result1.append(pl1)
    result2.append(pl2)
dataframe_average['result1'] = result1
dataframe_average['result2'] = result2
#print(dataframe_average)

writer = pandas.ExcelWriter('output.xlsx')
dataframe.to_excel(writer,'Sheet1')
dataframe_average.to_excel(writer,'Sheet2')
writer.save()

#dataframe = dataframe.cumsum()
#plt.figure()
#dataframe.plot()

# convert an array of values into a dataset matrix
def create_dataset(dataset, look_back=1, in_from=0, in_to=1, out_from=1, out_to=2):
	dataX, dataY = [], []
	for i in range(len(dataset)-look_back-1):
		dataX.append(dataset[i:i + look_back, in_from:in_to])
		dataY.append(dataset[i + look_back, out_from:out_to])
	return numpy.array(dataX), numpy.array(dataY)

dataset = dataframe_average.values
dataset = dataset.astype('float32')
#print(dataset)


train_size = int(len(dataset) * 0.9)
test_size = len(dataset) - train_size
train, test = dataset[0:train_size,:], dataset[train_size:len(dataset),:]

# reshape into X=t and Y=t+1
look_back = 3
in_from = 0
in_to = input_num
out_from = input_num
out_to = input_num + output_num
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
model.add(LSTM(output_dim=2, input_dim=trainX.shape[2], input_length=trainX.shape[1]))
model.add(Dense(2))
model.compile(loss='mean_squared_error', optimizer='adam')
for loop in range(10):
    print("--------------- Loop # %d ---------------" % loop)
    #del model
    #model = load_model('my_model.h5')
    #model.fit(trainX, trainY, nb_epoch=100, batch_size=1, verbose=2)
    model.fit(trainX, trainY, nb_epoch=100, batch_size=1, verbose=0)


    # make predictions
    trainPredict = model.predict(trainX)
    print("trainPredict:\n")
    success = 0
    fail = 0
    for idx in range(len(trainX)):
        err_0 = abs(trainY[idx][0] - trainPredict[idx][0])
        err_1 = abs(trainY[idx][1] - trainPredict[idx][1])
        if (trainY[idx][0] == 1 and trainPredict[idx][0] > trainPredict[idx][1]) or (
                trainY[idx][1] == 1 and trainPredict[idx][0] < trainPredict[idx][1]):
            success += 1
            print(trainY[idx][0], '\t', trainPredict[idx][0], '\t', trainY[idx][1], '\t', trainPredict[idx][1], '\t', err_0,
                  '\t', err_1, True)
        else:
            fail += 1
            print(trainY[idx][0], '\t', trainPredict[idx][0], '\t', trainY[idx][1], '\t', trainPredict[idx][1], '\t', err_0,
                  '\t', err_1, False)
    print(success / float(success + fail))

    testPredict = model.predict(testX)
    print("testPredict:\n")
    success = 0
    fail = 0
    for idx in range(len(testX)):
        err_0 = abs(testY[idx][0] - testPredict[idx][0])
        err_1 = abs(testY[idx][1] - testPredict[idx][1])
        if (testY[idx][0] == 1 and testPredict[idx][0] > testPredict[idx][1]) or (
                testY[idx][1] == 1 and testPredict[idx][0] < testPredict[idx][1]):
            success += 1
            print(testY[idx][0], '\t', testPredict[idx][0], '\t', testY[idx][1], '\t', testPredict[idx][1], '\t', err_0,
                  '\t', err_1, True)
        else:
            fail += 1
            print(testY[idx][0], '\t', testPredict[idx][0], '\t', testY[idx][1], '\t', testPredict[idx][1], '\t', err_0,
                  '\t', err_1, False)
    print(success / float(success + fail))
