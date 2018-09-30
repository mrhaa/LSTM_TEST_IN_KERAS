from Test_MariaDB import WrapDB
import pandas as pd
import numpy as np
import copy
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import multiprocessing as mp

import time

from pandas import Series, DataFrame
import pandas as pd
#from pandas_datareader import data
from pandas.tseries.offsets import Day, MonthEnd
import math
import numpy as np
import sys

import matplotlib.pyplot as plt

from scipy.stats import rankdata
from scipy.stats import stats
from scipy.optimize import minimize

from Wrap_Folione import Preprocess
import Wrap_Util



db = WrapDB()
db.connet(host="127.0.0.1", port=3306, database="WrapDB_1", user="root", password="ryumaria")

# 데이터 전처리 instance 생성
preprocess = Preprocess()

# 데이터 Info Read
data_info = db.get_data_info()
asset_list = ['S&P500','STOXX50','Nikkei225','상해종합','KOSPI','MSCI World','MSCI EM','미국 국채 2년물 금리','미국 국채 10년물 금리','미국 국채 30년물 금리','WTI 유가','구리','금']
preprocess.SetDataInfo(data_info=data_info, data_info_columns=["아이템코드", "아이템명", "개수", "시작일", "마지막일"], data_list=asset_list)

# Factor 별 데이터가 존재하는 기간 설정
preprocess.MakeDateRange(start_date="시작일", last_date="마지막일")

# 유효기간 내 데이터 Read
# raw 데이터의 기간이 체계가 없어 return 받은 start_date, end_date을 사용할 수 없다.
data_list, start_date, end_date = preprocess.GetDataList(item_cd="아이템코드", start_date="시작일", last_date="마지막일")
datas = db.get_bloomberg_datas(data_list=data_list, start_date=None, end_date=None)
preprocess.SetDatas(datas=datas, datas_columns=["아이템코드", "아이템명", "날짜", "값"])

# DataFrame 형태의 Sampled Data 생성
# 'B': business daily, 'D': calendar daily, 'W-MON, W-WED': weekly, 'M': monthly
raw_data = preprocess.MakeSampledDatas(sampling_type='W-MON', index='날짜', columns='아이템명', values='값')

# 유효한 데이터 가장 최근 값으로 채움
filled_data = preprocess.FillValidData(look_back_days=30, input_data=raw_data)
profit_data = filled_data.rolling(window=2).apply(lambda x: x[1] / x[0] - 1)

# 유효기간을 벗어난 데이터 삭제
row_list = copy.deepcopy(profit_data.index)
for row_nm in row_list:
    for column_nm in profit_data.columns:
        if math.isnan(profit_data[column_nm][row_nm]) == True:
            profit_data.drop(index=row_nm, inplace=True)
            break

#Wrap_Util.SaveExcelFiles(file='AssetAllocation_pivoted_data.xlsx', obj_dict={'raw_data': raw_data, 'filled_data': filled_data, 'profit_data': profit_data})

db.disconnect()



def RP_TargetVol(rets, Target, lb, ub):
    rets.index = pd.to_datetime(rets.index)
    covmat = pd.DataFrame.cov(rets)

    def annualize_scale(rets):

        med = np.median(np.diff(rets.index.values))
        seconds = int(med.astype('timedelta64[s]').item().total_seconds())
        if seconds < 60:
            freq = 'second'.format(seconds)
        elif seconds < 3600:
            freq = 'minute'.format(seconds // 60)
        elif seconds < 86400:
            freq = 'hour'.format(seconds // 3600)
        elif seconds < 604800:
            freq = 'day'.format(seconds // 86400)
        elif seconds < 2678400:
            freq = 'week'.format(seconds // 604800)
        elif seconds < 7948800:
            freq = 'month'.format(seconds // 2678400)
        else:
            freq = 'quarter'.format(seconds // 7948800)

        def switch1(x):
            return {
                'day': 252,
                'week': 52,
                'month': 12,
                'quarter': 4,
            }.get(x)

        return switch1(freq)

    # --- Risk Budget Portfolio Objective Function ---#

    def RiskParity_objective(x):

        variance = x.T @ covmat @ x
        sigma = variance ** 0.5
        mrc = 1 / sigma * (covmat @ x)
        rc = x * mrc
        #a = np.reshape(rc, (len(rc), 1))
        a = np.reshape(rc.values, (len(rc), 1))
        risk_diffs = a - a.T
        sum_risk_diffs_squared = np.sum(np.square(np.ravel(risk_diffs)))
        return (sum_risk_diffs_squared)

    # --- Constraints ---#

    def TargetVol_const_lower(x):

        variance = x.T @ covmat @ x
        sigma = variance ** 0.5
        sigma_scale = sigma * np.sqrt(annualize_scale(rets))

        vol_diffs = sigma_scale - (Target * 0.95)
        return (vol_diffs)

    def TargetVol_const_upper(x):

        variance = x.T @ covmat @ x
        sigma = variance ** 0.5
        sigma_scale = sigma * np.sqrt(annualize_scale(rets))

        vol_diffs = (Target * 1.05) - sigma_scale
        return (vol_diffs)

        # --- Calculate Portfolio ---#

    x0 = np.repeat(1 / covmat.shape[1], covmat.shape[1])
    print(x0)
    lbound = np.repeat(lb, covmat.shape[1])
    ubound = np.repeat(ub, covmat.shape[1])
    bnds = tuple(zip(lbound, ubound))
    constraints = ({'type': 'ineq', 'fun': TargetVol_const_lower},
                   {'type': 'ineq', 'fun': TargetVol_const_upper})
    options = {'ftol': 1e-20, 'maxiter': 5000, 'disp': True}

    result = minimize(fun=RiskParity_objective,
                      x0=x0,
                      method='SLSQP',
                      constraints=constraints,
                      options=options,
                      bounds=bnds)
    return (result.x)

result = RP_TargetVol(profit_data, Target=0.1, lb=0.01, ub=0.19)
print(result)

total_weight = 0
for idx, weight in enumerate(result):
    total_weight += weight
    print(profit_data.columns[idx], weight)
print(total_weight)

'''
wts_cash = pd.DataFrame(1 - wts_tv.sum(axis = 1))
wts_cash.columns = ['Cash']
wts_new = pd.concat([wts_tv, wts_cash], axis = 1)

rets_cash = pd.DataFrame(data = np.repeat(0.00, rets.shape[0]).reshape(rets.shape[0], 1),
                         index = rets.index,
                         columns = ['Cash'])
rets_new = pd.concat([rets, rets_cash], axis = 1)
'''
