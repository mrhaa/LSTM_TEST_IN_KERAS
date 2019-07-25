from Test_MariaDB import WrapDB
import pandas as pd
import numpy as np
import copy
import math
from datetime import datetime



db = WrapDB()
db.connet(host="127.0.0.1", port=3306, database="WrapDB_1", user="root", password="ryumaria")

target_list = {"MSCI World":14, "MSCI EM":15, "KOSPI":8, "S&P500":1, "상해종합":6,"STOXX50":2,"WTI 유가":172,"금":191}
#target_list = {"KOSPI":8}
datas = db.get_bloomberg_datas(data_list=target_list.values(), start_date=None, end_date=None)
datas.columns = ["아이템코드", "아이템명", "날짜", "값"]

sampling_type ='D'
index ='날짜'
columns ='아이템명'
values ='값'

datas["날짜T"] = datas[index].apply(lambda x: pd.to_datetime(str(x), format="%Y-%m-%d"))

# sampling type
# 'B': business daily, 'D': calendar daily, 'W': weekly, 'M': monthly
sampling_list = datas.resample(sampling_type, on="날짜T", convention="end")
sampled_datas = datas.loc[datas["날짜T"].isin(list(sampling_list.indices))]

pivoted_sampled_datas = sampled_datas.pivot(index="날짜", columns=columns, values=values)
#print(pivoted_sampled_datas)

count = 0
for column in pivoted_sampled_datas.columns:

    flag =  False
    start_date = pd.to_datetime("2100-01-01", format="%Y-%m-%d")
    end_date = pd.to_datetime("1900-01-01", format="%Y-%m-%d")
    start_value = 0
    end_value = 0
    for row_start in pivoted_sampled_datas.index:

        row_start_t = pd.to_datetime(row_start, format="%Y-%m-%d")

        if math.isnan(pivoted_sampled_datas[column][row_start]) == False:

            for end_idx, row_end in enumerate(pivoted_sampled_datas.index):

                row_end_t = pd.to_datetime(row_end, format="%Y-%m-%d")

                if row_end_t > row_start_t and math.isnan(pivoted_sampled_datas[column][row_end]) == False:

                    if (row_end_t - row_start_t).days > 90:
                        break
                    else:
                        profit = pivoted_sampled_datas[column][row_end] / pivoted_sampled_datas[column][row_start] - 1
                        if profit > 0.20:# or profit < -0.10:

                            flag = True

                            if row_start_t < start_date:
                                start_date = row_start_t
                                start_value = pivoted_sampled_datas[column][row_start]
                            if row_end_t > end_date:
                                end_date = row_end_t
                                end_value = pivoted_sampled_datas[column][row_end]

        if flag == True and end_idx < len(pivoted_sampled_datas.index) - 1 and end_date < row_start_t:
            print(column + "\t" + str(start_date)[:10] + "\t" + str(end_date)[:10] + "\t" + str((end_value/start_value-1)*100))

            flag = False

            start_date = pd.to_datetime("2100-01-01", format="%Y-%m-%d")
            end_date = pd.to_datetime("1900-01-01", format="%Y-%m-%d")
            start_value = 0
            end_value = 0

            count += 1

db.disconnect()