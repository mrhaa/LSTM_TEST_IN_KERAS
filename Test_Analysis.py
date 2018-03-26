#_*_ coding: utf-8 _*_

import win32com.client
from Test_MariaDB import WrapDB
import pandas as pd
import xlsxwriter
import openpyxl
from xlutils.copy import copy as xl_copy


# Wrap운용팀 DB Connect
db = WrapDB()
db.connet()

# 데이터 Info Read
data_info = db.get_data_info()
data_info.columns = ["아이템코드", "아이템명", "개수", "시작일", "마지막일"]

# 모든 Factor 데이터가 존재하는 기간 설정
date_range = {}
for ele in data_info:
    if ele in ("시작일", "마지막일"):
        if ele == "시작일":
            date_range[ele] = "1111-11-11"
        elif ele == "마지막일":
            date_range[ele] = "9999-99-99"
        else:
            continue

        for idx in range(0, len(data_info[ele])):
            if data_info[ele][idx]:
                if ele == "시작일" and int(date_range[ele].replace("-", "")) < int(data_info[ele][idx].replace("-", "")):
                    date_range[ele] = data_info[ele][idx]
                if ele == "마지막일" and int(date_range[ele].replace("-", "")) > int(data_info[ele][idx].replace("-", "")):
                    date_range[ele] = data_info[ele][idx]



# 유효기간 내 데이터 Read
data_list = list(data_info["아이템코드"])
start_date = date_range["시작일"]
end_date = date_range["마지막일"]
datas = db.get_datas(data_list, start_date, end_date)
datas.columns = ["아이템코드", "아이템명", "날짜", "값"]


# 월말 데이터만 선택
datas["날짜T"] = datas["날짜"].apply(lambda x: pd.to_datetime(str(x), format="%Y-%m-%d"))
#datas.set_index(datas['날짜T'], inplace=True)
monthly_samles = datas.resample('M', on="날짜T", convention="end")
monthly_datas = datas.loc[datas["날짜T"].isin(list(monthly_samles.indices))]
#print (monthly_datas)
for idx, values in enumerate(monthly_datas.values):
    print (idx, values)



# Wrap운용팀 DB Disconnect
db.disconnect()