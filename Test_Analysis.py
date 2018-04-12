#_*_ coding: utf-8 _*_

import win32com.client
from Test_MariaDB import WrapDB
import pandas as pd
import xlsxwriter
import openpyxl
from xlutils.copy import copy as xl_copy

from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import Test_Figure
import numpy as np



# Wrap운용팀 DB Connect
db = WrapDB()
db.connet(host="127.0.0.1", port=3306, database="WrapDB_1", user="root", password="maria")

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

# sampling type
# 0: business daily, 1: calendar daily, 2: weekly, 3: monthly
sampling_type = 1
if sampling_type == 0:
    sampling_list = datas.resample('B', on="날짜T", convention="end")
elif sampling_type == 1:
    sampling_list = datas.resample('D', on="날짜T", convention="end")
elif sampling_type == 2:
    sampling_list = datas.resample('W', on="날짜T", convention="end")
elif sampling_type == 3:
    sampling_list = datas.resample('M', on="날짜T", convention="end")
sampled_datas = datas.loc[datas["날짜T"].isin(list(sampling_list.indices))]
#print (sampled_datas)
#for idx, values in enumerate(sampled_datas.values):
#    print (idx, values)
#print (type(sampled_datas))

# pivot 이용해 PCA 분석을 위한 구조로 변경
pivoted_sampled_datas = sampled_datas.pivot(index='날짜', columns='아이템명', values='값')
#pivoted_sampled_datas[pivoted_sampled_datas.columns[4]][pivoted_sampled_datas.index[1]]

if 1:
    # Create a Pandas Excel writer using Openpyxl as the engine.
    writer = pd.ExcelWriter('test.xlsx', engine='openpyxl')
    # 주의: 파일이 암호화 걸리면 workbook load시 에러 발생
    writer.book = openpyxl.load_workbook('test.xlsx')
    # Pandas의 DataFrame 클래스를 그대로 이용해서 엑셀 생성 가능
    pivoted_sampled_datas.to_excel(writer, sheet_name='sheet3')
    writer.save()


# Test
#pivoted_sampled_datas = pivoted_sampled_datas[['BOJ자산','FED자산']]
#pivoted_sampled_datas = pd.DataFrame(data = np.array([[-1, -1], [-2, -1], [-3, -2], [1, 1], [2, 1], [3, 2]]), columns = ['BOJ자산','FED자산'])


if 0:
    # 정규화
    pivoted_sampled_datas = pd.DataFrame(StandardScaler().fit_transform(pivoted_sampled_datas))
    #pivoted_sampled_datas_T = pivoted_sampled_datas.transpose()
    #print (list(pivoted_sampled_datas))
    #print (pivoted_sampled_datas)

    pca = PCA(n_components=pivoted_sampled_datas.shape[1])
    #pca = PCA(n_components=2)
    #principalComponents = pca.fit_transform(pivoted_sampled_datas)
    pca.fit(pivoted_sampled_datas)
    principalComponents = pca.transform(pivoted_sampled_datas)
    #
    #print (principalDf)


if 0:
    principalDf = pd.DataFrame(data=principalComponents, columns=['principal component 1', 'principal component 2'])

    color = 'r'
    Test_Figure.Figure_2D_NoClass(pivoted_sampled_datas, 'Original', ['BOJ자산','FED자산'], color)
    Test_Figure.Figure_2D_NoClass(principalDf, '2 Component PCA', ['principal component 1', 'principal component 2'], color)


# Wrap운용팀 DB Disconnect
db.disconnect()



