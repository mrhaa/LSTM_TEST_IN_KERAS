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
datas = db.get_datas(data_list, start_date=None, end_date=None)
datas.columns = ["아이템코드", "아이템명", "날짜", "값"]


# 월말 데이터만 선택
datas["날짜T"] = datas["날짜"].apply(lambda x: pd.to_datetime(str(x), format="%Y-%m-%d"))
#datas.set_index(datas['날짜T'], inplace=True)


# Sampling 방법에 따른 데이터 누락을 위해 ref_data 생성
reference_list = datas.resample('D', on="날짜T", convention="end")
reference_datas = datas.loc[datas["날짜T"].isin(list(reference_list.indices))]
pivoted_reference_datas = reference_datas.pivot(index='날짜', columns='아이템명', values='값')


# sampling type
# 0: business daily, 1: calendar daily, 2: weekly, 3: monthly
sampling_type = 3
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
from datetime import datetime
from datetime import timedelta
import copy
pivoted_sampled_datas = sampled_datas.pivot(index='날짜', columns='아이템명', values='값')
for column_nm in pivoted_sampled_datas.columns:
    for row_nm in pivoted_sampled_datas.index:
        if pivoted_sampled_datas[column_nm][row_nm] == None:
            #print (column_nm, "\t", row_nm, "\t", pivoted_sampled_datas[column_nm][row_nm])

            #ref_row_nm = copy.copy(row_nm)
            ref_row_nm = row_nm

            # 해당일에 데이터가 없는 경우 가장 최근 값을 대신 사용함
            look_back_days = 10
            for loop_cnt in range(look_back_days):
                try:
                    if pivoted_reference_datas[column_nm][ref_row_nm] == None:
                        #print("No Data", str(ref_row_nm))
                        ref_row_nm = str(datetime.strptime(ref_row_nm, '%Y-%m-%d').date() - timedelta(days=1))
                    else:
                        pivoted_sampled_datas[column_nm][row_nm] = pivoted_reference_datas[column_nm][ref_row_nm]
                except KeyError:
                    #print("KeyError", str(ref_row_nm))
                    ref_row_nm = str(datetime.strptime(ref_row_nm, '%Y-%m-%d').date() - timedelta(days=1))

        # 이후 연산작업을 위해 decimal을 float 형태로 변경
        if pivoted_sampled_datas[column_nm][row_nm] != None:
            pivoted_sampled_datas[column_nm][row_nm] = float(pivoted_sampled_datas[column_nm][row_nm])


# 유효하지 않은 기간 drop
drop_basis_from = datetime.strptime('2007-01-31', '%Y-%m-%d').date()
drop_basis_to = datetime.strptime('2018-03-31', '%Y-%m-%d').date()
pivoted_sampled_datas_cp = copy.deepcopy(pivoted_sampled_datas)
for row_nm in pivoted_sampled_datas_cp.index:
    data_time = datetime.strptime(row_nm, '%Y-%m-%d').date()
    if data_time < drop_basis_from or data_time > drop_basis_to:
        #print (row_nm)
        pivoted_sampled_datas.drop(index=row_nm, inplace=True)


# 유효하지 않은 팩터 drop
total_omission_threshold = 1
last_omission_threshold = 1
last_considerable_num = 6
pivoted_sampled_datas_cp = copy.deepcopy(pivoted_sampled_datas)
for column_nm in pivoted_sampled_datas_cp.columns:
    total_null_cnt = pivoted_sampled_datas_cp[column_nm].isnull().sum()
    last_null_cnt = pivoted_sampled_datas_cp[column_nm][-last_considerable_num:].isnull().sum()

    if total_null_cnt > total_omission_threshold or last_null_cnt > last_omission_threshold:
        #print('유효하지 않은 누락\t', column_nm, '\t', total_null_cnt, '\t', last_null_cnt)
        pivoted_sampled_datas.drop(columns=column_nm, inplace=True)
    elif total_null_cnt:
        #print('유효한 누락\t', column_nm, '\t', total_null_cnt, '\t', last_null_cnt)
        for idx, row_nm in enumerate(pivoted_sampled_datas.index):
            if pivoted_sampled_datas[column_nm][row_nm] == None:
                if idx == 0:
                    pivoted_sampled_datas[column_nm][idx] = pivoted_sampled_datas[column_nm][idx+1]
                else:
                    pivoted_sampled_datas[column_nm][idx] = pivoted_sampled_datas[column_nm][idx-1]


# Batch 시작
window_size_from = 5
window_size_to = 24

pivoted_sampled_datas_last_pure_version = copy.deepcopy(pivoted_sampled_datas)
for window_size in range(window_size_from, window_size_to):

    # 테스트 초기 데이터 리셋
    pivoted_sampled_datas = copy.deepcopy(pivoted_sampled_datas_last_pure_version)

    pivoted_sampled_datas_mean = pivoted_sampled_datas.rolling(window=window_size, center=False).mean()
    pivoted_sampled_datas_std = pivoted_sampled_datas.rolling(window=window_size, center=False).std()
    pivoted_sampled_datas_cp = copy.deepcopy(pivoted_sampled_datas)
    for idx, row_nm in enumerate(pivoted_sampled_datas_cp.index):
        
        # 평균 및 표준편차 데이터가 생성되지 않는 구간 삭제
        drop_point = window_size - 1
        if idx < drop_point:
            pivoted_sampled_datas.drop(index=row_nm, inplace=True)
            pivoted_sampled_datas_mean.drop(index=row_nm, inplace=True)
            pivoted_sampled_datas_std.drop(index=row_nm, inplace=True)
        else:
            break


    # Z-Score 계산
    # 데이터의 변화가 없어 표준편차가 0(ZeroDivisionError)인 경우 때문에 DataFrame을 이용한 연산처리 불가
    pivoted_sampled_datas_zscore = copy.deepcopy(pivoted_sampled_datas)
    for column_nm in pivoted_sampled_datas_zscore.columns:
        for row_nm in pivoted_sampled_datas_zscore.index:
            try:
                pivoted_sampled_datas_zscore[column_nm][row_nm] = (pivoted_sampled_datas[column_nm][row_nm] - pivoted_sampled_datas_mean[column_nm][row_nm]) / pivoted_sampled_datas_std[column_nm][row_nm]
            except ZeroDivisionError:
                pivoted_sampled_datas_zscore[column_nm][row_nm] = 0.0



    # 시뮬레이션 로직
    if 1:
        profit_calc_start_time = datetime.strptime('2012-01-01', '%Y-%m-%d').date()
        min_max_check_term = 8
        weight_check_term = 4
        target_index_nm_list = ["MSCI ACWI","MSCI World","MSCI EM","KOSPI","S&P500","Nikkei225","상해종합"]
        
        model_cumulated_profit = {}
        bm_cumulated_profit = {}
        for index_nm in target_index_nm_list:
            # 누적 수익률 저장 공간
            model_cumulated_profit[index_nm] = {}
            bm_cumulated_profit[index_nm] = {}

            for column_nm in pivoted_sampled_datas_zscore.columns:

                if column_nm != index_nm:
                    # 누적 수익률 초기화
                    model_cumulated_profit[index_nm][column_nm] = 0.0
                    bm_cumulated_profit[index_nm][column_nm] = 0.0

                    # 주식 Buy, Sell 포지션 판단
                    new_point = min_max_check_term - 1
                    average_array = [0] * min_max_check_term
                    for idx, row_nm in enumerate(pivoted_sampled_datas_zscore.index):
                        try:
                            # 과거 moving average 생성 및 시프트
                            # min_max_check_term 개수 만큼 raw 데이터가 생겨야 average 생성 가능
                            if idx >= new_point:
                                average_array[:new_point] = average_array[-new_point:]
                                average_array[new_point] = (pivoted_sampled_datas_zscore[column_nm][idx - new_point:idx + 1].min() + pivoted_sampled_datas_zscore[column_nm][idx - new_point:idx + 1].max()) / 2

                                # 수익률 계산 시작
                                # weight_check_term 개수 만큼 average 데이터가 생겨야 노이즈 검증 가능
                                if datetime.strptime(row_nm, '%Y-%m-%d').date() >= profit_calc_start_time and idx - new_point >= weight_check_term:
                                    if average_array[new_point] == max(average_array):
                                        model_cumulated_profit[index_nm][column_nm] += pivoted_sampled_datas[index_nm][idx+1] / pivoted_sampled_datas[index_nm][idx] - 1
                                    bm_cumulated_profit[index_nm][column_nm] += pivoted_sampled_datas[index_nm][idx + 1] / pivoted_sampled_datas[index_nm][idx] - 1
                                    #print(index_nm, '\t', column_nm, '\t', row_nm, '\t', model_cumulated_profit, '\t', bm_cumulated_profit)
                        except IndexError:
                            #print("IndexError:\t", index_nm, '\t', column_nm, '\t', row_nm)
                            pass

                    # 모델의 성능이 BM 보다 좋은 팩터 결과만 출력
                    if model_cumulated_profit[index_nm][column_nm] > bm_cumulated_profit[index_nm][column_nm]:
                        print (window_size, '\t', index_nm, '\t', column_nm, '\t', model_cumulated_profit[index_nm][column_nm], '\t', bm_cumulated_profit[index_nm][column_nm], '\t', model_cumulated_profit[index_nm][column_nm]/bm_cumulated_profit[index_nm][column_nm])


    # 데이터 저장
    if 0:
        file_nm = 'test.xlsx'
        # 만약 해당 파일이 존재하지 않는 경우 생성
        workbook = xlsxwriter.Workbook(file_nm)
        workbook.close()

        # Create a Pandas Excel writer using Openpyxl as the engine.
        writer = pd.ExcelWriter(file_nm, engine='openpyxl')
        # 주의: 파일이 암호화 걸리면 workbook load시 에러 발생
        writer.book = openpyxl.load_workbook(file_nm)
        # Pandas의 DataFrame 클래스를 그대로 이용해서 엑셀 생성 가능
        pivoted_sampled_datas.to_excel(writer, sheet_name='pivoted_sampled_datas')
        pivoted_sampled_datas_mean.to_excel(writer, sheet_name='pivoted_sampled_datas_maen')
        pivoted_sampled_datas_std.to_excel(writer, sheet_name='pivoted_sampled_datas_std')
        pivoted_sampled_datas_zscore.to_excel(writer, sheet_name='pivoted_sampled_datas_zscore')
        #pivoted_reference_datas.to_excel(writer, sheet_name='pivoted_reference_datas')
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



