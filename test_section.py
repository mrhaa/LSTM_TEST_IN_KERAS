#_*_ coding: utf-8 _*_

from Test_MariaDB import WrapDB
from Wrap_Folione import Preprocess
import Wrap_Util
import pandas as pd
import statsmodels.api as sm
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import numpy as np
import json
import sys
import math
import copy
import warnings
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta


warnings.filterwarnings("ignore")

PREPROCESS = False

# Wrap운용팀 DB Connect
db = WrapDB()
db.connet(host="127.0.0.1", port=3306, database="WrapDB_1", user="root", password="ryumaria")

if PREPROCESS == True:

    datas = db.select_query("SELECT a.nm, b.date, b.value "
                            "  FROM item a, value b "
                            " WHERE a.cd = b.item_cd and a.nm in ('MSCI World', 'MSCI EM', 'KOSPI', 'S&P500', '상해종합','STOXX50','WTI 유가','금')")
    datas.columns = ['아이템명', '날짜', '값']

if PREPROCESS == True:
    print("구간 분류을 위한 데이터 구성('MSCI World', 'MSCI EM', 'KOSPI', 'S&P500', '상해종합','STOXX50','WTI 유가','금')")

    columns = datas.columns[0]  # '아이템명'
    index = datas.columns[1]    # '날짜'
    values = datas.columns[2]   # '값'
    
    # date type의 날짜 속성 추가
    datas["날짜T"] = datas[index].apply(lambda x: pd.to_datetime(str(x), format="%Y-%m-%d"))

    # Sampling 방법에 따른 데이터 누락을 위해 ref_data 생성
    reference_list = datas.resample('D', on="날짜T", convention="end")
    reference_datas = datas.loc[datas["날짜T"].isin(list(reference_list.indices))]
    pivoted_reference_datas = reference_datas.pivot(index=index, columns=columns, values=values)
    
    # sampling type
    # 'B': business daily, 'D': calendar daily, 'W': weekly, 'M': monthly
    sampling_list = datas.resample('D', on="날짜T", convention="end")
    sampled_datas = datas.loc[datas["날짜T"].isin(list(sampling_list.indices))]
    pivoted_sampled_datas = sampled_datas.pivot(index=index, columns=columns, values=values)

if PREPROCESS == True:
    print("구간분류을 위해 데이터 전처리(공휴일 채움)")
    pivoted_full_sampled_datas = copy.deepcopy(pivoted_sampled_datas)

    look_back_days = 20

    for column_nm in pivoted_sampled_datas.columns:
        for row_nm in pivoted_sampled_datas.index:

            #if datetime.strptime(row_nm, '%Y-%m-%d').date() <= datetime.strptime('1991-01-01', '%Y-%m-%d').date():
            #    continue

            # Debug
            #if row_nm == '1996-02-26' and column_nm == '상해종합':
            #   print(1)

            if math.isnan(pivoted_sampled_datas[column_nm][row_nm]) == True:
                # print (column_nm, "\t", row_nm, "\t", pivoted_sampled_datas[column_nm][row_nm])

                # ref_row_nm = copy.copy(row_nm)
                ref_row_nm = row_nm

                # 해당일에 데이터가 없는 경우 가장 최근 값을 대신 사용함
                for loop_cnt in range(look_back_days):
                    try:
                        if math.isnan(pivoted_reference_datas[column_nm][ref_row_nm]) == True:
                            # print("No Data", str(ref_row_nm))
                            ref_row_nm = str(datetime.strptime(ref_row_nm, '%Y-%m-%d').date() - timedelta(days=1))
                        else:
                            pivoted_full_sampled_datas[column_nm][row_nm] = pivoted_reference_datas[column_nm][ref_row_nm]
                            break
                    except KeyError:
                        # print("KeyError", str(ref_row_nm))
                        ref_row_nm = str(datetime.strptime(ref_row_nm, '%Y-%m-%d').date() - timedelta(days=1))


            # 이후 연산작업을 위해 decimal을 float 형태로 변경
            if math.isnan(pivoted_full_sampled_datas[column_nm][row_nm]) == False:
                pivoted_full_sampled_datas[column_nm][row_nm] = float(pivoted_full_sampled_datas[column_nm][row_nm])

    # 엑셀에서 제공하는 날짜를 숫자로 변경하는 로직 적용.
    pivoted_full_sampled_datas["날짜_INT"] = [(pd.to_datetime(str(index_t), format="%Y-%m-%d") - pd.to_datetime("1900-01-01", format="%Y-%m-%d")).days + 2 for index_t in pivoted_full_sampled_datas.index]

pre_term = 0
post_term = 60  # 본인 포함
if PREPROCESS == True:
    print("구간분류를 위해 Slope 계산(전: %s, 후: %s)" % (pre_term, post_term))

    pivoted_slope_sampled_datas = copy.deepcopy(pivoted_full_sampled_datas)
    #pivoted_slope_sampled_datas["기간"] = [day_t.days for day_t in pivoted_slope_sampled_datas["날짜T"].diff()]
    #Y = pivoted_full_sampled_datas['KOSPI'].pct_change()[-100:]

    for column in pivoted_full_sampled_datas.columns:
        print("구간분류 Slope 계산(%s)" %(column))
        if column == '날짜_INT':
            continue

        for idx, row in enumerate(pivoted_full_sampled_datas.index):

            # debug
            #if row == '1990-03-05':
            #    print(1)

            if idx < pre_term:
                pivoted_slope_sampled_datas[column][row] = math.nan
            elif idx + post_term > len(pivoted_full_sampled_datas.index):
                pivoted_slope_sampled_datas[column][row] = math.nan
            else:

                # 엑셀에서 제공하는 날짜를 숫자로 변경하는 로직 적용.
                if 0:
                    x_axis = [val for val in pivoted_full_sampled_datas["날짜_INT"][idx - pre_term:idx + post_term]] # X축 정규화 없음
                # x축의 시작점을 1000으로 설정
                else:
                    x_axis = [val / pivoted_full_sampled_datas["날짜_INT"][idx - pre_term:idx + post_term].min() * 1000 for val in pivoted_full_sampled_datas["날짜_INT"][idx - pre_term:idx + post_term]]
                y_axis = [val for val in pivoted_full_sampled_datas[column][idx - pre_term:idx + post_term]]
                slope = Wrap_Util.GetSlope(x_axis, y_axis)

                pivoted_slope_sampled_datas[column][row] = slope if math.isnan(slope) == False else math.nan

    Wrap_Util.SavePickleFile(file='.\\sections\\test_section_pivoted_full_sampled_datas_preterm(%s)_postterm(%s).pickle' % (pre_term, post_term), obj=pivoted_full_sampled_datas)
    Wrap_Util.SavePickleFile(file='.\\sections\\test_section_pivoted_slope_sampled_datas_preterm(%s)_postterm(%s).pickle' % (pre_term, post_term), obj=pivoted_slope_sampled_datas)

    Wrap_Util.SaveExcelFiles(file='.\\sections\\구간분류(지수별)_RawData(%s~%s %s&%s).xlsx' % (pivoted_sampled_datas.index.min(), pivoted_sampled_datas.index.max(), pre_term, post_term)
                             , obj_dict={'pivoted_reference_datas': pivoted_reference_datas,'pivoted_sampled_datas': pivoted_sampled_datas
                                        ,'pivoted_full_sampled_datas': pivoted_full_sampled_datas, 'pivoted_slope_sampled_datas': pivoted_slope_sampled_datas})
else:
    pivoted_full_sampled_datas = Wrap_Util.ReadPickleFile(file='.\\sections\\test_section_pivoted_full_sampled_datas_preterm(%s)_postterm(%s).pickle' % (pre_term, post_term))
    pivoted_slope_sampled_datas = Wrap_Util.ReadPickleFile(file='.\\sections\\test_section_pivoted_slope_sampled_datas_preterm(%s)_postterm(%s).pickle' % (pre_term, post_term))

if PREPROCESS == True:
    print("구간분류(상승 & 하락)")
    pivoted_section_sampled_datas = copy.deepcopy(pivoted_slope_sampled_datas)

    sections = {}

    for column in pivoted_slope_sampled_datas.columns:
        print("구간분류(%s)" % (column))
        try:
            # 기울기를 구하기 위한 참고 데이터 컬럼
            if column == '날짜_INT':
                continue

            sections[column] = {'up': {}, "down": {}}

            up_count = 0
            down_count = 0

            start_value = 0 # 구간의 시작점을 설정 가능한지 확인하는 FLAG
            start_flag = False # 구간분류를 하기 위한 데이터 존재 여부를 확인하는 FLAG
            for idx, row in enumerate(pivoted_slope_sampled_datas.index):

                # 데이터가 없는 구간 PASS
                if start_flag == False:
                    if math.isnan(pivoted_slope_sampled_datas[column][row]) == True:
                        continue
                    else:
                        start_flag = True
                
                # 구간을 처음 설정하는 시점
                if start_value == 0:
                    start_value = pivoted_slope_sampled_datas[column][row]
                    
                    # 기울기 값이 + 이면 상승구간
                    if start_value > 0:
                        sections[column]['up'][up_count] = {}
                        sections[column]['up'][up_count]['start_dt'] = row
                    # 기울기 값이 - 이면 하락구간
                    elif start_value < 0:
                        sections[column]['down'][down_count] = {}
                        sections[column]['down'][down_count]['start_dt'] = row

                #마지막 데이터는 직전구간으로 완료 시킨다.
                if idx+1 == len(pivoted_slope_sampled_datas.index) or math.isnan(pivoted_slope_sampled_datas[column][idx+1]) == True:
                    # 상승구간으로 끝
                    if pivoted_slope_sampled_datas[column][idx] > 0:
                        sections[column]['up'][up_count]['end_dt'] = row
                    # 하락구간으로 끝
                    elif pivoted_slope_sampled_datas[column][idx] < 0:
                        sections[column]['down'][down_count]['end_dt'] = row

                    break

                # 다음 데이터의 기울기가 바뀌면 구간을 완료시킨다.
                if start_value != 0 and ((start_value > 0 and pivoted_slope_sampled_datas[column][idx+1] < 0) or (start_value < 0 and pivoted_slope_sampled_datas[column][idx+1] > 0)):
                    if start_value > 0 and pivoted_slope_sampled_datas[column][idx+1] < 0:
                        sections[column]['up'][up_count]['end_dt'] = row
                        up_count += 1
                    elif start_value < 0 and pivoted_slope_sampled_datas[column][idx+1] > 0:
                        sections[column]['down'][down_count]['end_dt'] = row
                        down_count += 1
                    
                    # 초기값을 0으로 변경
                    start_value = 0
                    
        except TypeError:
            print("TypeError:", sys.exc_info(), column, row)
        except KeyError:
            print("KeyError:", sys.exc_info(), column, row)

    Wrap_Util.SavePickleFile(file='.\\sections\\test_sections_preterm(%s)_postterm(%s).pickle' % (pre_term, post_term), obj=sections)

    pd.read_json(json.dumps(sections)).to_excel(".\\sections\\test_sections_preterm(%s)_postterm(%s).xlsx" % (pre_term, post_term))
else:
    sections = Wrap_Util.ReadPickleFile(file='.\\sections\\test_sections_preterm(%s)_postterm(%s).pickle' % (pre_term, post_term))


simulation_end_date = '2019-02-28'
simulation_term_type = 1
if PREPROCESS == True:

    # Simulation 기간 타입
    # 1: 장기, 2: 중기, 3: 단기
    # 장기: 2001-01-01 부터 (IT 버블 시점), 데이터는 pivoted_sampled_datas의 기간과 연동(223 Factors)
    # 중기: 2007-01-01 부터 (금융위기 시점), 데이터는 pivoted_sampled_datas의 기간과 연동(274 Factors)
    # 단기: 2012-01-01 부터 (QE 시작 시점), 데이터는 pivoted_sampled_datas의 기간과 연동(315 Factors)
    if simulation_term_type == 1:
        simulation_start_date = '2001-01-01'
    elif simulation_term_type == 2:
        simulation_start_date = '2007-01-01'
    elif simulation_term_type == 3:
        simulation_start_date = '2012-01-01'


    # Z-Score 생성의 경우 과거 추가 기간이 필요함.
    # Z-Score의 최대 기간과 동일 (월 단위)
    raw_data_spare_term = 36


    # 데이터 전처리 instance 생성
    preprocess = Preprocess()

    # 데이터 Info Read
    data_info = db.get_data_info()
    preprocess.SetDataInfo(data_info=data_info, data_info_columns=["아이템코드", "아이템명", "개수", "시작일", "마지막일"])

    # Factor 별 데이터가 존재하는 기간 설정
    preprocess.MakeDateRange(start_date="시작일", last_date="마지막일")

    # 유효기간 내 데이터 Read
    # raw 데이터의 기간이 체계가 없어 return 받은 start_date, end_date을 사용할 수 없다.
    data_list, start_date, end_date = preprocess.GetDataList(item_cd="아이템코드", start_date="시작일",
                                                             last_date="마지막일")
    datas = db.get_bloomberg_datas(data_list=data_list, start_date=None, end_date=None)
    preprocess.SetDatas(datas=datas, datas_columns=["아이템코드", "아이템명", "날짜", "값"])

    # DataFrame 형태의 Sampled Data 생성
    # 'B': business daily, 'D': calendar daily, 'W': weekly, 'M': monthly
    preprocess.MakeSampledDatas(sampling_type='M', index='날짜', columns='아이템명', values='값')

    # 유효한 데이터 가장 최근 값으로 채움
    preprocess.FillValidData(look_back_days=20)

    # 유효하지 않은 기간의 데이터 삭제
    # drop_basis_from: '2007-01-31', drop_basis_to: '가장 최근 말일'는 가장 유효한 factor를 많이 사용할 수 있는 기간을 찾아 적용하였음.
    # pivoted_sampled_datas = preprocess.DropInvalidData(drop_basis_from='2001-01-01', drop_basis_to='2018-03-31')
    #print("simulation_start_date: ", simulation_start_date, str(datetime.strptime(simulation_start_date, '%Y-%m-%d').date() - relativedelta(months=raw_data_spare_term)))
    pivoted_sampled_factor_datas = preprocess.DropInvalidData(drop_basis_from=str(datetime.strptime(simulation_start_date, '%Y-%m-%d').date() - relativedelta(months=raw_data_spare_term)), drop_basis_to=simulation_end_date)

    # 전달 대비 변동률
    pivoted_sampled_factor_datas_mom = copy.deepcopy(pivoted_sampled_factor_datas)
    for column in pivoted_sampled_factor_datas.columns:
        for row_idx, row in enumerate(pivoted_sampled_factor_datas.index):

            if row_idx >= 1:
                try:
                    pivoted_sampled_factor_datas_mom[column][row] = pivoted_sampled_factor_datas[column][row_idx] / pivoted_sampled_factor_datas[column][row_idx - 1] - 1
                except ZeroDivisionError:
                    pivoted_sampled_factor_datas_mom[column][row] = pivoted_sampled_factor_datas[column][row_idx] - pivoted_sampled_factor_datas[column][row_idx - 1]
                    print("TypeError:", "\t", sys.exc_info(), "\t", "MoM: ", "\t", column, "\t", row)
            else:
                pivoted_sampled_factor_datas_mom[column][row] = math.nan

    # 전년 대비 변동률
    pivoted_sampled_factor_datas_yoy = copy.deepcopy(pivoted_sampled_factor_datas)
    for column in pivoted_sampled_factor_datas.columns:
        for row_idx, row in enumerate(pivoted_sampled_factor_datas.index):

            if row_idx >= 12:
                try:
                    pivoted_sampled_factor_datas_yoy[column][row] = pivoted_sampled_factor_datas[column][row_idx] / pivoted_sampled_factor_datas[column][row_idx - 12] - 1
                except ZeroDivisionError:
                    pivoted_sampled_factor_datas_yoy[column][row] = pivoted_sampled_factor_datas[column][row_idx] - pivoted_sampled_factor_datas[column][row_idx - 12]
                    print("TypeError:", "\t", sys.exc_info(), "\t", "YoY: ", "\t", column, "\t", row)
            else:
                pivoted_sampled_factor_datas_yoy[column][row] = math.nan

    Wrap_Util.SaveExcelFiles(file='.\\sections\\test_section_pivoted_sampled_datas_simulation_term_type_%s_target_date_%s.xlsx' % (simulation_term_type, simulation_end_date)
                             , obj_dict={'pivoted_sampled_factor_datas': pivoted_sampled_factor_datas, 'pivoted_sampled_factor_datas_mom': pivoted_sampled_factor_datas_mom
                                        , 'pivoted_sampled_factor_datas_yoy': pivoted_sampled_factor_datas_yoy})

    Wrap_Util.SavePickleFile(file='.\\sections\\test_section_pivoted_sampled_datas_simulation_term_type_%s_target_date_%s.pickle' % (simulation_term_type, simulation_end_date), obj=pivoted_sampled_factor_datas)
    Wrap_Util.SavePickleFile(file='.\\sections\\test_section_pivoted_sampled_datas_mom_simulation_term_type_%s_target_date_%s.pickle' % (simulation_term_type, simulation_end_date), obj=pivoted_sampled_factor_datas_mom)
    Wrap_Util.SavePickleFile(file='.\\sections\\test_section_pivoted_sampled_datas_yoy_simulation_term_type_%s_target_date_%s.pickle' % (simulation_term_type, simulation_end_date), obj=pivoted_sampled_factor_datas_mom)
else:
    pivoted_sampled_factor_datas = Wrap_Util.ReadPickleFile(file='.\\sections\\test_section_pivoted_sampled_datas_simulation_term_type_%s_target_date_%s.pickle' % (simulation_term_type, simulation_end_date))
    pivoted_sampled_factor_datas_mom = Wrap_Util.ReadPickleFile(file='.\\sections\\test_section_pivoted_sampled_datas_mom_simulation_term_type_%s_target_date_%s.pickle' % (simulation_term_type, simulation_end_date))
    pivoted_sampled_factor_datas_yoy = Wrap_Util.ReadPickleFile(file='.\\sections\\test_section_pivoted_sampled_datas_yoy_simulation_term_type_%s_target_date_%s.pickle' % (simulation_term_type, simulation_end_date))


up_case_mom = {}
up_case_yoy = {}
down_case_mom = {}
down_case_yoy = {}
up_case_mom_clean = {}
up_case_yoy_clean = {}
down_case_mom_clean = {}
down_case_yoy_clean = {}
if PREPROCESS == True:

    for index in sections:

        # debug
        #if index != 'KOSPI':
        #    break

        # 상승구간 케이스
        up_case_mom[index] = copy.deepcopy(pivoted_sampled_factor_datas_mom)
        up_case_yoy[index] = copy.deepcopy(pivoted_sampled_factor_datas_yoy)
        print(index + "의 상승구간")
        
        for column in up_case_mom[index].columns:
            for row in up_case_mom[index].index:
                if math.isnan(up_case_mom[index][column][row]) == False:
                    curr_dt = pd.to_datetime(row, format="%Y-%m-%d")

                    is_included = False
                    for section in sections[index]['up']:
                        try:
                            # 현재 마지막 구간은 end_dt가 없음
                            if len(sections[index]['up'][section]) == 2:
                                start_dt = pd.to_datetime(sections[index]['up'][section]['start_dt'], format="%Y-%m-%d")
                                end_dt = pd.to_datetime(sections[index]['up'][section]['end_dt'], format="%Y-%m-%d")

                                if curr_dt >= start_dt and curr_dt <= end_dt:
                                    is_included = True
                                    break
                        except KeyError:
                            print("Unexpected error:", sys.exc_info())


                    if is_included == False:
                        up_case_mom[index][column][row] = math.nan
                        up_case_yoy[index][column][row] = math.nan

        Wrap_Util.SavePickleFile(file='.\\sections\\test_up_case_mom_%s.pickle' % (index), obj=up_case_mom[index])
        Wrap_Util.SavePickleFile(file='.\\sections\\test_up_case_yoy_%s.pickle' % (index), obj=up_case_yoy[index])

        # 하락구간 케이스
        down_case_mom[index] = copy.deepcopy(pivoted_sampled_factor_datas_mom)
        down_case_yoy[index] = copy.deepcopy(pivoted_sampled_factor_datas_yoy)
        print(index + "의 하락구간")

        for column in down_case_mom[index].columns:
            for row in down_case_mom[index].index:
                if math.isnan(down_case_mom[index][column][row]) == False:
                    curr_dt = pd.to_datetime(row, format="%Y-%m-%d")

                    is_included = False
                    for section in sections[index]['down']:
                        try:
                            # 현재 마지막 구간은 end_dt가 없음
                            if len(sections[index]['up'][section]) == 2:
                                start_dt = pd.to_datetime(sections[index]['down'][section]['start_dt'], format="%Y-%m-%d")
                                end_dt = pd.to_datetime(sections[index]['down'][section]['end_dt'], format="%Y-%m-%d")

                                if curr_dt >= start_dt and curr_dt <= end_dt:
                                    is_included = True
                                    break
                                    
                        except KeyError:
                            print("Unexpected error:", sys.exc_info())

                    if is_included == False:
                        down_case_mom[index][column][row] = math.nan
                        down_case_yoy[index][column][row] = math.nan

        Wrap_Util.SavePickleFile(file='.\\sections\\test_down_case_mom_%s.pickle' % (index), obj=down_case_mom[index])
        Wrap_Util.SavePickleFile(file='.\\sections\\test_down_case_yoy_%s.pickle' % (index), obj=down_case_yoy[index])

    Wrap_Util.SaveExcelFiles(file='.\\sections\\test_up_case_mom.xlsx', obj_dict=up_case_mom)
    Wrap_Util.SaveExcelFiles(file='.\\sections\\test_up_case_yoy.xlsx', obj_dict=up_case_yoy)
    Wrap_Util.SaveExcelFiles(file='.\\sections\\test_down_case_mom.xlsx', obj_dict=down_case_mom)
    Wrap_Util.SaveExcelFiles(file='.\\sections\\test_down_case_yoy.xlsx', obj_dict=down_case_yoy)

    up_case_mom_clean = copy.deepcopy(up_case_mom)
    up_case_yoy_clean = copy.deepcopy(up_case_yoy)
    down_case_mom_clean = copy.deepcopy(down_case_mom)
    down_case_yoy_clean = copy.deepcopy(down_case_yoy)
    for index in sections:
        for row in up_case_mom[index].index:
            if up_case_mom[index].transpose()[row].sum() == 0.0:
                up_case_mom_clean[index].drop(index=row, inplace=True)
                up_case_yoy_clean[index].drop(index=row, inplace=True)

        Wrap_Util.SavePickleFile(file='.\\sections\\test_up_case_mom_clean_%s.pickle' % (index), obj=up_case_mom_clean[index])
        Wrap_Util.SavePickleFile(file='.\\sections\\test_up_case_yoy_clean_%s.pickle' % (index), obj=up_case_yoy_clean[index])

        for row in down_case_mom[index].index:
            if down_case_mom[index].transpose()[row].sum() == 0.0:
                down_case_mom_clean[index].drop(index=row, inplace=True)
                down_case_yoy_clean[index].drop(index=row, inplace=True)

        Wrap_Util.SavePickleFile(file='.\\sections\\test_down_case_mom_clean_%s.pickle' % (index), obj=down_case_mom_clean[index])
        Wrap_Util.SavePickleFile(file='.\\sections\\test_down_case_yoy_clean_%s.pickle' % (index), obj=down_case_yoy_clean[index])

    Wrap_Util.SaveExcelFiles(file='.\\sections\\test_up_case_mom_clean.xlsx', obj_dict=up_case_mom_clean)
    Wrap_Util.SaveExcelFiles(file='.\\sections\\test_up_case_yoy_clean.xlsx', obj_dict=up_case_yoy_clean)
    Wrap_Util.SaveExcelFiles(file='.\\sections\\test_down_case_mom_clean.xlsx', obj_dict=down_case_mom_clean)
    Wrap_Util.SaveExcelFiles(file='.\\sections\\test_down_case_yoy_clean.xlsx', obj_dict=down_case_yoy_clean)

else:

    for index in sections:
        up_case_mom[index] = Wrap_Util.ReadPickleFile(file='.\\sections\\test_up_case_mom_%s.pickle' % (index))
        up_case_yoy[index] = Wrap_Util.ReadPickleFile(file='.\\sections\\test_up_case_yoy_%s.pickle' % (index))
        down_case_mom[index] = Wrap_Util.ReadPickleFile(file='.\\sections\\test_down_case_mom_%s.pickle' % (index))
        down_case_yoy[index] = Wrap_Util.ReadPickleFile(file='.\\sections\\test_down_case_yoy_%s.pickle' % (index))

        up_case_mom_clean[index] = Wrap_Util.ReadPickleFile(file='.\\sections\\test_up_case_mom_clean_%s.pickle' % (index))
        up_case_yoy_clean[index] = Wrap_Util.ReadPickleFile(file='.\\sections\\test_up_case_yoy_clean_%s.pickle' % (index))
        down_case_mom_clean[index] = Wrap_Util.ReadPickleFile(file='.\\sections\\test_down_case_mom_clean_%s.pickle' % (index))
        down_case_yoy_clean[index] = Wrap_Util.ReadPickleFile(file='.\\sections\\test_down_case_yoy_clean_%s.pickle' % (index))


    for index in sections:
        means = up_case_mom_clean[index].transpose().mean()
        stds = up_case_mom_clean[index].transpose().std()
        for term in up_case_mom_clean[index].transpose().columns:
            for factor in up_case_mom_clean[index].transpose().index:
                lower_barrier = means[term] - 2 * stds[term]
                upper_barrier = means[term] + 2 * stds[term]
                if up_case_mom_clean[index].transpose()[term][factor] > upper_barrier:
                    print("UPCASE MoM UPPER CASE:\t", index, "\t", term, "\t", factor, "\t", str(up_case_mom_clean[index].transpose()[term][factor]))
                    up_case_mom_clean[index].transpose()[term][factor] = upper_barrier

                if up_case_mom_clean[index].transpose()[term][factor] < lower_barrier:
                    print("UPCASE MoM LOWER CASE:\t", index, "\t", term, "\t", factor, "\t", str(up_case_mom_clean[index].transpose()[term][factor]))
                    up_case_mom_clean[index].transpose()[term][factor] = lower_barrier

    for index in sections:
        means = up_case_yoy_clean[index].transpose().mean()
        stds = up_case_yoy_clean[index].transpose().std()
        for term in up_case_yoy_clean[index].transpose().columns:
            for factor in up_case_yoy_clean[index].transpose().index:
                lower_barrier = means[term] - 2 * stds[term]
                upper_barrier = means[term] + 2 * stds[term]
                if up_case_yoy_clean[index].transpose()[term][factor] > upper_barrier:
                    print("UPCASE YoY UPPER CASE:\t", index, "\t", term, "\t", factor, "\t", str(up_case_yoy_clean[index].transpose()[term][factor]))
                    up_case_yoy_clean[index].transpose()[term][factor] = upper_barrier

                if up_case_yoy_clean[index].transpose()[term][factor] < lower_barrier:
                    print("UPCASE YoY LOWER CASE:\t", index, "\t", term, "\t", factor, "\t", str(up_case_yoy_clean[index].transpose()[term][factor]))
                    up_case_yoy_clean[index].transpose()[term][factor] = lower_barrier

    for index in sections:
        means = down_case_mom_clean[index].transpose().mean()
        stds = down_case_mom_clean[index].transpose().std()
        for term in down_case_mom_clean[index].transpose().columns:
            for factor in down_case_mom_clean[index].transpose().index:
                lower_barrier = means[term] - 2 * stds[term]
                upper_barrier = means[term] + 2 * stds[term]
                if down_case_mom_clean[index].transpose()[term][factor] > upper_barrier:
                    print("DOWNCASE MoM UPPER CASE:\t", index, "\t", term, "\t", factor, "\t", str(down_case_mom_clean[index].transpose()[term][factor]))
                    down_case_mom_clean[index].transpose()[term][factor] = upper_barrier

                if down_case_mom_clean[index].transpose()[term][factor] < lower_barrier:
                    print("DOWNCASE MoM LOWER CASE:\t", index, "\t", term, "\t", factor, "\t", str(down_case_mom_clean[index].transpose()[term][factor]))
                    down_case_mom_clean[index].transpose()[term][factor] = lower_barrier

    for index in sections:
        means = down_case_yoy_clean[index].transpose().mean()
        stds = down_case_yoy_clean[index].transpose().std()
        for term in down_case_yoy_clean[index].transpose().columns:
            for factor in down_case_yoy_clean[index].transpose().index:
                lower_barrier = means[term] - 2 * stds[term]
                upper_barrier = means[term] + 2 * stds[term]
                if down_case_yoy_clean[index].transpose()[term][factor] > upper_barrier:
                    print("DOWNCASE YoY UPPER CASE:\t", index, "\t", term, "\t", factor, "\t", str(down_case_yoy_clean[index].transpose()[term][factor]))
                    down_case_yoy_clean[index].transpose()[term][factor] = upper_barrier

                if down_case_yoy_clean[index].transpose()[term][factor] < lower_barrier:
                    print("DOWNCASE YoY LOWER CASE:\t", index, "\t", term, "\t", factor, "\t", str(down_case_yoy_clean[index].transpose()[term][factor]))
                    down_case_yoy_clean[index].transpose()[term][factor] = lower_barrier





if 1:
    for index in sections:
        mom_avg = up_case_mom_clean[index].mean()
        yoy_avg = up_case_yoy_clean[index].mean()

        # TO DO
        # 변화율이기 때문에 전달 값이 0인 경우 inf가 저장된다.
        # Y축은 전년대비 값을 적용, X축은 전달대비 값을 적용
        for idx, val in enumerate(mom_avg):
            if math.isinf(val) == True or math.isnan(val) == True:
                avg[idx] = 0
        #model = KMeans(init="k-means++", n_clusters=2, random_state=0).fit_predict(avg.transpose().to_frame())
        #model = KMeans(init="k-means++", n_clusters=2, random_state=0).fit_predict(avg.transpose().to_frame().transpose().append(avg.transpose().to_frame().transpose(), ignore_index=True).transpose())


        feature = mom_avg.transpose().to_frame().transpose().append(yoy_avg.transpose().to_frame().transpose(), ignore_index=True).transpose()
        feature.columns = ['a','b']
        model = KMeans(n_clusters=2, algorithm='auto')
        model.fit(feature)
        predict = pd.DataFrame(model.predict(feature))
        predict.columns=['predict']

        r = pd.concat([feature, predict], axis=1)

        #plt.scatter(r['a'], r['b'], c=r['predict'], alpha=0.5)
        plt.plot(r['a'], r['b'], 'ro')

        centers = pd.DataFrame(model.cluster_centers_, columns=['a', 'b'])
        center_x = centers['a']
        center_y = centers['b']
        plt.scatter(center_x, center_y, s=50, marker='D', c='b')

        plt.title(index)
        plt.xlabel('mom')
        plt.ylabel('yoy')
        plt.show()

        '''
        for row in up_case_mom_clean[index].index:

            model = KMeans(init="k-means++", n_clusters=2, random_state=0)
            # 1차원
            if 1:
                model.fit(up_case_mom_clean[index].transpose()[row].to_frame())
                print(model)
            # 2차원
            else:
                model.fit(up_case_mom_clean[index].transpose()[row].to_frame().transpose().append(up_case_mom_clean[index].transpose()[row].to_frame().transpose(), ignore_index=True).transpose())

            # Visualization
            if 0:
                plt.plot(up_case_mom_clean[index].transpose()[row], up_case_mom_clean[index].transpose()[row], 'ro')
                plt.grid(True)
                plt.xlabel('Slope')
                plt.ylabel('Slope')
                plt.title(row)
                plt.show()
        '''


db.disconnect()

