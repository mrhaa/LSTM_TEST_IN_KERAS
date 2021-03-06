#_*_ coding: utf-8 _*_

#import win32com.client
from Test_MariaDB import WrapDB
import pandas as pd
import numpy as np
import copy
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import multiprocessing as mp
import time
import matplotlib.pyplot as plt

from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import Test_Figure
import Wrap_Util
from Wrap_Folione import Preprocess
from Wrap_Folione import Folione
import Wrap_Folione as wf

import sys
import os
import platform
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
base_dir = (os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
if platform.system() == 'Windows':
    pickle_dir = '%s\\pickle\\' % (base_dir)
else:
    pickle_dir = '%s/pickle/' % (base_dir)


# Folione 모델 외부(전단계)
use_datas_pickle = True # 중간 저장된 raw data 사용 여부

# Folione 작업
do_simulation = True
# Folione 모델 내부
use_window_size_pickle = True # 중간 저장된 Z-Score data 사용 여부
use_correlation_pickle = True # 중간 저장된 Correlation data 사용 여부(Target Index와 Factor간의 관계)
use_factor_selection_pickle = True
make_folione_signal = True

# 병렬처리 사용여부
use_parallel_process = True

# Debug 데이터 생성 여부
save_datas_excel = True
save_correlations_txt = False

# Signal DB 저장 여부
save_signal_process_db = True
save_signal_last_db = True

# 데이터 분석
do_pca = False

# 그래프 생성
do_figure = False



# test용 파라미터 사용 여부
is_test = False

# 과거 상황에서 Simluation을 진행하기 위해 기간을 Array로 받음.
#back_test_dates = ['2018-01-31', '2018-02-28', '2018-03-31', '2018-04-30', '2018-05-31', '2018-06-30', '2018-07-31']
back_test_dates = ['2020-09-30']

# Simulation 기간 타입
# 1: 장기, 2: 중기, 3: 단기
# 장기: 2001-01-01 부터 (IT 버블 시점), 데이터는 pivoted_sampled_datas의 기간과 연동(223 Factors)
# 중기: 2007-01-01 부터 (금융위기 시점), 데이터는 pivoted_sampled_datas의 기간과 연동(274 Factors)
# 단기: 2012-01-01 부터 (QE 시작 시점), 데이터는 pivoted_sampled_datas의 기간과 연동(315 Factors)
# 초단기: 2020-01-01 부터 (코로나 시점), 데이터는 pivoted_sampled_datas의 기간과 연동(Factors)
# 최근2년: 직전 2년 전 부터, 데이터는 pivoted_sampled_datas의 기간과 연동(Factors)
simulation_term_type = 3

# Z-Score 생성의 경우 과거 추가 기간이 필요함.
# Z-Score의 최대 기간과 동일(월 단위)
raw_data_spare_term = 36

# max correlation을 판단하기 위한 최대 lag
max_lag_term = 3

# factor 예측 모형에서 사용되는 최대 factor 갯수
max_signal_factors_num = 10

if __name__ == '__main__':

    for back_test_date in back_test_dates:
        if simulation_term_type == 1:
            simulation_start_date = '2000-12-31'
        elif  simulation_term_type == 2:
            simulation_start_date = '2006-12-31'
        elif simulation_term_type == 3:
            simulation_start_date = '2011-12-31'
        elif simulation_term_type == 4:
            simulation_start_date = '2018-12-31'
        elif simulation_term_type == 5:
            # 율달인 경우 자동처리되는 로직 없음
            simulation_start_date = str(int(back_test_date[0:4])-2) + back_test_date[4:]
        simulation_end_date = back_test_date

        if is_test == False:
            min_max_check_term = 1  # 이번값을 그대로 사용하지 않고 특정기간의 평균을 사용, 값이 커질 수록 MA효과(후행성 데이터로 변경)가 강해진다.
            weight_check_term = 2  # 이번 값이 최근 MAX 값인지 확인하는 기간
        else:
            min_max_check_term = 1  # 이번값을 그대로 사용하지 않고 특정기간의 평균을 사용, 값이 커질 수록 MA효과(후행성 데이터로 변경)가 강해진다. # TEST
            weight_check_term = 2  # 이번 값이 최근 MAX 값인지 확인하는 기간 # TEST

        # DB에서 Raw 데이터를 읽어서 전처리하는 경우
        if use_datas_pickle == False:
            # Wrap운용팀 DB Connect
            db = WrapDB()
            db.connet(host="127.0.0.1", port=3306, database="WrapDB_1", user="root", password="ryumaria")

            # 데이터 전처리 instance 생성
            preprocess = Preprocess()

            # 데이터 Info Read
            # simulation기간 + z-score를 계산할 수 있은 추가기간이 factor별 최h소 필요 데이터 수량
            data_info = db.get_data_info(min_num=int((datetime.strptime(simulation_end_date, '%Y-%m-%d')-datetime.strptime(simulation_start_date, '%Y-%m-%d')).days/30)
                                                 + raw_data_spare_term + min_max_check_term + weight_check_term + max_lag_term)
            preprocess.SetDataInfo(data_info=data_info, data_info_columns=["아이템코드", "아이템명", "개수", "시작일", "마지막일"])

            # Factor 별 데이터가 존재하는 기간 설정
            #preprocess.MakeDateRange(start_date="시작일", last_date="마지막일")

            # 유효기간 내 데이터 Read
            # raw 데이터의 기간이 체계가 없어 return 받은 start_date, end_date을 사용할 수 없다.
            #data_list, start_date, end_date = preprocess.GetDataList(item_cd="아이템코드", start_date="시작일", last_date="마지막일")
            data_list = preprocess.GetDataList(item_cd="아이템코드")
            datas = db.get_bloomberg_datas(data_list=data_list)
            preprocess.SetDatas(datas=datas, datas_columns=["아이템코드", "아이템명", "날짜", "값"])

            # DataFrame 형태의 Sampled Data 생성
            # 'B': business daily, 'D': calendar daily, 'W': weekly, 'M': monthly
            preprocess.MakeSampledDatas(sampling_type='M', index='날짜', columns='아이템명', values='값')

            # 유효한 데이터 가장 최근 값으로 채움
            # 예를 틀어 월말일이 휴일인 경우 해당 값이 비어있으면 직전영업 값을 찾아서 넣어줌.
            # 모델을 위한 데이터의 주기가 1달이기 때문에 30일까지만 look back함.
            preprocess.FillValidData(look_back_days=30)

            # 유효하지 않은 기간의 데이터 삭제
            # drop_basis_from: '2007-01-31', drop_basis_to: '가장 최근 말일'는 가장 유효한 factor를 많이 사용할 수 있는 기간을 찾아 적용하였음.
            #pivoted_sampled_datas = preprocess.DropInvalidData(drop_basis_from='2001-01-01', drop_basis_to='2018-03-31')
            print("simulation_data_period: ", str(datetime.strptime(simulation_start_date, '%Y-%m-%d').date()
                                                  - relativedelta(months=raw_data_spare_term + min_max_check_term + weight_check_term + max_lag_term))
                                            , simulation_end_date)

            # factor에 따라 발표 시점에 lag가 있어 shift가 필요한 경우들이 있음.
            # lag때문에 사용하지 못하게 되는 factor가 많기 때문에 관련 내용을 처리할 수 있도록 수정이 필요
            lag_shift_yn = True
            preprocess.DropInvalidData(drop_basis_from=str(datetime.strptime(simulation_start_date, '%Y-%m-%d').date()
                                                           - relativedelta(months=raw_data_spare_term + min_max_check_term + weight_check_term + max_lag_term))
                                    , drop_basis_to=simulation_end_date, lag_shift_yn=lag_shift_yn)
            pivoted_sampled_datas = preprocess.GetPivotedSampledDatas()
            pivoted_reference_datas = preprocess.GetPivotedReferenceDatas()
            init_pivoted_sampled_datas = preprocess.GetInitPivotedSampledDatas()
            filled_pivoted_sampled_datas = preprocess.GetFilledPivotedSampledDatas()


            Wrap_Util.SaveExcelFiles(file='%spivoted_sampled_datas_start_date_%s_end_date_%s_min_max_check_term_%s_weight_check_term_%s.xlsx'
                                          % (pickle_dir, simulation_start_date, simulation_end_date, min_max_check_term, weight_check_term)
                                     , obj_dict={'pivoted_reference_datas': pivoted_reference_datas, 'init_pivoted_sampled_datas': init_pivoted_sampled_datas
                                                , 'filled_pivoted_sampled_datas': filled_pivoted_sampled_datas, 'pivoted_sampled_datas': pivoted_sampled_datas})
            Wrap_Util.SaveCSVFiles(file='%spivoted_sampled_datas_start_date_%s_end_date_%s_min_max_check_term_%s_weight_check_term_%s.csv'
                                        % (pickle_dir, simulation_start_date, simulation_end_date, min_max_check_term, weight_check_term), obj=pivoted_sampled_datas)
            Wrap_Util.SavePickleFile(file='%spivoted_sampled_datas_start_date_%s_end_date_%s_min_max_check_term_%s_weight_check_term_%s.pickle'
                                          % (pickle_dir, simulation_start_date, simulation_end_date, min_max_check_term, weight_check_term), obj=pivoted_sampled_datas)
        else:
            pivoted_sampled_datas = Wrap_Util.ReadPickleFile(file='%spivoted_sampled_datas_start_date_%s_end_date_%s_min_max_check_term_%s_weight_check_term_%s.pickle'
                                                                  % (pickle_dir, simulation_start_date, simulation_end_date, min_max_check_term, weight_check_term))


        if do_simulation == True:
            # Batch 시작
            # Z-Score 생성 기간(18개월 ~ ), 최대 기간은 raw_data_spare_term과 동일
            # 실험적으로 24개월보다 기간이 Window기간이 짧은 경우 Z-Score의 통계적 신뢰성이 떨어진다.
            # Correlation이 불안정함(+, - 반복)
            if is_test == False:
                window_sizes = {"from": 12, "to": raw_data_spare_term}
            else:
                window_sizes = {"from": 27, "to": 27}
                window_sizes = {"from": 12, "to": raw_data_spare_term}
            profit_calc_start_date = simulation_start_date
            profit_calc_end_date = simulation_end_date

            if is_test == False:
                #target_index_nm_list = ["MSCI World", "MSCI EM", "KOSPI", "S&P500", "상해종합","STOXX50","WTI 유가","금"]
                target_index_nm_list = ["KOSPI", "S&P500", "상해종합", "STOXX50"]
            else:
                target_index_nm_list = ["KOSPI"]
                target_index_nm_list = ["KOSPI", "S&P500", "상해종합", "STOXX50"]

            # 병렬처리 시 실행되는 프로세스의 최대 갯수
            max_proces_num = 8
            window_step = 3
            jobs = []
            pivoted_sampled_datas_last_pure_version = copy.deepcopy(pivoted_sampled_datas)
            for window_size in range(window_sizes["from"], window_sizes["to"] + 1, window_step):
                for target_index_nm in target_index_nm_list:
                    folione = Folione(pivoted_sampled_datas_last_pure_version, window_size, simulation_term_type
                                      , profit_calc_start_date, profit_calc_end_date, min_max_check_term, weight_check_term, max_lag_term, max_signal_factors_num, target_index_nm
                                      , use_window_size_pickle, use_factor_selection_pickle, use_correlation_pickle
                                      , make_folione_signal
                                      , save_datas_excel, save_correlations_txt, save_signal_process_db, save_signal_last_db, use_parallel_process)

                    if use_parallel_process == True:
                        # 신규 프로세스 생성
                        p = mp.Process(target=wf.FolioneStart, args=(folione,))
                        jobs.append(p)
                        p.start()

                        # 최대 Process 갯수를 넘으면 대기
                        while len(jobs) >= max_proces_num:
                            jobs = [job for job in jobs if job.is_alive()]
                            print("%s Process Left" % len(jobs))
                            time.sleep(10)

                        time.sleep(10)
                    else:
                        folione.MakeZScore()

                        folione.CalcCorrelation()
                        folione.SelectFactor()
                        if 0:
                            folione.MakeSignal()
                        else:
                            folione.MakeSignal_AllCombis()


            # Iterate through the list of jobs and remove one that are finished, checking every second.
            count_loop = 0
            while len(jobs) > 0:
                jobs = [job for job in jobs if job.is_alive()]
                print("%s: %s Process Left" % (count_loop, len(jobs)))
                time.sleep(10)

                count_loop += 1


        # Test
        #pivoted_sampled_datas = pivoted_sampled_datas[['BOJ자산','FED자산']]
        #pivoted_sampled_datas = pd.DataFrame(data = np.array([[-1, -1], [-2, -1], [-3, -2], [1, 1], [2, 1], [3, 2]]), columns = ['BOJ자산','FED자산'])


        if do_pca == True:
            # 정규화
            if 0:
                pivoted_sampled_std_datas = pd.DataFrame(StandardScaler().fit_transform(pivoted_sampled_datas.values))
                pivoted_sampled_std_datas.columns = pivoted_sampled_datas.columns
                pivoted_sampled_std_datas.index = pivoted_sampled_datas.index
            else:
                pivoted_sampled_std_datas = copy.deepcopy(pivoted_sampled_datas)

                if 0:
                    for column_nm in pivoted_sampled_datas.columns:
                        if column_nm not in ("KOSPI", "S&P500"):
                            pivoted_sampled_std_datas.drop(columns=column_nm, inplace=True)

            #pivoted_sampled_datas_T = pivoted_sampled_datas.transpose()
            #print (list(pivoted_sampled_datas))
            #print (pivoted_sampled_datas)

            if 1:
                pca = PCA(svd_solver='full', n_components=pivoted_sampled_std_datas.shape[1])
            else:
                pca = PCA(n_components=10)
            #principalComponents = pca.fit_transform(pivoted_sampled_datas)
            pca.fit(pivoted_sampled_std_datas.values)
            principalComponents = pd.DataFrame(data=pca.transform(pivoted_sampled_std_datas.values))
            #principalComponents.index = pivoted_sampled_datas.index
            #principalComponents["S&P500"] = pca.transform(pivoted_sampled_std_datas["S&P500"].values)
            '''
            a = copy.deepcopy(pivoted_sampled_datas)
            for column_nm in a.columns:
                if column_nm != "S&P500":
                    for row_nm in a.index:
                        a[column_nm][row_nm] = 0
            b = pd.DataFrame(data=pca.transform(a.values))
            '''
            w, U = np.linalg.eig(pca.get_covariance())
            #
            if 0:
                plt.scatter(pivoted_sampled_std_datas["KOSPI"].values, pivoted_sampled_std_datas["S&P500"].values, s=100, c='r')
                plt.scatter(principalComponents[0].values, principalComponents[1].values, s=100, c='b')
                plt.xlim(-3000, 3000)
                plt.ylim(-3000, 3000)
                plt.show()

            print(principalComponents)

        if do_figure == True:
            principalDf = pd.DataFrame(data=principalComponents, columns=['principal component 1', 'principal component 2'])

            color = 'r'
            Test_Figure.Figure_2D_NoClass(pivoted_sampled_datas, 'Original', ['BOJ자산','FED자산'], color)
            Test_Figure.Figure_2D_NoClass(principalDf, '2 Component PCA', ['principal component 1', 'principal component 2'], color)

        # DB에서 Raw 데이터를 읽어서 전처리하는 경우
        if use_datas_pickle == False:
            # Wrap운용팀 DB Disconnect
            db.disconnect()


