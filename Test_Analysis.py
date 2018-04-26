#_*_ coding: utf-8 _*_

import win32com.client
from Test_MariaDB import WrapDB
import pandas as pd
import copy
from datetime import datetime
from datetime import timedelta
import multiprocessing as mp
import time

from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import Test_Figure
import Wrap_Util
from Wrap_Folione import Preprocess
from Wrap_Folione import Folione
import Wrap_Folione as wf

# 준비단계 데이터 생성
use_datas_pickle = True
use_window_size_pickle = True
use_factor_selection_pickle = True
use_correlation_pickle = True

# 병렬처리 사용여부
use_parallel_process = True

# Folione 작업
do_simulation = True
make_simulate_signal = False

# Debug 데이터 생성 여부
save_datas_excel = False
save_correlations_txt = False

# 데이터 분석
do_pca = False

# 그래프 생성
do_figure = False


if __name__ == '__main__':

    if use_datas_pickle == False:
        # Wrap운용팀 DB Connect
        db = WrapDB()
        db.connet(host="127.0.0.1", port=3306, database="WrapDB_1", user="root", password="maria")

        # 데이터 전처리 instance 생성
        preprocess = Preprocess()

        # 데이터 Info Read
        data_info = db.get_data_info()
        preprocess.SetDataInfo(data_info=data_info, data_info_columns=["아이템코드", "아이템명", "개수", "시작일", "마지막일"])

        # Factor 별 데이터가 존재하는 기간 설정
        preprocess.MakeDateRange(start_date="시작일", last_date="마지막일")

        # 유효기간 내 데이터 Read
        data_list, start_date, end_date = preprocess.GetDataList(item_cd="아이템코드", start_date="시작일", last_date="마지막일")
        datas = db.get_datas(data_list=data_list, start_date=None, end_date=None)
        preprocess.SetDatas(datas=datas, datas_columns=["아이템코드", "아이템명", "날짜", "값"])

        # DataFrame 형태의 Sampled Data 생성
        # 'B': business daily, 'D': calendar daily, 'W': weekly, 'M': monthly
        preprocess.MakeSampledDatas(sampling_type='M', index='날짜', columns='아이템명', values='값')

        # 유효한 데이터 가장 최근 값으로 채움 
        preprocess.FillValidData(look_back_days=10)

        # 유효하지 않은 기간의 데이터 삭제
        pivoted_sampled_datas = preprocess.DropInvalidData(drop_basis_from='2007-01-31', drop_basis_to='2018-03-31')
        
        Wrap_Util.SavePickleFile(file='.\\pickle\\pivoted_sampled_datas.pickle', obj=pivoted_sampled_datas)
    else:
        pivoted_sampled_datas = Wrap_Util.ReadPickleFile(file='.\\pickle\\pivoted_sampled_datas.pickle')


    if do_simulation == True:
        # Batch 시작
        window_size_from = 12
        window_size_to = 37
        #window_size_to = 13

        # 단기: 2012-01-01 부터 (QE 시작 시점)
        # 장기: 데이터 시작일 부터
        profit_calc_start_date = '2012-01-01'
        min_max_check_term = 8
        weight_check_term = 4
        target_index_nm_list = ["MSCI ACWI", "MSCI World", "MSCI EM", "KOSPI", "S&P500", "Nikkei225", "상해종합"]
        # target_index_nm_list = ["MSCI EM"]

        pivoted_sampled_datas_last_pure_version = copy.deepcopy(pivoted_sampled_datas)
        for window_size in range(window_size_from, window_size_to):

            folione = Folione(pivoted_sampled_datas_last_pure_version, window_size
                              , profit_calc_start_date, min_max_check_term, weight_check_term, target_index_nm_list
                              , use_window_size_pickle, use_factor_selection_pickle, use_correlation_pickle
                              , make_simulate_signal
                              , save_datas_excel, save_correlations_txt)

            if use_parallel_process == True:
                # 신규 프로세스 생성
                p = mp.Process(target=wf.FolioneStart, args=(folione,))
                p.start()
                time.sleep(1)
            else:
                folione.MakeZScore()
                folione.CalcCorrelation()
                folione.SelectFactor()
                folione.MakeSignal()


    # Test
    #pivoted_sampled_datas = pivoted_sampled_datas[['BOJ자산','FED자산']]
    #pivoted_sampled_datas = pd.DataFrame(data = np.array([[-1, -1], [-2, -1], [-3, -2], [1, 1], [2, 1], [3, 2]]), columns = ['BOJ자산','FED자산'])


    if do_pca == True:
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


    if do_figure == True:
        principalDf = pd.DataFrame(data=principalComponents, columns=['principal component 1', 'principal component 2'])

        color = 'r'
        Test_Figure.Figure_2D_NoClass(pivoted_sampled_datas, 'Original', ['BOJ자산','FED자산'], color)
        Test_Figure.Figure_2D_NoClass(principalDf, '2 Component PCA', ['principal component 1', 'principal component 2'], color)


    if use_datas_pickle == False:
        # Wrap운용팀 DB Disconnect
        db.disconnect()



