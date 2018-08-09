#_*_ coding: utf-8 _*_

import win32com.client
from Test_MariaDB import WrapDB
import pandas as pd
import copy
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import multiprocessing as mp
import time

from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import Test_Figure
import Wrap_Util
from Wrap_Folione import Preprocess
from Wrap_Folione import Folione
import Wrap_Folione as wf

# Folione 모델 외부(전단계)
use_datas_pickle = True # 중간 저장된 raw data 사용 여부

# Folione 작업
do_simulation = True
# Folione 모델 내부
use_window_size_pickle = True # 중간 저장된 Z-Score data 사용 여부
use_correlation_pickle = True # 중간 저장된 Correlation data 사용 여부(Target Index와 Factor간의 관계)
use_factor_selection_pickle = True
make_simulate_signal = True

# 병렬처리 사용여부
use_parallel_process = True

# Debug 데이터 생성 여부
save_datas_excel = True
save_correlations_txt = True

# 데이터 분석
do_pca = False

# 그래프 생성
do_figure = False


if __name__ == '__main__':

    # Simulation 기간 타입
    # 1: 장기, 2: 중기, 3: 단기
    # 장기: 2001-01-01 부터 (IT 버블 시점), 데이터는 pivoted_sampled_datas의 기간과 연동(223 Factors)
    # 중기: 2007-01-01 부터 (금융위기 시점), 데이터는 pivoted_sampled_datas의 기간과 연동(274 Factors)
    # 단기: 2012-01-01 부터 (QE 시작 시점), 데이터는 pivoted_sampled_datas의 기간과 연동(315 Factors)
    simulation_term_type = 2
    if simulation_term_type == 1:
        simulation_start_date = '2001-01-01'
    elif  simulation_term_type == 2:
        simulation_start_date = '2007-01-01'
    elif simulation_term_type == 3:
        simulation_start_date = '2012-01-01'
    simulation_end_date = '2018-07-31'

    # Z-Score 생성의 경우 과거 추가 기간이 필요함.
    # Z-Score의 최대 기간과 동일 (월 단위)
    raw_data_spare_term = 36

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
        # raw 데이터의 기간이 체계가 없어 return 받은 start_date, end_date을 사용할 수 없다.
        data_list, start_date, end_date = preprocess.GetDataList(item_cd="아이템코드", start_date="시작일", last_date="마지막일")
        datas = db.get_bloomberg_datas(data_list=data_list, start_date=None, end_date=None)
        preprocess.SetDatas(datas=datas, datas_columns=["아이템코드", "아이템명", "날짜", "값"])

        # DataFrame 형태의 Sampled Data 생성
        # 'B': business daily, 'D': calendar daily, 'W': weekly, 'M': monthly
        preprocess.MakeSampledDatas(sampling_type='M', index='날짜', columns='아이템명', values='값')

        # 유효한 데이터 가장 최근 값으로 채움 
        preprocess.FillValidData(look_back_days=10)

        # 유효하지 않은 기간의 데이터 삭제
        # drop_basis_from: '2007-01-31', drop_basis_to: '가장 최근 말일'는 가장 유효한 factor를 많이 사용할 수 있는 기간을 찾아 적용하였음.
        #pivoted_sampled_datas = preprocess.DropInvalidData(drop_basis_from='2001-01-01', drop_basis_to='2018-03-31')
        print("simulation_start_date: ", simulation_start_date, str(datetime.strptime(simulation_start_date, '%Y-%m-%d').date() - relativedelta(months=raw_data_spare_term)))
        pivoted_sampled_datas = preprocess.DropInvalidData(drop_basis_from=str(datetime.strptime(simulation_start_date, '%Y-%m-%d').date() - relativedelta(months=raw_data_spare_term)), drop_basis_to=simulation_end_date)
        Wrap_Util.SaveExcelFiles(file='.\\pickle\\pivoted_sampled_datas_simulation_term_type_%s.xlsx' % (simulation_term_type), obj_dict={'pivoted_sampled_datas': pivoted_sampled_datas})
        
        Wrap_Util.SavePickleFile(file='.\\pickle\\pivoted_sampled_datas_simulation_term_type_%s.pickle' % (simulation_term_type), obj=pivoted_sampled_datas)
    else:
        pivoted_sampled_datas = Wrap_Util.ReadPickleFile(file='.\\pickle\\pivoted_sampled_datas_simulation_term_type_%s.pickle' % (simulation_term_type))


    if do_simulation == True:
        # Batch 시작
        # Z-Score 생성 기간(18개월 ~ ), 최대 기간은 raw_data_spare_term과 동일
        # 실험적으로 24개월보다 기간이 Window기간이 짧은 경우 Z-Score의 통계적 신뢰성이 떨어진다.
        # Correlation이 불안정함(+, - 반복)
        window_sizes = {"from": 24, "to": raw_data_spare_term}
        profit_calc_start_date = simulation_start_date
        profit_calc_end_date = simulation_end_date
        min_max_check_term = 2
        weight_check_term = 4

        # 장기 시뮬레이션
        if simulation_term_type == 1:
            target_index_nm_list = ["KOSPI", "S&P500"]
        # 중기, 단기 시뮬레이션
        else:
            target_index_nm_list = ["MSCI World", "MSCI EM", "KOSPI", "S&P500", "상해종합"]
        '''
        # Test
        target_index_nm_list = ["S&P500"]
        '''

        pivoted_sampled_datas_last_pure_version = copy.deepcopy(pivoted_sampled_datas)
        for window_size in range(window_sizes["from"], window_sizes["to"] + 1, 3):
            for target_index_nm in target_index_nm_list:
                folione = Folione(pivoted_sampled_datas_last_pure_version, window_size, simulation_term_type
                                  , profit_calc_start_date, profit_calc_end_date, min_max_check_term, weight_check_term, target_index_nm
                                  , use_window_size_pickle, use_factor_selection_pickle, use_correlation_pickle
                                  , make_simulate_signal
                                  , save_datas_excel, save_correlations_txt, use_parallel_process)

                if use_parallel_process == True:
                    # 신규 프로세스 생성
                    p = mp.Process(target=wf.FolioneStart, args=(folione,))
                    p.start()
                    time.sleep(1)
                else:
                    folione.MakeZScore()
                    folione.CalcCorrelation()
                    folione.SelectFactor()
                    #folione.MakeSignal()
                    folione.MakeSignal_AllCombis()


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



