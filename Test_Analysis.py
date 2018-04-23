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
from Wrap_Folione import Folione
import Wrap_Folione as wf


use_datas_pickle = True
use_window_size_pickle = True
use_factor_selection_pickle = True
use_parallel_process = True
#do_simulation = True
do_pca = False
do_figure = False
save_datas_excel = False


if __name__ == '__main__':

    if use_datas_pickle == False:
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

        Wrap_Util.SavePickleFile(file='pivoted_sampled_datas.pickle', obj=pivoted_sampled_datas)
    else:
        pivoted_sampled_datas = Wrap_Util.ReadPickleFile(file='pivoted_sampled_datas.pickle')


    # Batch 시작
    window_size_from = 5
    window_size_to = 24

    pivoted_sampled_datas_last_pure_version = copy.deepcopy(pivoted_sampled_datas)
    for window_size in range(window_size_from, window_size_to):

        profit_calc_start_date = '2012-01-01'
        min_max_check_term = 8
        weight_check_term = 4
        target_index_nm_list = ["MSCI ACWI", "MSCI World", "MSCI EM", "KOSPI", "S&P500", "Nikkei225", "상해종합"]


        folione = Folione(pivoted_sampled_datas_last_pure_version, window_size
                          , profit_calc_start_date, min_max_check_term, weight_check_term, target_index_nm_list
                          , use_window_size_pickle, use_factor_selection_pickle, save_datas_excel)


        if use_parallel_process == True:
            # 신규 프로세스 생성
            p = mp.Process(target=wf.worker, args=(folione,))
            p.start()
            time.sleep(1)
        else:
            folione.MakeZScore()
            folione.SelectFactor()
            folione.SimulateSignal()


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



