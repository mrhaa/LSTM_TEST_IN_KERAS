#_*_ coding: utf-8 _*_

import copy
import itertools
from datetime import datetime
from datetime import timedelta
import operator
import pandas as pd
import math
import numpy

import Wrap_Util
from Test_MariaDB import WrapDB



def FolioneStart(obj):
    obj.Start()


class Preprocess (object):

    def __init__(self):

        self.data_info = None
        self.datas = None

        self.date_range = {}

        self.reference_list = None
        self.reference_datas = None
        self.pivoted_reference_datas = None
        self.sampled_datas = None
        self.pivoted_sampled_datas = None


    def SetDataInfo(self, data_info, data_info_columns, data_list=None):

        # 정상적인 상황
        if data_list == None:
            self.data_info = data_info
        # 예외적인 상황
        # 관련 로직에 대한 확인 필요(2019-11-28, 류상진)
        else:
            self.data_info = pd.DataFrame(columns=data_info.columns)
            count = 0
            for data in data_info.transpose():
                if data_info.transpose()[data][1] in data_list:
                    print(data_info.transpose()[data][1])
                    self.data_info[count] = data_info.transpose()[data]
                    count += 1
            self.data_info = self.data_info.transpose()

        self.data_info.columns = data_info_columns


    # 최대한 많은 factor를 사용하기 위해 데이터가 존재하는 공통기간을 찾음.
    def MakeDateRange(self, start_date, last_date):

        for ele in self.data_info:
            if ele in (start_date, last_date):
                # 시작일의 초기값은 가장 빠른 날짜
                if ele == start_date:
                    self.date_range[ele] = "1111-11-11"
                # 마지막일의 초기값은 가장 늦은 날짜 
                elif ele == last_date:
                    self.date_range[ele] = "9999-99-99"
                # 나머지 element의 경우 패스
                else:
                    continue

                for idx in range(0, len(self.data_info[ele])):
                    if self.data_info[ele][idx]:
                        # 가장 늦은 시작일을 찾는다.
                        if ele == start_date and int(self.date_range[ele].replace("-", "")) < int(self.data_info[ele][idx].replace("-", "")):
                            self.date_range[ele] = self.data_info[ele][idx]
                        # 가장 빠른 마지막일을 찾는다.
                        if ele == last_date and int(self.date_range[ele].replace("-", "")) > int(self.data_info[ele][idx].replace("-", "")):
                            self.date_range[ele] = self.data_info[ele][idx]


    def GetDataList(self, item_cd, start_date, last_date):
        
        # factor명을 통해 factor 코드 리스트 생성 
        data_list = list(self.data_info[item_cd])
        
        # 사용 가능한 데이터의 유효 범위 설정
        start_date = self.date_range[start_date]
        end_date = self.date_range[last_date]

        return data_list, start_date, end_date


    def SetDatas(self, datas, datas_columns):

        self.datas = datas
        self.datas.columns = datas_columns


    def MakeSampledDatas(self, sampling_type, index, columns, values):

        # date type의 날짜 속성 추가
        self.datas["날짜T"] = self.datas[index].apply(lambda x: pd.to_datetime(str(x), format="%Y-%m-%d"))
        # datas.set_index(datas['날짜T'], inplace=True)

        # Sampling 방법에 따른 데이터 누락을 위해 ref_data 생성
        self.reference_list = self.datas.resample('D', on="날짜T", convention="end")
        self.reference_datas = self.datas.loc[self.datas["날짜T"].isin(list(self.reference_list.indices))]
        self.pivoted_reference_datas = self.reference_datas.pivot(index=index, columns=columns, values=values)

        # sampling type
        # 'B': business daily, 'D': calendar daily, 'W': weekly, 'M': monthly
        sampling_list = self.datas.resample(sampling_type, on="날짜T", convention="end")
        self.sampled_datas = self.datas.loc[self.datas["날짜T"].isin(list(sampling_list.indices))]

        self.pivoted_sampled_datas = self.sampled_datas.pivot(index=index, columns=columns, values=values)

        if 0:
            # 'B': business daily, 'D': calendar daily, 'W': weekly, 'M': monthly
            tmp_sampling_list = self.datas.resample('D', on="날짜T", convention="end")
            tmp_sampled_datas = self.datas.loc[self.datas["날짜T"].isin(list(tmp_sampling_list.indices))]
            tmp_pivoted_sampled_datas = tmp_sampled_datas.pivot(index=index, columns=columns, values=values)


            for column in tmp_pivoted_sampled_datas.columns:
                for idx, row in enumerate(tmp_pivoted_sampled_datas.index):
                    if math.isnan(tmp_pivoted_sampled_datas[column][row]) == False:
                        print(column + '\t' + str((len(tmp_pivoted_sampled_datas.index) - tmp_pivoted_sampled_datas[column].isnull().sum())/(len(tmp_pivoted_sampled_datas.index) - idx)))
                        break

            Wrap_Util.SaveExcelFiles(file='tmp_pivoted_sampled_datas.xlsx', obj_dict={'pivoted_sampled_datas': tmp_pivoted_sampled_datas})

        return copy.deepcopy(self.pivoted_sampled_datas)


    def FillValidData(self, look_back_days, input_data=None):

        if input_data is not None:
            self.pivoted_sampled_datas = copy.deepcopy(input_data)

        for column_nm in self.pivoted_sampled_datas.columns:
            for row_nm in self.pivoted_sampled_datas.index:

                # Debug
                #if row_nm == '1996-02-26' and column_nm == '상해종합':
                #    print(1)

                # 월말 데이터가 없는경우
                if math.isnan(self.pivoted_sampled_datas[column_nm][row_nm]) == True:
                    # print (column_nm, "\t", row_nm, "\t", pivoted_sampled_datas[column_nm][row_nm])

                    # ref_row_nm = copy.copy(row_nm)
                    ref_row_nm = row_nm

                    # 해당일에 데이터가 없는 경우 가장 최근 값을 대신 사용함
                    for loop_cnt in range(look_back_days):
                        try:
                            if math.isnan(self.pivoted_reference_datas[column_nm][ref_row_nm]) == True:
                                # print("No Data", str(ref_row_nm))
                                ref_row_nm = str(datetime.strptime(ref_row_nm, '%Y-%m-%d').date() - timedelta(days=1))

                            # 가장 최신 데이터를 찾은 경우
                            else:
                                self.pivoted_sampled_datas[column_nm][row_nm] = float(self.pivoted_reference_datas[column_nm][ref_row_nm])
                                break

                        except KeyError:
                            # print("KeyError", str(ref_row_nm))
                            ref_row_nm = str(datetime.strptime(ref_row_nm, '%Y-%m-%d').date() - timedelta(days=1))

                # 불필요한 작업이라 삭제예정(2019-11-29, 류상진)
                '''
                # 이후 연산작업을 위해 decimal을 float 형태로 변경
                if math.isnan(self.pivoted_sampled_datas[column_nm][row_nm]) == False:
                    self.pivoted_sampled_datas[column_nm][row_nm] = float(self.pivoted_sampled_datas[column_nm][row_nm])
                '''

        return copy.deepcopy(self.pivoted_sampled_datas)


    def DropInvalidData(self, drop_basis_from, drop_basis_to, lag_shift_yn=False):

        # 데이터 유효기간 정의
        drop_basis_from = datetime.strptime(drop_basis_from, '%Y-%m-%d').date()
        drop_basis_to = datetime.strptime(drop_basis_to, '%Y-%m-%d').date()

        # lag가 있는 factor는 평가일로 shift 시킴
        if lag_shift_yn == True:

            # 데이터 오류에 의해 미래 데이터에 의해 shift되는 경우 삭제
            pivoted_sampled_datas_cp = copy.deepcopy(self.pivoted_sampled_datas)
            for row_nm in pivoted_sampled_datas_cp.index:
                data_time = datetime.strptime(row_nm, '%Y-%m-%d').date()
                if data_time > drop_basis_to:
                    self.pivoted_sampled_datas.drop(index=row_nm, inplace=True)

            # lag가 발생한 factor는 먼저 shift 시킴
            pivoted_sampled_datas_cp = copy.deepcopy(self.pivoted_sampled_datas)
            for column_nm in pivoted_sampled_datas_cp.columns:
                if math.isnan(pivoted_sampled_datas_cp[column_nm][-1]) == True:
                    lag_count = 0
                    for i in range(1, 13):
                        if math.isnan(pivoted_sampled_datas_cp[column_nm][-i]) == True:
                            lag_count += 1
                        else:
                            break

                    self.pivoted_sampled_datas[column_nm] = pivoted_sampled_datas_cp[column_nm].shift(periods=lag_count)

            self.pivoted_sampled_datas = self.pivoted_sampled_datas.fillna(method='ffill', limit=1)

            # 유효기간을 벗어난 데이터 삭제
            pivoted_sampled_datas_cp = copy.deepcopy(self.pivoted_sampled_datas)
            for row_nm in pivoted_sampled_datas_cp.index:
                data_time = datetime.strptime(row_nm, '%Y-%m-%d').date()
                if data_time < drop_basis_from:
                    self.pivoted_sampled_datas.drop(index=row_nm, inplace=True)

            # 3개월 이상 데이터가 누락된 경우 삭제
            pivoted_sampled_datas_cp = copy.deepcopy(self.pivoted_sampled_datas)
            for column_nm in pivoted_sampled_datas_cp.columns:
                empty_cnt = 0
                for row_nm in pivoted_sampled_datas_cp.index:
                    if math.isnan(pivoted_sampled_datas_cp[column_nm][row_nm]) == True:
                        empty_cnt += 1
                    else:
                        empty_cnt = 0
                    
                    # 3개월 연속으로 데이터 누락이 발생하면 해당 factor 삭제
                    if empty_cnt >= 3:
                        self.pivoted_sampled_datas.drop(columns=column_nm, inplace=True)
                        break
             
            # 마지막으로 누락된 데이터는 이전달 데이터를 복사함
            self.pivoted_sampled_datas = self.pivoted_sampled_datas.fillna(method='ffill')


        # lag가 있는 factor는 제외시킴
        else:
            # 유효기간을 벗어난 데이터 삭제
            pivoted_sampled_datas_cp = copy.deepcopy(self.pivoted_sampled_datas)
            for row_nm in pivoted_sampled_datas_cp.index:
                data_time = datetime.strptime(row_nm, '%Y-%m-%d').date()
                if data_time < drop_basis_from or data_time > drop_basis_to:
                    self.pivoted_sampled_datas.drop(index=row_nm, inplace=True)

            # 유효하지 않은 팩터 drop
            total_omission_threshold = 1
            last_omission_threshold = 1
            last_considerable_num = 6
            pivoted_sampled_datas_cp = copy.deepcopy(self.pivoted_sampled_datas)
            for column_nm in pivoted_sampled_datas_cp.columns:
                # 전체 유효기간 중 데이터가 없는 갯수
                total_null_cnt = pivoted_sampled_datas_cp[column_nm].isnull().sum()
                # 유효기간 중 마지막 특정 기간 내 데이터가 없는 갯수
                last_null_cnt = pivoted_sampled_datas_cp[column_nm][-last_considerable_num:].isnull().sum()
    
                if total_null_cnt > total_omission_threshold or last_null_cnt > last_omission_threshold:
                    # print('유효하지 않은 누락\t', column_nm, '\t', total_null_cnt, '\t', last_null_cnt)
                    self.pivoted_sampled_datas.drop(columns=column_nm, inplace=True)
                elif total_null_cnt:
                    # print('유효한 누락\t', column_nm, '\t', total_null_cnt, '\t', last_null_cnt)
                    for idx, row_nm in enumerate(self.pivoted_sampled_datas.index):
                        if math.isnan(self.pivoted_sampled_datas[column_nm][row_nm]) == True:
                            if idx == 0:
                                self.pivoted_sampled_datas[column_nm][idx] = self.pivoted_sampled_datas[column_nm][idx + 1]
                            else:
                                self.pivoted_sampled_datas[column_nm][idx] = self.pivoted_sampled_datas[column_nm][idx - 1]

        return self.pivoted_sampled_datas


class Folione (object):

    def __init__(self, raw_data, window_size, simulation_term_type
                 , profit_calc_start_date, profit_calc_end_date, min_max_check_term, weight_check_term, target_index_nm
                 , use_window_size_pickle=False, use_factor_selection_pickle=False, use_correlation_pickle=False
                 , make_folione_signal = False
                 , save_datas_excel=False, save_correlations_txt=False, save_signal_process_db=False, save_signal_last_db=True, use_parallel_process=False):

        # Z-Score 생성하기 위한 준비 데이터
        # row: 날짜, column: factor
        self.raw_data = copy.deepcopy(raw_data)
        self.mean_data = None
        self.std_data = None
        self.zscore_data = None

        # Z-Score를 이용한 One Factor 수익률 데이터
        # 1단계: target index, 2단계: factor
        self.model_accumulated_profits = {}
        self.bm_accumulated_profits = {}

        # Z-Score를 이용한 Multi Factor 시그널, Factor 별 수익률이 높은 순으로 Factor 누적
        # 1단계: target index, 2단계: 누적 factor, 3단계: 날짜
        self.model_signals = {}

        # Factor 별 Target Index와 Moving Correlation
        # 1단계: rolling month, 2단계: target index, 3단계: factor
        self.rolling_correlations = {}
        self.corr = None

        # Folione 고유 파라미터
        self.window_size = copy.deepcopy(window_size) # Z-Score 만들기 위한 기간
        self.simulation_term_type = copy.deepcopy(simulation_term_type)  # 장,중,단기 시뮬레이션
        self.profit_calc_start_date = datetime.strptime(profit_calc_start_date, '%Y-%m-%d').date() # Factor 별 누적 수익률 시작일 (장/단기에 따라 변동)
        self.profit_calc_end_date = datetime.strptime(profit_calc_end_date,'%Y-%m-%d').date()  # Factor 별 누적 수익률 끝일
        self.target_index_nm = copy.deepcopy(target_index_nm) # target index, Ex. "MSCI ACWI", "MSCI World", "MSCI EM", "KOSPI", "S&P500", "Nikkei225", "상해종합"
        self.min_max_check_term = copy.deepcopy(min_max_check_term) # Min/Max의 평균 Z-Score를 구하기 위한 기간
        self.weight_check_term = copy.deepcopy(weight_check_term) # 평균 Z-Score의 Momentum 안정성을 판단하기 위한 기간
        
        # 중간 산출물 재사용 여부 확인 Flag
        # 최초 1회는 생성 되어야 하며, data 변경시 재생성 되어야함
        self.use_window_size_pickle = copy.deepcopy(use_window_size_pickle) # self.zscore_data 재사용
        self.use_factor_selection_pickle = copy.deepcopy(use_factor_selection_pickle) # self.model_accumulated_profits & self.bm_accumulated_profits 재사용
        self.use_correlation_pickle = copy.deepcopy(use_correlation_pickle) # self.rolling_correlations 재사용

        # 다중 Factor를 이용한 Signal 생성 여부 Flag
        self.make_folione_signal = copy.deepcopy(make_folione_signal)

        # Debug용 데이터 저장 여부
        self.save_datas_excel = copy.deepcopy(save_datas_excel)
        self.save_correlations_txt = copy.deepcopy(save_correlations_txt)
        self.save_signal_process_db = copy.deepcopy(save_signal_process_db)
        self.save_signal_last_db = copy.deepcopy(save_signal_last_db)

        self.use_parallel_process = copy.deepcopy(use_parallel_process)



    def Start(self):

        self.MakeZScore()
        self.CalcCorrelation()
        self.SelectFactor()
        if 0:
            self.MakeSignal()
        else:
            self.MakeSignal_AllCombis()


    def MakeZScore(self):

        if self.use_window_size_pickle == False:
            self.mean_data = copy.deepcopy(self.raw_data.rolling(window=self.window_size, center=False).mean())
            self.std_data = copy.deepcopy(self.raw_data.rolling(window=self.window_size, center=False).std())
            raw_datas_cp = copy.deepcopy(self.raw_data)
            for idx, row_nm in enumerate(raw_datas_cp.index):

                # 평균 및 표준편차 데이터가 생성되지 않는 구간 삭제
                drop_point = self.window_size - 1
                if idx < drop_point:
                    #self.raw_data.drop(index=row_nm, inplace=True)
                    self.mean_data.drop(index=row_nm, inplace=True)
                    self.std_data.drop(index=row_nm, inplace=True)
                else:
                    break

            # Z-Score 계산
            # 데이터의 변화가 없어 표준편차가 0(ZeroDivisionError)인 경우 때문에 DataFrame을 이용한 연산처리 불가
            self.zscore_data = copy.deepcopy(self.mean_data)
            for column_nm in self.zscore_data.columns:
                for idx, row_nm in enumerate(self.zscore_data.index):
                    '''
                    # Test 미국 연방기금 목표금리, 2010-12-31
                    if (column_nm == '미국 연방기금 목표금리' and row_nm == '2010-12-31') or (column_nm == '중국 기준금리' and row_nm == '2014-06-30'):
                        print ('Test Debug', '\t', column_nm, '\t', row_nm)
                    '''
                    try:
                        #self.zscore_data[column_nm][row_nm] = (self.raw_data[column_nm][row_nm] - self.mean_data[column_nm][row_nm]) / self.std_data[column_nm][row_nm]
                        # mean과 std data의 경우 raw data보다 window_size -1 만큼 적음. window_size부터 z-score 생성
                        self.zscore_data[column_nm][row_nm] = (self.raw_data[column_nm][(self.window_size - 1) + idx] - self.mean_data[column_nm][row_nm]) / self.std_data[column_nm][row_nm]

                        # std 0이어도 Error 발생하지 않는 경우 있음.
                        if numpy.isnan(self.zscore_data[column_nm][row_nm]) == True or numpy.isinf(self.zscore_data[column_nm][row_nm]) == True:
                            self.zscore_data[column_nm][row_nm] = self.zscore_data[column_nm][idx-1]
                    except ZeroDivisionError:
                        #self.zscore_data[column_nm][row_nm] = 0.0
                        self.zscore_data[column_nm][row_nm] = self.zscore_data[column_nm][idx-1]

            Wrap_Util.SavePickleFile(file='.\\pickle\\pivoted_sampled_datas_zscore_target_index_%s_simulation_term_type_%s_target_date_%s_window_size_%s.pickle'
                                          % (self.target_index_nm, self.simulation_term_type, self.profit_calc_end_date, self.window_size), obj=self.zscore_data)

            if self.save_datas_excel:
                Wrap_Util.SaveExcelFiles(file='.\\pickle\\save_datas_excel_target_index_%s_simulation_term_type_%s_target_date_%s_window_size_%s.xlsx'
                                              % (self.target_index_nm, self.simulation_term_type, self.profit_calc_end_date, self.window_size)
                                         , obj_dict={'raw_data': self.raw_data, 'mean_data': self.mean_data, 'std_data': self.std_data, 'zscore_data': self.zscore_data})

        else:
            self.zscore_data = Wrap_Util.ReadPickleFile(file='.\\pickle\\pivoted_sampled_datas_zscore_target_index_%s_simulation_term_type_%s_target_date_%s_window_size_%s.pickle'
                                                             % (self.target_index_nm, self.simulation_term_type, self.profit_calc_end_date, self.window_size))

        return True


    def SelectFactor(self):

        if self.use_factor_selection_pickle == False:

            # Wrap운용팀 DB Connect
            db = WrapDB()
            db.connet(host="127.0.0.1", port=3306, database="WrapDB_1", user="root", password="ryumaria")
            factors_nm_cd_map = db.get_factors_nm_cd()

            if self.save_datas_excel:
                factor_signal_data = copy.deepcopy(self.zscore_data)
                average_zscore_data = copy.deepcopy(self.zscore_data)
                max_zscore_data = copy.deepcopy(self.zscore_data)

            index_nm = self.target_index_nm

            # 누적 수익률 저장 공간
            self.model_accumulated_profits[index_nm] = {}
            self.bm_accumulated_profits[index_nm] = {}

            check_first_data = False
            for column_nm in self.zscore_data.columns:

                if self.save_datas_excel:
                    factor_signal_data[column_nm].values.fill(0)
                    average_zscore_data[column_nm].values.fill(0)
                    max_zscore_data[column_nm].values.fill(0)

                # Factor별 수익률 계산
                # 현재, Target Index와 Factor가 동일한 경우는 제외한다.
                #if column_nm != index_nm:
                # Factor가 Target Indext대비 선행성이 없으면 제외한다.
                if column_nm != index_nm and int(self.corr[index_nm + "_" + column_nm]['lag']) != 0:
                    # 누적 수익률 초기화
                    self.model_accumulated_profits[index_nm][column_nm] = 1.0
                    self.bm_accumulated_profits[index_nm][column_nm] = 1.0
                    
                    model_signal = "BUY"
                    
                    # Correlation lag 사용여부
                    # FACTOR LAG 사용: 1, 미사용: 0
                    use_factor_lag = 1
                    max_factor_lag = int(max(self.corr.transpose()['lag'].values)) if use_factor_lag else 0

                    # 주식 Buy, Sell 포지션 판단
                    new_point = self.weight_check_term - 1
                    average_array = [0] * self.weight_check_term
                    for idx, row_nm in enumerate(self.zscore_data.index):
                        
                        # 시그널이 필요한 전달까지 가장 잘 맞추는 Factor 선정
                        #if row_nm == self.zscore_data.index[-1]:
                        #    break
                        
                        try:
                            # 과거 moving average 생성 및 시프트
                            # min_max_check_term 개수 만큼 raw 데이터가 생겨야 average 생성 가능
                            if idx >= (self.min_max_check_term - 1) + max_factor_lag:
                                # 최신 데이터를 한칸씩 시프트
                                average_array[:new_point] = average_array[-new_point:]

                                # 역관계이면 z-score에 -1을 곱한다.
                                # Corr가 가장 높은 기간으로 lag 적용(Factor가 Target Index보다 선행)
                                factor_lag = int(self.corr[index_nm + "_" + column_nm]['lag']) if use_factor_lag else 0
                                # 과거 Folione과 동일한 로직(중간값 개념), lag 개념 추가
                                if 0:
                                    average_array[-1] = int(self.corr[index_nm + "_" + column_nm]['direction']) \
                                                        * (self.zscore_data[column_nm][idx - (self.min_max_check_term - 1) - factor_lag:idx - factor_lag + 1].min()
                                                           + self.zscore_data[column_nm][idx - (self.min_max_check_term - 1) - factor_lag:idx - factor_lag + 1].max()) / 2
                                # 신규 Folione과 동일한 로직(평균 개념), lag 개념 추가
                                else:
                                    average_array[-1] = int(self.corr[index_nm + "_" + column_nm]['direction']) * self.zscore_data[column_nm][idx - (self.min_max_check_term - 1) - factor_lag:idx - factor_lag + 1].mean()

                                # 수익률 계산 시작
                                # factor 검증 start date 이후 부터 처리
                                # weight_check_term 개수 만큼 average 데이터가 생겨야 노이즈 검증 가능
                                if datetime.strptime(row_nm,'%Y-%m-%d').date() >= self.profit_calc_start_date and idx - (self.min_max_check_term - 1 + max_factor_lag) >= self.weight_check_term:
                                    
                                    # Test, Debug용, Window Size에 따라 누적수익률 시작점 확인
                                    if check_first_data == False:
                                        print (self.window_size, index_nm, row_nm)
                                        check_first_data = True

                                    if 0:
                                        # 이번 signal의 위치에 맞게 주식비율 조절 매수
                                        ai_profit_rate = 0.0159 / 12 # 예탁이용료 1달 수익률
                                        buy_ratio = (average_array[new_point] - average_array.min()) / (average_array.max() - average_array.min())
                                        if buy_ratio >= 0.0:
                                            self.model_accumulated_profits[index_nm][column_nm] *= (1 + (buy_ratio * (self.raw_data[index_nm][self.window_size + idx] / self.raw_data[index_nm][self.window_size + idx - 1] - 1) + (1 - buy_ratio) * ai_profit_rate))
                                    else:
                                        '''
                                        # Test 미국 산업생산
                                        if (column_nm == '미국 산업생산'):
                                            print('Test Debug', '\t', column_nm, '\t', row_nm)
                                        '''
                                        # 이번 signal이 max인 경우 주식 100% 매수
                                        #if average_array[new_point] == max(average_array):
                                        if average_array[-1] == max(average_array):
                                            # self.raw_data[index_nm].index.values[self.window_size + idx]
                                            # z-score의 경우 raw data보다 window_size -1 만큼 적음. window_size부터 z-score 생성됨
                                            if self.window_size + idx < len(self.raw_data.index):
                                                self.model_accumulated_profits[index_nm][column_nm] *= (self.raw_data[index_nm][self.window_size + idx] / self.raw_data[index_nm][self.window_size + idx - 1])

                                            # 3단계. 예측 index & factor & 시계열별로 signal을 가진다
                                            model_signal = "BUY"

                                            if self.save_datas_excel:
                                                factor_signal_data[column_nm][idx] = 1

                                        else:
                                            model_signal = "SELL"

                                    if self.save_datas_excel:
                                        average_zscore_data[column_nm][idx] = average_array[new_point]
                                        max_zscore_data[column_nm][idx] = max(average_array)

                                    # z-score의 경우 raw data보다 window_size -1 만큼 적음. window_size부터 z-score 생성됨
                                    if self.window_size + idx < len(self.raw_data.index):
                                        self.bm_accumulated_profits[index_nm][column_nm] *= (self.raw_data[index_nm][self.window_size + idx] / self.raw_data[index_nm][self.window_size + idx - 1])
                                    # print(index_nm, '\t', column_nm, '\t', row_nm, '\t', model_accumulated_profits, '\t', bm_accumulated_profits)

                        except IndexError:
                            print("IndexError:\t", index_nm, '\t', column_nm, '\t', row_nm)
                            pass

                    # 모델의 성능이 BM 보다 좋은 팩터 결과만 출력
                    if self.model_accumulated_profits[index_nm][column_nm] > self.bm_accumulated_profits[index_nm][column_nm]:
                        print(self.window_size, '\t', index_nm, '\t', column_nm, '\t',
                              #self.corr_max[index_nm + "_" + column_nm], '\t', self.corr_max[index_nm + "_" + column_nm], '\t',
                              self.model_accumulated_profits[index_nm][column_nm], '\t',
                              self.bm_accumulated_profits[index_nm][column_nm])

                    # Factor result DB 저장
                    if self.save_signal_last_db == True and row_nm == str(self.profit_calc_end_date):
                        date_info = {'start_dt': str(self.profit_calc_start_date), 'end_dt': str(self.profit_calc_end_date),'curr_dt': row_nm}
                        target_cd = factors_nm_cd_map[index_nm]
                        factor_cd = factors_nm_cd_map[column_nm]
                        signal_cd = 1 if model_signal == "BUY" else 0
                        etc = {'window_size': self.window_size, 'factor_profit': float(self.model_accumulated_profits[index_nm][column_nm]),
                               'index_profit': float(self.bm_accumulated_profits[index_nm][column_nm]), 'term_type': self.simulation_term_type}

                        # 마지막 Signal만 저장
                        if self.save_signal_last_db == True and row_nm == str(self.profit_calc_end_date):
                            db.insert_factor_signal(date_info, target_cd, factor_cd, signal_cd, etc)

            Wrap_Util.SavePickleFile(file='.\\pickle\\model_accumulated_profits_target_index_%s_simulation_term_type_%s_target_date_%s_window_size_%s.pickle'
                                          % (self.target_index_nm, self.simulation_term_type, self.profit_calc_end_date, self.window_size), obj=self.model_accumulated_profits)
            Wrap_Util.SavePickleFile(file='.\\pickle\\bm_accumulated_profits_target_index_%s_simulation_term_type_%s_target_date_%s_window_size_%s.pickle'
                                          % (self.target_index_nm, self.simulation_term_type, self.profit_calc_end_date, self.window_size), obj=self.bm_accumulated_profits)

            if self.save_datas_excel:
                Wrap_Util.SaveExcelFiles(file='.\\pickle\\factor_signal_excel_target_index_%s_simulation_term_type_%s_target_date_%s_window_size_%s.xlsx'
                                          % (self.target_index_nm, self.simulation_term_type, self.profit_calc_end_date, self.window_size)
                                         , obj_dict={'target_index': self.raw_data[index_nm][self.window_size - 1:], 'zscore_data': self.zscore_data, 'factor_signal_data': factor_signal_data
                                         , 'average_zscore_data': average_zscore_data, 'max_zscore_data': max_zscore_data, 'corr': self.corr})

            # Wrap운용팀 DB Disconnect
            db.disconnect()

        else:
            self.model_accumulated_profits = Wrap_Util.ReadPickleFile(file='.\\pickle\\model_accumulated_profits_target_index_%s_simulation_term_type_%s_target_date_%s_window_size_%s.pickle'
                                                                           % (self.target_index_nm, self.simulation_term_type, self.profit_calc_end_date, self.window_size))
            self.bm_accumulated_profits = Wrap_Util.ReadPickleFile(file='.\\pickle\\bm_accumulated_profits_target_index_%s_simulation_term_type_%s_target_date_%s_window_size_%s.pickle'
                                                                        % (self.target_index_nm, self.simulation_term_type, self.profit_calc_end_date, self.window_size))

        return True


    def MakeSignal(self):

        if self.make_folione_signal == True:

            if self.save_datas_excel:
                model_signal_data = pd.DataFrame(index=self.zscore_data.index)
                average_zscore_data = copy.deepcopy(model_signal_data)
                max_zscore_data = copy.deepcopy(model_signal_data)

                rst_idx_str = ["Window Size", "Target Index", "시작일", "마지막일", "기간(일)", "시그널", "누적 모델 수익률(연환산)","누적 BM 수익률(연환산)", "누적 모델 수익률", "누적 BM 수익률", "펙터(#)", "펙터"]
                result_data = pd.DataFrame(index=rst_idx_str)

            # Signal 결과 저장
            #f = open(".\\pickle\\Signal_target_index_%s_simulation_term_type_%s_window_size_%s_From %s To %s.txt" % (self.target_index_nm, self.simulation_term_type, self.window_size, self.profit_calc_start_date, self.profit_calc_end_date), 'w')
            idx_str = "Window Size" + '\t' + "Target Index" + '\t' + "시작일" + '\t' + "마지막일" + '\t' + "기간(일)" + '\t' + "시그널" + '\t' + "누적 모델 수익률(연환산)" + '\t' \
                      + "누적 BM 수익률(연환산)" + '\t' + "누적 모델 수익률" + '\t' + "누적 BM 수익률" + '\t' + "펙터(#)" + '\t' + "펙터" + '\n'
            #f.write(idx_str)

            # 병렬처리 아닌 경우 로그 프린트
            if self.use_parallel_process == False:
                print(idx_str)

            # factor 예측 모형에서 사용되는 최대 factor 갯수는 10
            max_simulate_factor_num = 10
            index_nm = self.target_index_nm

            # 1단계. 예측 index별로 container 생성
            self.model_signals[index_nm] = {}

            model_profitable_factors_sorted = dict(sorted(self.model_accumulated_profits[index_nm].items(), key=operator.itemgetter(1), reverse=True))

            signal_factors_nm = ""
            simulate_factor_list = []
            for profitable_factor in model_profitable_factors_sorted:

                # 수익률 관련 메타 정보 저장
                check_first_data = False
                profit_start_date = '1111-11-11'
                profit_end_date = '9999-99-99'

                # 최대 factor 갯수는 10개까지 테스트 & BM보다 좋은 수익률을 내는 factor
                #if len(simulate_factor_list) <= max_simulate_factor_num and self.model_accumulated_profits[index_nm][profitable_factor] > self.bm_accumulated_profits[index_nm][profitable_factor]:
                if len(simulate_factor_list) <= max_simulate_factor_num:
                    if len(simulate_factor_list):
                        signal_factors_nm = signal_factors_nm + " & " + profitable_factor
                    else:
                        signal_factors_nm = profitable_factor
                    simulate_factor_list.append(profitable_factor)

                    # 2단계. 예측 index & factor combination별로 container 생성
                    self.model_signals[index_nm][signal_factors_nm] = {}

                    if self.save_datas_excel:
                        model_signal_data[signal_factors_nm] = 0
                        average_zscore_data[signal_factors_nm] = 0
                        max_zscore_data[signal_factors_nm] = 0
                        result_data[signal_factors_nm] = None
                else:
                    break


                # 모델을 이용한 누적수익률
                accumulated_model_profit = 1.0
                accumulated_bm_profit = 1.0

                # Correlation lag 사용여부
                # FACTOR LAG 사용: 1, 미사용: 0
                use_factor_lag = 1
                max_factor_lag = int(max(self.corr.transpose()['lag'].values)) if use_factor_lag else 0

                new_point = self.weight_check_term - 1
                average_array = [0] * self.weight_check_term
                for idx, row_nm in enumerate(self.zscore_data.index):
                    try:
                        # 과거 moving average 생성 및 시프트
                        # min_max_check_term 개수 만큼 raw 데이터가 생겨야 average 생성 가능
                        if idx >= (self.min_max_check_term - 1) + max_factor_lag:
                            average_array[:new_point] = average_array[-new_point:]

                            average_array[-1] = 0
                            # 다수 factor를 이용해 모델 예측하는 경우 factor들의 값을 더한 후 평균
                            for factor in simulate_factor_list:
                                factor_lag = int(self.corr[index_nm + "_" + factor]['lag']) if use_factor_lag else 0
                                # 과거 Folione과 동일한 로직(중간값 개념), lag 개념 추가
                                if 0:
                                    average_array[-1] += int(self.corr[index_nm + "_" + factor]['direction']) * \
                                                         (self.zscore_data[factor][idx - (self.min_max_check_term - 1) - factor_lag:idx - factor_lag + 1].min()
                                                          + self.zscore_data[factor][idx - (self.min_max_check_term - 1) - factor_lag:idx - factor_lag + 1].max()) / 2
                                # 신규 Folione과 동일한 로직(평균 개념), lag 개념 추가
                                else:
                                    average_array[-1] += int(self.corr[index_nm + "_" + factor]['direction']) * self.zscore_data[factor][idx - (self.min_max_check_term - 1) - factor_lag:idx - factor_lag + 1].mean()
                            average_array[-1] /= len(simulate_factor_list)

                            # 수익률 계산 시작
                            # weight_check_term 개수 만큼 average 데이터가 생겨야 노이즈 검증 가능
                            if datetime.strptime(row_nm, '%Y-%m-%d').date() >= self.profit_calc_start_date and idx - (self.min_max_check_term - 1 + max_factor_lag) >= self.weight_check_term:

                                # Test, Debug용, Window Size에 따라 누적수익률 시작점 확인
                                if check_first_data == False:
                                    #print(self.window_size, index_nm, row_nm)
                                    profit_start_date = row_nm
                                    check_first_data = True

                                # 조건 만족으로 BUY 포지션
                                if 0:
                                    # 이번 signal의 위치에 맞게 주식비율 조절 매수
                                    ai_profit_rate = 0.0159 / 12  # 예탁이용료 1달 수익률
                                    buy_ratio = (average_array[new_point] - average_array.min()) / (average_array.max() - average_array.min())
                                    if buy_ratio >= 0.0:
                                        accumulated_model_profit *= (1 + (buy_ratio * (self.raw_data[index_nm][self.window_size + idx] / self.raw_data[index_nm][self.window_size + idx - 1] - 1) + (1 - buy_ratio) * ai_profit_rate))
                                else:
                                    #if average_array[new_point] == max(average_array):
                                    if average_array[-1] == max(average_array):
                                        # z-score의 경우 raw data보다 window_size -1 만큼 적음. window_size부터 z-score 생성됨
                                        if self.window_size + idx < len(self.raw_data.index):
                                            accumulated_model_profit *= (self.raw_data[index_nm][self.window_size + idx + idx] / self.raw_data[index_nm][self.window_size + idx - 1])

                                        # 3단계. 예측 index & factor combination & 시계열별로 signal을 가진다
                                        self.model_signals[index_nm][signal_factors_nm][row_nm] = "BUY"

                                        if self.save_datas_excel:
                                            model_signal_data[signal_factors_nm][row_nm] = 1
                                    else:
                                        self.model_signals[index_nm][signal_factors_nm][row_nm] = "SELL"
                                profit_end_date = row_nm

                                if self.save_datas_excel:
                                    average_zscore_data[signal_factors_nm][row_nm] = average_array[-1]
                                    max_zscore_data[signal_factors_nm][row_nm] = max(average_array)

                                # z-score의 경우 raw data보다 window_size -1 만큼 적음. window_size부터 z-score 생성됨
                                if self.window_size + idx < len(self.raw_data.index):
                                    accumulated_bm_profit *= (self.raw_data[index_nm][self.window_size + idx] / self.raw_data[index_nm][self.window_size + idx - 1])

                    except IndexError:
                        print("IndexError:\t", index_nm, '\t', signal_factors_nm, '\t', row_nm)
                        pass

                # 유효 factor들의 combination을 이용하여
                if self.save_datas_excel and accumulated_model_profit > accumulated_bm_profit:
                    profit_period = (datetime.strptime(profit_end_date, '%Y-%m-%d').date() - datetime.strptime(profit_start_date, '%Y-%m-%d').date()).days

                    result_data[signal_factors_nm]["Window Size"] = self.window_size
                    result_data[signal_factors_nm]["Target Index"] = index_nm
                    result_data[signal_factors_nm]["시작일"] = profit_start_date
                    result_data[signal_factors_nm]["마지막일"] = profit_end_date
                    result_data[signal_factors_nm]["기간(일)"] = profit_period
                    result_data[signal_factors_nm]["시그널"] = self.model_signals[index_nm][signal_factors_nm][profit_end_date]
                    result_data[signal_factors_nm]["누적 모델 수익률(연환산)"] = accumulated_model_profit / profit_period * 365
                    result_data[signal_factors_nm]["누적 BM 수익률(연환산)"] = accumulated_bm_profit / profit_period * 365
                    result_data[signal_factors_nm]["누적 모델 수익률"] = accumulated_model_profit
                    result_data[signal_factors_nm]["누적 BM 수익률"] = accumulated_bm_profit
                    result_data[signal_factors_nm]["펙터(#)"] = len(simulate_factor_list)
                    result_data[signal_factors_nm]["펙터"] = signal_factors_nm

                    signal_str = str(self.window_size) + '\t' + index_nm + '\t' + profit_start_date + '\t' + profit_end_date + '\t' + str(profit_period) + '\t' + self.model_signals[index_nm][signal_factors_nm][profit_end_date] + '\t' \
                                 + str(accumulated_model_profit / profit_period * 365) + '\t' + str(accumulated_bm_profit / profit_period * 365) + '\t' \
                                 + str(accumulated_model_profit) + '\t' + str(accumulated_bm_profit) + '\t' + str(len(simulate_factor_list)) + '\t' + signal_factors_nm

                    # 병렬처리 아닌 경우 로그 프린트
                    if self.use_parallel_process == False:
                        print(signal_str)

                    #signal_str += '\n'
                    #f.write(signal_str)


            #f.close()

            if self.save_datas_excel:
                Wrap_Util.SaveExcelFiles(file='.\\pickle\\model_signal_excel_target_index_%s_simulation_term_type_%s_target_date_%s_window_size_%s.xlsx'
                                              % (self.target_index_nm, self.simulation_term_type, self.profit_calc_end_date, self.window_size)
                                         , obj_dict={'target_index': self.raw_data[index_nm][self.window_size - 1:], 'factor_signal_data': model_signal_data
                                         , 'average_zscore_data': average_zscore_data, 'max_zscore_data': max_zscore_data, 'result_data': result_data, 'corr': self.corr, 'zscore_data': self.zscore_data})

        return True


    def MakeSignal_AllCombis(self):

        if self.make_folione_signal == True:

            # Wrap운용팀 DB Connect
            db = WrapDB()
            db.connet(host="127.0.0.1", port=3306, database="WrapDB_1", user="root", password="ryumaria")
            factors_nm_cd_map = db.get_factors_nm_cd()

            if self.save_datas_excel:
                model_signal_data = pd.DataFrame(index=self.zscore_data.index)
                average_zscore_data = copy.deepcopy(model_signal_data)
                max_zscore_data = copy.deepcopy(model_signal_data)

                rst_idx_str = ["Window Size", "Target Index", "시작일", "마지막일", "기간(일)", "시그널", "누적 모델 수익률(연환산)", "누적 BM 수익률(연환산)", "누적 모델 수익률", "누적 BM 수익률", "펙터(#)", "펙터"]
                result_data = pd.DataFrame(index = rst_idx_str)


            # Signal 결과 저장
            #f = open(".\\pickle\\Signal_target_index_%s_simulation_term_type_%s_window_size_%s_From %s To %s.txt" % (self.target_index_nm, self.simulation_term_type, self.window_size, self.profit_calc_start_date,self.profit_calc_end_date), 'w')
            idx_str = "Window Size" + '\t' + "Target Index" + '\t' + "시작일" + '\t' + "마지막일" + '\t' + "기간(일)" + '\t' + "시그널" + '\t' + "누적 모델 수익률(연환산)" + '\t' \
                      + "누적 BM 수익률(연환산)" + '\t' + "누적 모델 수익률" + '\t' + "누적 BM 수익률" + '\t' + "펙터(#)" + '\t' + "펙터" + '\n'
            #f.write(idx_str)

            # 병렬처리 아닌 경우 로그 프린트
            if self.use_parallel_process == False:
                print(idx_str)
            
            # factor 예측 모형에서 사용되는 최대 factor 갯수는 10
            max_signal_factors_num = 10
            index_nm = self.target_index_nm


            # 결과 DB 저장시 기존 생성 내용 삭제
            if self.save_signal_process_db == True:
                table_nm = "result"
                db.delete_folione_signal(table_nm, factors_nm_cd_map[index_nm], self.profit_calc_start_date, self.profit_calc_end_date, self.window_size)

            if self.save_signal_last_db == True:
                table_nm = "result_last"
                db.delete_folione_signal(table_nm, factors_nm_cd_map[index_nm], self.profit_calc_start_date, self.profit_calc_end_date, self.window_size)
                

            # 1단계. 예측 index별로 container 생성
            self.model_signals[index_nm] = {}

			# reverse가 True이면 내림차순, False이면 올림차순
            model_profitable_factors_sorted = dict(sorted(self.model_accumulated_profits[index_nm].items(), key=operator.itemgetter(1), reverse=True))
            
            # combination factor 갯수
            for ele_count in range(1, max_signal_factors_num+1):

                combis = list(itertools.combinations(list(model_profitable_factors_sorted)[:10], ele_count))

                # 특정 갯수로 만들어질 수 있는 Combination 리스트
                for combi in combis:

                    # 동일 리스트의 경우 순서에 의한 문제 제거(DB Key 에러 문제)
                    combi = sorted(combi)

                    # 수익률 관련 메타 정보 저장
                    check_first_data = False
                    profit_start_date = '1111-11-11'
                    profit_end_date = '9999-99-99'

                    signal_factors_nm = ""
                    signal_factors_list = []
                    for profitable_factor in combi:
                        if len(signal_factors_list):
                            signal_factors_nm = signal_factors_nm + " & " + profitable_factor
                        else:
                            signal_factors_nm = profitable_factor
                        signal_factors_list.append(profitable_factor)

                    # 2단계. 예측 index & factor combination별로 container 생성
                    self.model_signals[index_nm][signal_factors_nm] = {}

                    if self.save_datas_excel:
                        model_signal_data[signal_factors_nm] = 0
                        average_zscore_data[signal_factors_nm] = 0
                        max_zscore_data[signal_factors_nm] = 0
                        result_data[signal_factors_nm] = None


                    # 모델을 이용한 누적수익률
                    accumulated_model_profit = 1.0
                    accumulated_bm_profit = 1.0

                    # Correlation lag 사용여부
                    # FACTOR LAG 사용: 1, 미사용: 0
                    use_factor_lag = 1
                    max_factor_lag = int(max(self.corr.transpose()['lag'].values)) if use_factor_lag else 0

                    new_point = self.weight_check_term - 1
                    average_array = [0] * self.weight_check_term
                    for idx, row_nm in enumerate(self.zscore_data.index):
                        try:
                            # 과거 moving average 생성 및 시프트
                            # min_max_check_term 개수 만큼 raw 데이터가 생겨야 average 생성 가능
                            if idx >= (self.min_max_check_term - 1) + max_factor_lag:
                                average_array[:new_point] = average_array[-new_point:]

                                average_array[-1] = 0
                                # 다수 factor를 이용해 모델 예측하는 경우 factor들의 값을 더한 후 평균
                                for factor in signal_factors_list:
                                    factor_lag = int(self.corr[index_nm + "_" + factor]['lag']) if use_factor_lag else 0
                                    # 과거 Folione과 동일한 로직(중간값 개념), lag 개념 추가
                                    if 0:
                                        average_array[-1] += int(self.corr[index_nm + "_" + factor]['direction']) \
                                                             * (self.zscore_data[factor][idx - (self.min_max_check_term - 1) - factor_lag:idx - factor_lag + 1].min()
                                                                + self.zscore_data[factor][idx - (self.min_max_check_term - 1) - factor_lag:idx - factor_lag + 1].max()) / 2
                                    # 신규 Folione과 동일한 로직(평균 개념), lag 개념 추가
                                    else:
                                        average_array[-1] += int(self.corr[index_nm + "_" + factor]['direction']) * self.zscore_data[factor][idx - (self.min_max_check_term - 1) - factor_lag:idx - factor_lag + 1].mean()
                                average_array[-1] /= len(signal_factors_list)

                                # 수익률 계산 시작
                                # weight_check_term 개수 만큼 average 데이터가 생겨야 노이즈 검증 가능
                                if datetime.strptime(row_nm, '%Y-%m-%d').date() >= self.profit_calc_start_date and idx - (self.min_max_check_term - 1 + max_factor_lag) >= self.weight_check_term:

                                    # Test, Debug용, Window Size에 따라 누적수익률 시작점 확인
                                    if check_first_data == False:
                                        #print(self.window_size, index_nm, row_nm)
                                        profit_start_date = row_nm
                                        check_first_data = True

                                    # 조건 만족으로 BUY 포지션
                                    if 0:
                                        # 이번 signal의 위치에 맞게 주식비율 조절 매수
                                        ai_profit_rate = 0.0159 / 12  # 예탁이용료 1달 수익률
                                        buy_ratio = (average_array[new_point] - average_array.min()) / (average_array.max() - average_array.min())
                                        if buy_ratio >= 0.0:
                                            accumulated_model_profit *= (1 + (buy_ratio * (self.raw_data[index_nm][self.window_size + idx] / self.raw_data[index_nm][self.window_size + idx - 1] - 1) + (1 - buy_ratio) * ai_profit_rate))
                                    else:
                                        #if average_array[new_point] == max(average_array):
                                        if average_array[-1] == max(average_array):
                                            # z-score의 경우 raw data보다 window_size -1 만큼 적음. window_size부터 z-score 생성됨
                                            if self.window_size + idx < len(self.raw_data.index):
                                                accumulated_model_profit *= (self.raw_data[index_nm][self.window_size + idx] / self.raw_data[index_nm][self.window_size + idx - 1])

                                            # 3단계. 예측 index & factor combination & 시계열별로 signal을 가진다
                                            self.model_signals[index_nm][signal_factors_nm][row_nm] = "BUY"

                                            if self.save_datas_excel:
                                                model_signal_data[signal_factors_nm][row_nm] = 1
                                        else:
                                            self.model_signals[index_nm][signal_factors_nm][row_nm] = "SELL"
                                    profit_end_date = row_nm

                                    if self.save_datas_excel:
                                        average_zscore_data[signal_factors_nm][row_nm] = average_array[-1]
                                        max_zscore_data[signal_factors_nm][row_nm] = max(average_array)

                                    # z-score의 경우 raw data보다 window_size -1 만큼 적음. window_size부터 z-score 생성됨
                                    if self.window_size + idx < len(self.raw_data.index):
                                        accumulated_bm_profit *= (self.raw_data[index_nm][self.window_size + idx] / self.raw_data[index_nm][self.window_size + idx - 1])

                                    # 시그널 DB 저장
                                    if self.save_signal_process_db == True or (self.save_signal_last_db == True and row_nm == str(self.profit_calc_end_date)):
                                        date_info = {'start_dt': str(self.profit_calc_start_date),'end_dt': str(self.profit_calc_end_date), 'curr_dt': row_nm}
                                        target_cd = factors_nm_cd_map[index_nm]
                                        factor_info = {'factors_num': len(signal_factors_list),'multi_factors_nm': signal_factors_nm,'factors_cd': [factors_nm_cd_map[factor_nm] for factor_nm in signal_factors_list]}
                                        signal_cd = 1 if self.model_signals[index_nm][signal_factors_nm][row_nm] == "BUY" else 0
                                        etc = {'window_size': self.window_size, 'model_profit': float(accumulated_model_profit), 'bm_profit': float(accumulated_bm_profit), 'term_type': self.simulation_term_type}

                                        # 발생하는 모든 과정의 Signal을 저장
                                        if self.save_signal_process_db == True:
                                            table_nm = 'result'
                                            db.insert_folione_signal(table_nm, date_info, target_cd, factor_info, signal_cd, etc)

                                        # 마지막 Signal만 저장
                                        if self.save_signal_last_db == True and row_nm == str(self.profit_calc_end_date):
                                            table_nm = 'result_last'
                                            db.insert_folione_signal(table_nm, date_info, target_cd, factor_info, signal_cd, etc)

                                    # Scenario End Date, Exit
                                    if row_nm == self.profit_calc_end_date:
                                        break

                        except IndexError:
                            print("IndexError:\t", index_nm, '\t', signal_factors_nm, '\t', row_nm)
                            pass
                        except Exception as inst:
                            print(type(inst))  # the exception instance
                            print(inst.args)  # arguments stored in .args
                            print(inst)

                    # 유효 factor들의 combination을 이용하여
                    if self.save_datas_excel and accumulated_model_profit > accumulated_bm_profit:
                        profit_period = (datetime.strptime(profit_end_date, '%Y-%m-%d').date() - datetime.strptime(profit_start_date, '%Y-%m-%d').date()).days

                        result_data[signal_factors_nm]["Window Size"] = self.window_size
                        result_data[signal_factors_nm]["Target Index"] = index_nm
                        result_data[signal_factors_nm]["시작일"] = profit_start_date
                        result_data[signal_factors_nm]["마지막일"] = profit_end_date
                        result_data[signal_factors_nm]["기간(일)"] = profit_period
                        result_data[signal_factors_nm]["시그널"] = self.model_signals[index_nm][signal_factors_nm][profit_end_date]
                        result_data[signal_factors_nm]["누적 모델 수익률(연환산)"] = accumulated_model_profit / profit_period * 365
                        result_data[signal_factors_nm]["누적 BM 수익률(연환산)"] = accumulated_bm_profit / profit_period * 365
                        result_data[signal_factors_nm]["누적 모델 수익률"] = accumulated_model_profit
                        result_data[signal_factors_nm]["누적 BM 수익률"] = accumulated_bm_profit
                        result_data[signal_factors_nm]["펙터(#)"] = len(signal_factors_list)
                        result_data[signal_factors_nm]["펙터"] = signal_factors_nm

                        signal_str = str(self.window_size) + '\t' + index_nm + '\t' + profit_start_date + '\t' + profit_end_date + '\t' + str(profit_period) + '\t' + self.model_signals[index_nm][signal_factors_nm][profit_end_date] + '\t' \
                                     + str(accumulated_model_profit / profit_period * 365) + '\t' + str(accumulated_bm_profit / profit_period * 365) + '\t' \
                                     + str(accumulated_model_profit) + '\t' + str(accumulated_bm_profit) + '\t' + str(len(signal_factors_list))  + '\t' + signal_factors_nm

                        # 병렬처리 아닌 경우 로그 프린트
                        if self.use_parallel_process == False:
                            print(signal_str)

                        #signal_str += '\n'
                        #f.write(signal_str)

            #f.close()

            if self.save_datas_excel:
                Wrap_Util.SaveExcelFiles(file='.\\pickle\\model_all_combi_signal_excel_target_index_%s_simulation_term_type_%s_target_date_%s_window_size_%s.xlsx'
                                              % (self.target_index_nm, self.simulation_term_type, self.profit_calc_end_date, self.window_size)
                                         , obj_dict={'target_index': self.raw_data[index_nm][self.window_size - 1:], 'model_signal_data': model_signal_data
                                         , 'average_zscore_data': average_zscore_data, 'max_zscore_data': max_zscore_data, 'result_data': result_data, 'corr': self.corr, 'zscore_data': self.zscore_data})

            # Wrap운용팀 DB Disconnect
            db.disconnect()

        return True


    def CalcCorrelation(self):

        # 1단계: target index_factor, rolling month 중 최고 값 사용
        corr_direction = {}
        corr_lag = {}
        corr_max = {}

        if self.use_correlation_pickle == False:
            # rolling corr 계산

            if self.save_correlations_txt == True:
                f = open(".\\pickle\\rolling_corr_target_index_%s_simulation_term_type_%s_window_size_%s.txt" % (self.target_index_nm, self.simulation_term_type, self.window_size), 'w')

            # 상관관계를 계산할 때 raw data 또는 Z-Score를 사용할 지 선택
            # Raw Data = 0, Z-Score = 1
            use_data_type = 1
            using_data = self.zscore_data if use_data_type else self.raw_data

            # column_nm_1은 Target Index
            for column_nm_1 in using_data.columns:
                if column_nm_1 == self.target_index_nm:

                    if self.save_correlations_txt == True:
                        corr_mtrx_str = "Target Index\t" + str(column_nm_1) + "\n"
                        # print("Window Size:\t", str(self.window_size), "\t, \t", corr_mtrx_str)

                    self.rolling_correlations[column_nm_1] = {}

                    # column_nm_2은 Factor
                    for column_nm_2 in using_data.columns:

                        if self.save_correlations_txt == True:
                            corr_mtrx_str = corr_mtrx_str + column_nm_2 + "\t"

                        self.rolling_correlations[column_nm_1][column_nm_2] = {}

                        # Correlation을 통해 lag와 상관성(정,역) 확인
                        max_corr = 0.0
                        max_lag_idx = 0
                        # 0 ~ 3개월 까지 Factor와 Target Index에 lag 적용 테스트
                        max_lag_term = 3
                        for rolling_month in range(max_lag_term + 1):

                            # 문법상 첫번째 구간(Factor와 Target Index의 lag 없이 동일 시점 적용)은 그냥 처리해야 함
                            if rolling_month == 0:
                                # pandas의 correation 계산이 안돼 numpy로 변경, 향후 오류 원인 확인 필요
                                self.rolling_correlations[column_nm_1][column_nm_2][rolling_month] = numpy.corrcoef(using_data[column_nm_1][-self.window_size:].tolist(), using_data[column_nm_2][-self.window_size:].tolist())[0][1]
                            else:
                                # pandas의 correation 계산이 안돼 numpy로 변경, 향후 오류 원인 확인 필요
                                self.rolling_correlations[column_nm_1][column_nm_2][rolling_month] = numpy.corrcoef(using_data[column_nm_1][-self.window_size:].tolist(), using_data[column_nm_2][-(self.window_size + rolling_month):-rolling_month].tolist())[0][1]

                            # Correlation을 통해 lag와 상관성(정,역) 확인
                            if math.isnan(self.rolling_correlations[column_nm_1][column_nm_2][rolling_month]) == False:
                                if abs(self.rolling_correlations[column_nm_1][column_nm_2][rolling_month]) > abs(max_corr):
                                    max_corr = self.rolling_correlations[column_nm_1][column_nm_2][rolling_month]
                                    max_lag_idx = rolling_month

                                    corr_direction[column_nm_1 + "_" + column_nm_2] = 1 if max_corr > 0.0 else -1
                                    corr_lag[column_nm_1 + "_" + column_nm_2] = max_lag_idx
                                    corr_max[column_nm_1 + "_" + column_nm_2] = max_corr
                                    # print(column_nm_1 + "_" + column_nm_2, '\t', max_lag_idx, '\t', max_corr)

                            # 1년 단위로 값이 변경되는 Factor의 경우 corr가 nan이 나오는 경우 있어 초기화 시킴
                            elif math.isnan(self.rolling_correlations[column_nm_1][column_nm_2][rolling_month]) == True and (max_corr == 0.0 and max_lag_idx == 0):
                                corr_direction[column_nm_1 + "_" + column_nm_2] = 1
                                corr_lag[column_nm_1 + "_" + column_nm_2] = max_lag_idx
                                corr_max[column_nm_1 + "_" + column_nm_2] = max_corr

                            if self.save_correlations_txt == True:
                                corr_mtrx_str = corr_mtrx_str + str(self.rolling_correlations[column_nm_1][column_nm_2][rolling_month]) + "\t"

                        if self.save_correlations_txt == True:
                            corr_mtrx_str = corr_mtrx_str + "\n"

                    if self.save_correlations_txt == True:
                        corr_mtrx_str = corr_mtrx_str + "\n"
                        f.write(corr_mtrx_str)
                        #print(corr_mtrx_str)

            if self.save_correlations_txt == True:
                f.close()

            # Corr 정보 생성
            self.corr = pd.DataFrame(data=[corr_direction, corr_lag, corr_max], index=['direction','lag','max'])


            Wrap_Util.SavePickleFile(file='.\\pickle\\rolling_corr_target_index_%s_simulation_term_type_%s_target_date_%s_window_size_%s.pickle'
                                          % (self.target_index_nm, self.simulation_term_type, self.profit_calc_end_date, self.window_size), obj=self.rolling_correlations)

            if self.save_datas_excel:
                Wrap_Util.SaveExcelFiles(file='.\\pickle\\rolling_corr_excel_target_index_%s_simulation_term_type_%s_target_date_%s_window_size_%s.xlsx'
                                              % (self.target_index_nm, self.simulation_term_type, self.profit_calc_end_date, self.window_size) , obj_dict={'corr': self.corr})

        else:
            self.rolling_correlations = Wrap_Util.ReadPickleFile(file='.\\pickle\\rolling_corr_target_index_%s_simulation_term_type_%s_target_date_%s_window_size_%s.pickle'
                                                                      % (self.target_index_nm, self.simulation_term_type, self.profit_calc_end_date, self.window_size))

            # Factor와 Target Index의 상관성 확인
            index_nm = self.target_index_nm
            for column_nm in self.zscore_data.columns:

                # Correlation을 통해 lag와 상관성(정,역) 확인
                max_corr = 0.0
                max_lag_idx = 0
                for lag_num in self.rolling_correlations[index_nm][column_nm]:
                    if abs(self.rolling_correlations[index_nm][column_nm][lag_num]) > abs(max_corr):
                        max_corr = self.rolling_correlations[index_nm][column_nm][lag_num]
                        max_lag_idx = lag_num
                corr_direction[index_nm + "_" + column_nm] = 1 if max_corr > 0.0 else -1
                corr_lag[index_nm + "_" + column_nm] = max_lag_idx
                corr_max[index_nm + "_" + column_nm] = max_corr
                # print(index_nm + "_" + column_nm, '\t', max_lag_idx, '\t', max_corr)

            self.corr = pd.DataFrame(data=[corr_direction, corr_lag, corr_max],index=['direction', 'lag', 'max'])

        return True