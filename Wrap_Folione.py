#_*_ coding: utf-8 _*_

import copy
from datetime import datetime
from datetime import timedelta
import operator
import pandas as pd

import Wrap_Util


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


    def SetDataInfo(self, data_info, data_info_columns):

        self.data_info = data_info
        self.data_info.columns = data_info_columns


    def MakeDateRange(self, start_date, last_date):

        for ele in self.data_info:
            if ele in (start_date, last_date):
                if ele == start_date:
                    self.date_range[ele] = "1111-11-11"
                elif ele == last_date:
                    self.date_range[ele] = "9999-99-99"
                else:
                    continue

                for idx in range(0, len(self.data_info[ele])):
                    if self.data_info[ele][idx]:
                        if ele == start_date and int(self.date_range[ele].replace("-", "")) < int(self.data_info[ele][idx].replace("-", "")):
                            self.date_range[ele] = self.data_info[ele][idx]
                        if ele == last_date and int(self.date_range[ele].replace("-", "")) > int(self.data_info[ele][idx].replace("-", "")):
                            self.date_range[ele] = self.data_info[ele][idx]


    def GetDataList(self, item_cd, start_date, last_date):

        data_list = list(self.data_info[item_cd])
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


    def FillValidData(self, look_back_days):

        for column_nm in self.pivoted_sampled_datas.columns:
            for row_nm in self.pivoted_sampled_datas.index:
                if self.pivoted_sampled_datas[column_nm][row_nm] == None:
                    # print (column_nm, "\t", row_nm, "\t", pivoted_sampled_datas[column_nm][row_nm])

                    # ref_row_nm = copy.copy(row_nm)
                    ref_row_nm = row_nm

                    # 해당일에 데이터가 없는 경우 가장 최근 값을 대신 사용함
                    for loop_cnt in range(look_back_days):
                        try:
                            if self.pivoted_reference_datas[column_nm][ref_row_nm] == None:
                                # print("No Data", str(ref_row_nm))
                                ref_row_nm = str(datetime.strptime(ref_row_nm, '%Y-%m-%d').date() - timedelta(days=1))
                            else:
                                self.pivoted_sampled_datas[column_nm][row_nm] = self.pivoted_reference_datas[column_nm][
                                    ref_row_nm]
                        except KeyError:
                            # print("KeyError", str(ref_row_nm))
                            ref_row_nm = str(datetime.strptime(ref_row_nm, '%Y-%m-%d').date() - timedelta(days=1))

                # 이후 연산작업을 위해 decimal을 float 형태로 변경
                if self.pivoted_sampled_datas[column_nm][row_nm] != None:
                    self.pivoted_sampled_datas[column_nm][row_nm] = float(self.pivoted_sampled_datas[column_nm][row_nm])


    def DropInvalidData(self, drop_basis_from, drop_basis_to):

        # 유효하지 않은 기간 drop
        drop_basis_from = datetime.strptime(drop_basis_from, '%Y-%m-%d').date()
        drop_basis_to = datetime.strptime(drop_basis_to, '%Y-%m-%d').date()
        pivoted_sampled_datas_cp = copy.deepcopy(self.pivoted_sampled_datas)
        for row_nm in pivoted_sampled_datas_cp.index:
            data_time = datetime.strptime(row_nm, '%Y-%m-%d').date()
            if data_time < drop_basis_from or data_time > drop_basis_to:
                # print (row_nm)
                self.pivoted_sampled_datas.drop(index=row_nm, inplace=True)

        # 유효하지 않은 팩터 drop
        total_omission_threshold = 1
        last_omission_threshold = 1
        last_considerable_num = 6
        pivoted_sampled_datas_cp = copy.deepcopy(self.pivoted_sampled_datas)
        for column_nm in pivoted_sampled_datas_cp.columns:
            total_null_cnt = pivoted_sampled_datas_cp[column_nm].isnull().sum()
            last_null_cnt = pivoted_sampled_datas_cp[column_nm][-last_considerable_num:].isnull().sum()

            if total_null_cnt > total_omission_threshold or last_null_cnt > last_omission_threshold:
                # print('유효하지 않은 누락\t', column_nm, '\t', total_null_cnt, '\t', last_null_cnt)
                self.pivoted_sampled_datas.drop(columns=column_nm, inplace=True)
            elif total_null_cnt:
                # print('유효한 누락\t', column_nm, '\t', total_null_cnt, '\t', last_null_cnt)
                for idx, row_nm in enumerate(self.pivoted_sampled_datas.index):
                    if self.pivoted_sampled_datas[column_nm][row_nm] == None:
                        if idx == 0:
                            self.pivoted_sampled_datas[column_nm][idx] = self.pivoted_sampled_datas[column_nm][idx + 1]
                        else:
                            self.pivoted_sampled_datas[column_nm][idx] = self.pivoted_sampled_datas[column_nm][idx - 1]

        return self.pivoted_sampled_datas


class Folione (object):

    def __init__(self, raw_data, window_size, profit_calc_start_date, min_max_check_term, weight_check_term, target_index_nm_list
                 , use_window_size_pickle=False, use_factor_selection_pickle=False, use_correlation_pickle=False
                 , make_folione_signal = False
                 , save_datas_excel=False, save_correlations_txt=False):

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

        # Folione 고유 파라미터
        self.window_size = copy.deepcopy(window_size) # Z-Score 만들기 위한 기간
        self.profit_calc_start_date = datetime.strptime(profit_calc_start_date, '%Y-%m-%d').date() # Factor 별 누적 수익률 시작일 (장/단기에 따라 변동)
        self.target_index_nm_list = copy.deepcopy(target_index_nm_list) # target index, Ex. "MSCI ACWI", "MSCI World", "MSCI EM", "KOSPI", "S&P500", "Nikkei225", "상해종합"
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


    def Start(self):

        self.MakeZScore()
        self.CalcCorrelation()
        self.SelectFactor()
        self.MakeSignal()


    def MakeZScore(self):

        if self.use_window_size_pickle == False:
            self.mean_data = copy.deepcopy(self.raw_data.rolling(window=self.window_size, center=False).mean())
            self.std_data = copy.deepcopy(self.raw_data.rolling(window=self.window_size, center=False).std())
            raw_datas_cp = copy.deepcopy(self.raw_data)
            for idx, row_nm in enumerate(raw_datas_cp.index):

                # 평균 및 표준편차 데이터가 생성되지 않는 구간 삭제
                drop_point = self.window_size - 1
                if idx < drop_point:
                    self.raw_data.drop(index=row_nm, inplace=True)
                    self.mean_data.drop(index=row_nm, inplace=True)
                    self.std_data.drop(index=row_nm, inplace=True)
                else:
                    break

            # Z-Score 계산
            # 데이터의 변화가 없어 표준편차가 0(ZeroDivisionError)인 경우 때문에 DataFrame을 이용한 연산처리 불가
            self.zscore_data = copy.deepcopy(self.raw_data)
            for column_nm in self.zscore_data.columns:
                for row_nm in self.zscore_data.index:
                    try:
                        self.zscore_data[column_nm][row_nm] = (self.raw_data[column_nm][row_nm] - self.mean_data[column_nm][row_nm]) / self.std_data[column_nm][row_nm]
                    except ZeroDivisionError:
                        self.zscore_data[column_nm][row_nm] = 0.0

            Wrap_Util.SavePickleFile(file='.\\pickle\\pivoted_sampled_datas_zscore_window_size_%s.pickle' % (self.window_size), obj=self.zscore_data)

            if self.save_datas_excel:
                Wrap_Util.SaveExcelFiles(file='.\\pickle\\save_datas_excel_window_size_%s.xlsx' % (self.window_size), obj_dict={'raw_data': self.raw_data, 'mean_data': self.mean_data, 'std_data': self.std_data, 'zscore_data': self.zscore_data})

        else:
            self.zscore_data = Wrap_Util.ReadPickleFile(file='.\\pickle\\pivoted_sampled_datas_zscore_window_size_%s.pickle' % (self.window_size))

        return True


    def SelectFactor(self):

        if self.use_factor_selection_pickle == False:

            for index_nm in self.target_index_nm_list:
                # 누적 수익률 저장 공간
                self.model_accumulated_profits[index_nm] = {}
                self.bm_accumulated_profits[index_nm] = {}

                for column_nm in self.zscore_data.columns:
                    if column_nm != index_nm:
                        # 누적 수익률 초기화
                        self.model_accumulated_profits[index_nm][column_nm] = 0.0
                        self.bm_accumulated_profits[index_nm][column_nm] = 0.0

                        # 주식 Buy, Sell 포지션 판단
                        new_point = self.min_max_check_term - 1
                        average_array = [0] * self.min_max_check_term
                        for idx, row_nm in enumerate(self.zscore_data.index):
                            try:
                                # 과거 moving average 생성 및 시프트
                                # min_max_check_term 개수 만큼 raw 데이터가 생겨야 average 생성 가능
                                if idx >= new_point:
                                    average_array[:new_point] = average_array[-new_point:]
                                    average_array[new_point] = (self.zscore_data[column_nm][idx - new_point:idx + 1].min() + self.zscore_data[column_nm][idx - new_point:idx + 1].max()) / 2

                                    # 수익률 계산 시작
                                    # weight_check_term 개수 만큼 average 데이터가 생겨야 노이즈 검증 가능
                                    if datetime.strptime(row_nm,'%Y-%m-%d').date() >= self.profit_calc_start_date and idx - new_point >= self.weight_check_term:
                                        # 조건 만족으로 BUY 포지션
                                        if average_array[new_point] == max(average_array):
                                            self.model_accumulated_profits[index_nm][column_nm] += self.raw_data[index_nm][idx + 1] / self.raw_data[index_nm][idx] - 1
                                        self.bm_accumulated_profits[index_nm][column_nm] += self.raw_data[index_nm][idx + 1] / self.raw_data[index_nm][idx] - 1
                                        # print(index_nm, '\t', column_nm, '\t', row_nm, '\t', model_accumulated_profits, '\t', bm_accumulated_profits)
                            except IndexError:
                                # print("IndexError:\t", index_nm, '\t', column_nm, '\t', row_nm)
                                pass

                        # 모델의 성능이 BM 보다 좋은 팩터 결과만 출력
                        if self.model_accumulated_profits[index_nm][column_nm] > self.bm_accumulated_profits[index_nm][column_nm]:
                            print(self.window_size, '\t', index_nm, '\t', column_nm, '\t',
                                  self.model_accumulated_profits[index_nm][column_nm], '\t',
                                  self.bm_accumulated_profits[index_nm][column_nm], '\t',
                                  self.model_accumulated_profits[index_nm][column_nm] / self.bm_accumulated_profits[index_nm][column_nm] - 1)

            Wrap_Util.SavePickleFile(file='.\\pickle\\model_accumulated_profits_window_size_%s.pickle' % (self.window_size), obj=self.model_accumulated_profits)
            Wrap_Util.SavePickleFile(file='.\\pickle\\bm_accumulated_profits_window_size_%s.pickle' % (self.window_size), obj=self.bm_accumulated_profits)

        else:
            self.model_accumulated_profits = Wrap_Util.ReadPickleFile(file='.\\pickle\\model_accumulated_profits_window_size_%s.pickle' % (self.window_size))
            self.bm_accumulated_profits = Wrap_Util.ReadPickleFile(file='.\\pickle\\bm_accumulated_profits_window_size_%s.pickle' % (self.window_size))

        return True


    def MakeSignal(self):

        if self.make_folione_signal == True:
            # factor 예측 모형에서 사용되는 최대 factor 갯수는 10
            max_simulate_factor_num = 10
            for index_nm in self.target_index_nm_list:

                # 1단계. 예측 index별로 container 생성
                self.model_signals[index_nm] = {}

                model_profitable_factors_sorted = dict(sorted(self.model_accumulated_profits[index_nm].items(), key=operator.itemgetter(1), reverse=True))

                signal_factors_nm = ""
                simulate_factor_list = []
                for profitable_factor in model_profitable_factors_sorted:

                    # 최대 factor 갯수는 10개까지 테스트 & BM보다 좋은 수익률을 내는 factor
                    if len(simulate_factor_list) <= max_simulate_factor_num and self.model_accumulated_profits[index_nm][profitable_factor] > self.bm_accumulated_profits[index_nm][profitable_factor]:
                        if len(simulate_factor_list):
                            signal_factors_nm = signal_factors_nm + " & " + profitable_factor
                        else:
                            signal_factors_nm = profitable_factor
                        simulate_factor_list.append(profitable_factor)

                        # 2단계. 예측 index & factor combination별로 container 생성
                        self.model_signals[index_nm][signal_factors_nm] = {}
                    else:
                        break


                    # 모델을 이용한 누적수익률
                    accumulated_model_profit = 0.0
                    accumulated_bm_profit = 0.0

                    new_point = self.min_max_check_term - 1
                    average_array = [0] * self.min_max_check_term
                    for idx, row_nm in enumerate(self.zscore_data.index):
                        try:
                            # 과거 moving average 생성 및 시프트
                            # min_max_check_term 개수 만큼 raw 데이터가 생겨야 average 생성 가능
                            if idx >= new_point:
                                average_array[:new_point] = average_array[-new_point:]

                                tmp_array = [0] * self.min_max_check_term
                                # 다수 factor를 이용해 모델 예측하는 경우 factor들 min, max 값을 더한 후 평균
                                for factor in simulate_factor_list:
                                    tmp_array += self.zscore_data[factor][idx - new_point:idx + 1]
                                average_array[new_point] = ((min(tmp_array) + max(tmp_array)) / 2) / len(simulate_factor_list)

                                # 수익률 계산 시작
                                # weight_check_term 개수 만큼 average 데이터가 생겨야 노이즈 검증 가능
                                if datetime.strptime(row_nm, '%Y-%m-%d').date() >= self.profit_calc_start_date and idx - new_point >= self.weight_check_term:

                                    # 조건 만족으로 BUY 포지션
                                    if average_array[new_point] == max(average_array):
                                        accumulated_model_profit += self.raw_data[index_nm][idx + 1] / self.raw_data[index_nm][idx] - 1

                                        # 3단계. 예측 index & factor combination & 시계열별로 signal을 가진다
                                        self.model_signals[index_nm][signal_factors_nm][row_nm] = "BUY"
                                    else:
                                        self.model_signals[index_nm][signal_factors_nm][row_nm] = "SELL"
                                    accumulated_bm_profit += self.raw_data[index_nm][idx + 1] / self.raw_data[index_nm][idx] - 1

                        except IndexError:
                            # print("IndexError:\t", index_nm, '\t', column_nm, '\t', row_nm)
                            pass

                    # 유효 factor들의 combination을 이용하여
                    print(self.window_size, '\t', index_nm, '\t', signal_factors_nm, '\t', accumulated_model_profit, '\t', accumulated_bm_profit)

        return True


    def CalcCorrelation(self):

        if self.use_correlation_pickle == False:
            # rolling corr 계산

            if self.save_correlations_txt == True:
                f = open(".\\pickle\\rolling_corr_window_size_%s.txt" % (self.window_size), 'w')

            for rolling_month in range(10):

                self.rolling_correlations[rolling_month] = {}

                if self.save_correlations_txt == True:
                    corr_mtrx_str = "Rolling Month\t" + str(rolling_month) + "\n"
                    f.write(corr_mtrx_str)
                    print("Window Size:\t", str(self.window_size), "\t, \t", corr_mtrx_str)

                for column_nm_1 in self.raw_data.columns:
                    if column_nm_1 in self.target_index_nm_list:

                        self.rolling_correlations[rolling_month][column_nm_1] = {}

                        corr_mtrx_str = column_nm_1 + "\t"
                        for column_nm_2 in self.raw_data.columns:
                            # 문법상 첫번째 구간은 그냥 처리해야 함
                            if rolling_month:
                                #self.rolling_correlations[rolling_month][column_nm_1][column_nm_2] = self.raw_data[column_nm_1][rolling_month:].corr(self.raw_data[column_nm_2][:-rolling_month], method='spearman')
                                self.rolling_correlations[rolling_month][column_nm_1][column_nm_2] = self.raw_data[column_nm_1][-self.window_size:].corr(self.raw_data[column_nm_2][-(self.window_size+rolling_month):-rolling_month], method='spearman')
                            else:
                                #self.rolling_correlations[rolling_month][column_nm_1][column_nm_2] = self.raw_data[column_nm_1].corr(self.raw_data[column_nm_2], method='spearman')
                                self.rolling_correlations[rolling_month][column_nm_1][column_nm_2] = self.raw_data[column_nm_1][-self.window_size:].corr(self.raw_data[column_nm_2][-self.window_size:], method='spearman')

                            if self.save_correlations_txt == True:
                                corr_mtrx_str = corr_mtrx_str + str(self.rolling_correlations[rolling_month][column_nm_1][column_nm_2]) + "\t"

                        if self.save_correlations_txt == True:
                            corr_mtrx_str = corr_mtrx_str + "\n"
                            f.write(corr_mtrx_str)

            if self.save_correlations_txt == True:
                f.close()

            Wrap_Util.SavePickleFile(file='.\\pickle\\rolling_corr_window_size_%s.pickle' % (self.window_size), obj=self.rolling_correlations)
        else:
            self.rolling_correlations = Wrap_Util.ReadPickleFile(file='.\\pickle\\rolling_corr_window_size_%s.pickle' % (self.window_size))

        return True