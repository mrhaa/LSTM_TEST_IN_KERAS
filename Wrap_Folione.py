#_*_ coding: utf-8 _*_

import copy
from datetime import datetime
from datetime import timedelta

import Wrap_Util


def worker(obj):
    obj.run()


class Folione (object):

    def __init__(self, raw_data, window_size, profit_calc_start_date, min_max_check_term, weight_check_term, target_index_nm_list, use_window_size_pickle=False, use_factor_selection_pickle=False, save_datas_excel=False):

        self.raw_data = copy.deepcopy(raw_data)
        self.mean_data = None
        self.std_data = None
        self.zscore_data = None

        self.model_cumulated_profit = {}
        self.bm_cumulated_profit = {}

        self.window_size = copy.deepcopy(window_size)
        self.profit_calc_start_date = copy.deepcopy(profit_calc_start_date)
        self.target_index_nm_list = copy.deepcopy(target_index_nm_list)
        self.min_max_check_term = copy.deepcopy(min_max_check_term)
        self.weight_check_term = copy.deepcopy(weight_check_term)

        self.use_window_size_pickle = copy.deepcopy(use_window_size_pickle)
        self.use_factor_selection_pickle = copy.deepcopy(use_factor_selection_pickle)
        self.save_datas_excel = copy.deepcopy(save_datas_excel)


    def run(self):

        self.MakeZScore()
        self.SelectFactor()


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

            Wrap_Util.SavePickleFile(file='pivoted_sampled_datas_zscore_window_size_%s.pickle' % (self.window_size), obj=self.zscore_data)

        else:
            self.zscore_data = Wrap_Util.ReadPickleFile(file='pivoted_sampled_datas_zscore_window_size_%s.pickle' % (self.window_size))


        if self.save_datas_excel:
            Wrap_Util.SaveExcelFiles(file='save_datas_excel_window_size_%s.xlsx' % (self.window_size), obj_dict={'raw_data': self.raw_data, 'mean_data': self.mean_data, 'std_data': self.std_data, 'zscore_data': self.zscore_data})


    def SelectFactor(self):

        if self.use_factor_selection_pickle == False:
            profit_calc_start_time = datetime.strptime(self.profit_calc_start_date, '%Y-%m-%d').date()

            for index_nm in self.target_index_nm_list:
                # 누적 수익률 저장 공간
                self.model_cumulated_profit[index_nm] = {}
                self.bm_cumulated_profit[index_nm] = {}

                for column_nm in self.zscore_data.columns:
                    if column_nm != index_nm:
                        # 누적 수익률 초기화
                        self.model_cumulated_profit[index_nm][column_nm] = 0.0
                        self.bm_cumulated_profit[index_nm][column_nm] = 0.0

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
                                    if datetime.strptime(row_nm,'%Y-%m-%d').date() >= profit_calc_start_time and idx - new_point >= self.weight_check_term:
                                        # 조건 만족으로 BUY 포지션
                                        if average_array[new_point] == max(average_array):
                                            self.model_cumulated_profit[index_nm][column_nm] += self.raw_data[index_nm][idx + 1] / self.raw_data[index_nm][idx] - 1
                                        self.bm_cumulated_profit[index_nm][column_nm] += self.raw_data[index_nm][idx + 1] / self.raw_data[index_nm][idx] - 1
                                        # print(index_nm, '\t', column_nm, '\t', row_nm, '\t', model_cumulated_profit, '\t', bm_cumulated_profit)
                            except IndexError:
                                # print("IndexError:\t", index_nm, '\t', column_nm, '\t', row_nm)
                                pass

                        # 모델의 성능이 BM 보다 좋은 팩터 결과만 출력
                        if self.model_cumulated_profit[index_nm][column_nm] > self.bm_cumulated_profit[index_nm][column_nm]:
                            print(self.window_size, '\t', index_nm, '\t', column_nm, '\t',
                                  self.model_cumulated_profit[index_nm][column_nm], '\t',
                                  self.bm_cumulated_profit[index_nm][column_nm], '\t',
                                  self.model_cumulated_profit[index_nm][column_nm] / self.bm_cumulated_profit[index_nm][column_nm] - 1)

            Wrap_Util.SavePickleFile(file='model_cumulated_profit_window_size_%s.pickle' % (self.window_size), obj=self.model_cumulated_profit)
            Wrap_Util.SavePickleFile(file='bm_cumulated_profit_window_size_%s.pickle' % (self.window_size), obj=self.bm_cumulated_profit)

        else:
            self.model_cumulated_profit = Wrap_Util.ReadPickleFile(file='model_cumulated_profit_window_size_%s.pickle' % (self.window_size))
            self.bm_cumulated_profit = Wrap_Util.ReadPickleFile(file='bm_cumulated_profit_window_size_%s.pickle' % (self.window_size))


