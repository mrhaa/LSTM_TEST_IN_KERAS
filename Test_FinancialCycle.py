#_*_ coding: utf-8 _*_

#import win32com.client

import os
import platform
import copy

import pandas as pd
import numpy as np
import math
from scipy.optimize import minimize

from Test_MariaDB import WrapDB
from Test_Figure import Figure
import Wrap_Util

PRINT_SC = False

base_dir = (os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))


def maximize_profit(right_up_case, right_down_case, macro_list, index_list, timeseries,lb=0.00, ub=0.1):

    def profit(x, args):
        right_sum = args

        return 1/sum(x * right_sum)

    def sum_weight(x):
        return 1-sum(x)

    macro_cnt = len(macro_list)
    weights_list = {}
    for index_cd in index_list:
        right_up_sum = np.repeat(0, macro_cnt)
        right_down_sum = np.repeat(0, macro_cnt)
        for idx, macro_cd in enumerate(macro_list):
            for time_cd in timeseries:
                if math.isnan(right_up_case[macro_cd][index_cd][time_cd]) == False:
                    right_up_sum[idx] += 1
                if math.isnan(right_down_case[macro_cd][index_cd][time_cd]) == False:
                    right_down_sum[idx] += 1

        x0 = np.repeat(1 / macro_cnt, macro_cnt)
        lbound = np.repeat(lb, macro_cnt)
        ubound = np.repeat(ub, macro_cnt)
        bnds = tuple(zip(lbound, ubound))
        constraints = {'type': 'eq', 'fun': sum_weight}
        options = {'ftol': 1e-20, 'maxiter': 5000, 'disp': False}

        result = minimize(fun=profit,
                          x0=x0,
                          args=(right_up_sum+right_down_sum),
                          method='SLSQP',
                          constraints=constraints,
                          options=options,
                          bounds=bnds)

        weights_list[index_cd] = [round(weight, 2) for weight in result.x]

    return weights_list

class FinancialCycle(object):

    def __init__(self, db):
        self.db = db

        self.macro_master_df = None
        self.macro_value_df = None
        self.pivoted_macro_value_df = None
        self.macro_cnt = 0
        self.macro_len = 0
        self.macro_list = None
        self.macro_timeseries = None
        self.macro_last_df = None

        self.index_master_df = None
        self.index_value_df = None
        self.pivoted_index_value_df = None
        self.index_cnt = 0
        self.index_len = 0
        self.index_list = None
        self.index_timeseries = None

        self.pivoted_macro_status_df = None # 해당 월의 값은 지수 데이터의 다음 월이랑 매핑, shift df를 이용해 로직 적용
        self.pivoted_macro_momentum_df = None # 해당 월의 값은 지수 데이터의 다음 월이랑 매핑, shift df를 이용해 로직 적용
        self.pivoted_macro_status_shift_df = {} # shift의 단위는 M이고 기간이 동적으로 변경될 수 있음. 기간을 dictionary의 key로 사용
        self.pivoted_macro_momentum_shift_df = {} # shift의 단위는 M이고 기간이 동적으로 변경될 수 있음. 기간을 dictionary의 key로 사용

        self.pivoted_index_direction_df = None # 해당 월의 방향은 해당 row에 저장
        self.pivoted_index_yield_df = None # 해당 월의 쉬익률은 해당 row에 저장

        # 매크로 데이터의 속성과 지수 데이터의 움직임 관계를 통계냄
        self.macro_status_index_direction_relation_right_df = None
        self.macro_status_index_direction_relation_up_right_df = None
        self.macro_status_index_direction_relation_down_right_df = None

        self.macro_momentum_index_direction_relation_right_df = None
        self.macro_momentum_index_direction_relation_up_right_df = None
        self.macro_momentum_index_direction_relation_down_right_df = None

        self.macro_status_momentum_index_direction_relation_right_df = None
        self.macro_status_momentum_index_direction_relation_up_right_df = None
        self.macro_status_momentum_index_direction_relation_down_right_df = None

        # 매크로 데이터의 속성과 지수 데이터의 움직임 관계를 시계열로 관리
        # 해당 row에 있는 값은 해당 달의 값을 의미한다. (ex. 2/28: 2월에 맞았는지를 의미, 2월의 지수 움직임과 1월의 매크로 통계값)
        # 그래프에 지수와 같이 보여주기 위해서는 position이 1이 되어야 함
        self.momentum_direction_up_right_series = {}
        self.momentum_direction_down_right_series = {}

    def get_macro_master(self):
        # 매크로 시계열 데이터 셋
        sql = "select a.cd as cd, a.nm as nm, a.ctry as ctry, a.base as base" \
              "  from macro_master a" \
              "     , macro_value b" \
              " where a.base is not null" \
              "   and a.cd = b.cd" \
              " group by a.cd, a.nm, a.ctry, a.base" \
              " having count(*) > 0"
        self.macro_master_df = self.db.select_query(sql)
        self.macro_master_df.columns = ('cd', 'nm', 'ctry', 'base')
        self.macro_master_df.set_index('cd', inplace=True)

    def get_macro_value(self):
        sql = "select cd, date, value" \
              "  from macro_value"
        self.macro_value_df = self.db.select_query(sql)
        self.macro_value_df.columns = ('cd', 'date', 'value')
        self.pivoted_macro_value_df = self.macro_value_df.pivot(index='date', columns='cd', values='value')

        self.set_macro_cnt()
        self.set_macro_len()
        self.set_macro_list()
        self.set_macro_timeseries()

    def set_macro_cnt(self):
        self.macro_cnt = len(self.pivoted_macro_value_df.columns)

    def set_macro_len(self):
        self.macro_len = len(self.pivoted_macro_value_df.index)

    def set_macro_list(self):
        self.macro_list = self.pivoted_macro_value_df.columns

    def set_macro_timeseries(self):
        self.macro_timeseries = self.pivoted_macro_value_df.index

    # 매크로 데이터가 기준값 이상인 경우, r_cd로 분류
    def set_macro_status(self, r_cd=(1,-1), shift=1):
        self.pivoted_macro_status_df = pd.DataFrame(columns=self.macro_list, index=self.macro_timeseries)
        for macro_cd in self.macro_list:
            base_value = self.macro_master_df['base'][macro_cd]
            for idx, date_cd in enumerate(self.macro_timeseries):
                self.pivoted_macro_status_df[macro_cd][date_cd] = r_cd[0] if self.pivoted_macro_value_df[macro_cd][date_cd] > base_value else r_cd[1]

        # 지수 움직임과 시점을 맞추기 위해 1개월 lag
        self.pivoted_macro_status_shift_df[shift] = self.pivoted_macro_status_df.shift(shift)

        # 구간 예상을 위해 마지막 매크로 데이터 저장
        self.macro_last_df = self.pivoted_macro_status_df[-1:]

    # 매크로 데이터가 이전값 보다 경우, r_cd로 분류
    def set_macro_momentum(self, r_cd=(1,-1), shift=1):
        self.pivoted_macro_momentum_df = pd.DataFrame(columns=self.macro_list, index=self.macro_timeseries)
        for macro_cd in self.macro_list:
            for idx, date_cd in enumerate(self.macro_timeseries):
                if idx > 0:
                    self.pivoted_macro_momentum_df[macro_cd][date_cd] = r_cd[0] if self.pivoted_macro_value_df[macro_cd][date_cd] > prev_value else r_cd[1]
                prev_value = self.pivoted_macro_value_df[macro_cd][date_cd]

        # 지수 움직임과 시점을 맞추기 위해 1개월 lag
        self.pivoted_macro_momentum_shift_df[shift] = self.pivoted_macro_momentum_df.shift(shift)

    def get_index_master(self):
        # 지수 시계열 데이터 셋팅
        sql = "select cd, nm, ctry" \
              "  from index_master"
        self.index_master_df = db.select_query(sql)
        self.index_master_df.columns = ('cd', 'nm', 'ctry')
        self.index_master_df.set_index('cd', inplace=True)

    def get_index_value(self):
        sql = "select cd, date, value" \
              "  from index_value"
        self.index_value_df = db.select_query(sql)
        self.index_value_df.columns = ('cd', 'date', 'value')
        self.pivoted_index_value_df = self.index_value_df.pivot(index='date', columns='cd', values='value')

        self.set_index_cnt()
        self.set_index_len()
        self.set_index_list()
        self.set_index_timeseries()

    def set_index_cnt(self):
        self.index_cnt = len(self.pivoted_index_value_df.columns)

    def set_index_len(self):
        self.index_len = len(self.pivoted_index_value_df.index)

    def set_index_list(self):
        self.index_list = self.pivoted_index_value_df.columns

    def set_index_timeseries(self):
        self.index_timeseries = self.pivoted_index_value_df.index

    # 지수가 상승한 경우, r_cd로 분류
    def set_index_direction(self, r_cd=(1,-1)):
        self.pivoted_index_direction_df = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
        for index_cd in self.index_list:
            for idx, date_cd in enumerate(self.index_timeseries):
                if idx > 0:
                    self.pivoted_index_direction_df[index_cd][date_cd] = r_cd[0] if self.pivoted_index_value_df[index_cd][date_cd] > prev_value else r_cd[1]
                prev_value = self.pivoted_index_value_df[index_cd][date_cd]

    # 지수 데이터의 단위 수익률
    def set_index_yield(self):
        self.pivoted_index_yield_df = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
        for index_cd in self.index_list:
            for idx, date_cd in enumerate(self.index_timeseries):
                if idx > 0:
                    self.pivoted_index_yield_df[index_cd][date_cd] = self.pivoted_index_value_df[index_cd][date_cd] / prev_value - 1
                prev_value = self.pivoted_index_value_df[index_cd][date_cd]

    # 매크로 데이터가 기준값 이상이고 지수 상승, 매크로 데이터가 기준값 이하이고 지수 하락 경우 COUNT, r_cd로 분류
    def calc_matching_status_ratio(self, r_cd=(1,-1), shift=1):
        self.macro_status_index_direction_relation_right_df = pd.DataFrame(columns=self.macro_list, index=self.index_list)
        self.macro_status_index_direction_relation_up_right_df = pd.DataFrame(columns=self.macro_list, index=self.index_list)
        self.macro_status_index_direction_relation_down_right_df = pd.DataFrame(columns=self.macro_list, index=self.index_list)

        for macro_cd in self.macro_list:
            for index_cd in self.index_list:
                right_up_cnt = 0
                right_down_cnt = 0
                for date_cd in self.index_timeseries:
                    # 통계 값에 nan에 의한 오류는 무시
                    if self.pivoted_macro_status_shift_df[shift][macro_cd][date_cd] == r_cd[0] and self.pivoted_index_direction_df[index_cd][date_cd] == r_cd[0]:
                        right_up_cnt += 1
                    elif self.pivoted_macro_status_shift_df[shift][macro_cd][date_cd] == r_cd[1] and self.pivoted_index_direction_df[index_cd][date_cd] == r_cd[1]:
                        right_down_cnt += 1

                self.macro_status_index_direction_relation_right_df[macro_cd][index_cd] = (right_up_cnt + right_down_cnt) / self.index_len
                self.macro_status_index_direction_relation_up_right_df[macro_cd][index_cd] = right_up_cnt / self.index_len
                self.macro_status_index_direction_relation_down_right_df[macro_cd][index_cd] = right_down_cnt / self.index_len

    # 매크로 데이터가 이전값 보다 크고 지수 상승, 매크로 데이터가 이전값 보다 작고 지수 하락 경우 COUNT, r_cd로 분류
    def calc_matching_momentum_ratio(self, r_cd=(1,-1), shift=1):
        self.macro_momentum_index_direction_relation_right_df = pd.DataFrame(columns=self.macro_list, index=self.index_list)
        self.macro_momentum_index_direction_relation_up_right_df = pd.DataFrame(columns=self.macro_list, index=self.index_list)
        self.macro_momentum_index_direction_relation_down_right_df = pd.DataFrame(columns=self.macro_list, index=self.index_list)

        for macro_cd in self.macro_list:
            for index_cd in self.index_list:
                right_up_cnt = 0
                right_down_cnt = 0
                for date_cd in self.index_timeseries:
                    # 통계 값에 nan에 의한 오류는 무시
                    if self.pivoted_macro_momentum_shift_df[shift][macro_cd][date_cd] == r_cd[0] and self.pivoted_index_direction_df[index_cd][date_cd] == r_cd[0]:
                        right_up_cnt += 1
                    elif self.pivoted_macro_momentum_shift_df[shift][macro_cd][date_cd] == r_cd[1] and self.pivoted_index_direction_df[index_cd][date_cd] == r_cd[1]:
                        right_down_cnt += 1

                self.macro_momentum_index_direction_relation_right_df[macro_cd][index_cd] = (right_up_cnt + right_down_cnt) / self.index_len
                self.macro_momentum_index_direction_relation_up_right_df[macro_cd][index_cd] = right_up_cnt / self.index_len
                self.macro_momentum_index_direction_relation_down_right_df[macro_cd][index_cd] = right_down_cnt / self.index_len

    # r_cd로 분류
    def set_matching_momentum_series(self, r_cd=(1,-1), shift=1):
        for macro_cd in self.macro_list:
            self.momentum_direction_up_right_series[macro_cd] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
            self.momentum_direction_down_right_series[macro_cd] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
            for index_cd in self.index_list:
                for date_cd in self.index_timeseries:
                    # 통계 값에 nan에 의한 오류는 무시
                    if self.pivoted_macro_momentum_shift_df[shift][macro_cd][date_cd] == r_cd[0] and self.pivoted_index_direction_df[index_cd][date_cd] == r_cd[0]:
                        self.momentum_direction_up_right_series[macro_cd][index_cd][date_cd] = 1
                    elif self.pivoted_macro_momentum_shift_df[shift][macro_cd][date_cd] == r_cd[1] and self.pivoted_index_direction_df[index_cd][date_cd] == r_cd[1]:
                        self.momentum_direction_down_right_series[macro_cd][index_cd][date_cd] = 1

    # r_cd로 분류
    def calc_matching_status_momentum_ratio(self, r_cd=(1,-1), shift=1):
        self.macro_status_momentum_index_direction_relation_right_df = pd.DataFrame(columns=self.macro_list, index=self.index_list)
        self.macro_status_momentum_index_direction_relation_up_right_df = pd.DataFrame(columns=self.macro_list, index=self.index_list)
        self.macro_status_momentum_index_direction_relation_down_right_df = pd.DataFrame(columns=self.macro_list, index=self.index_list)

        for macro_cd in self.macro_list:
            for index_cd in self.index_list:
                right_up_cnt = 0
                right_down_cnt = 0
                for date_cd in self.index_timeseries:
                    # 통계 값에 nan에 의한 오류는 무시
                    if self.pivoted_macro_status_shift_df[shift][macro_cd][date_cd] == r_cd[0] and \
                            self.pivoted_macro_momentum_shift_df[shift][macro_cd][date_cd] == r_cd[0] and \
                            self.pivoted_index_direction_df[index_cd][date_cd] == r_cd[0]:
                        right_up_cnt += 1
                    elif self.pivoted_macro_status_shift_df[shift][macro_cd][date_cd] == r_cd[1] and \
                            self.pivoted_macro_momentum_shift_df[shift][macro_cd][date_cd] == r_cd[1] and \
                            self.pivoted_index_direction_df[index_cd][date_cd] == r_cd[1]:
                        right_down_cnt += 1

                self.macro_status_momentum_index_direction_relation_right_df[macro_cd][index_cd] = (right_up_cnt + right_down_cnt) / self.index_len
                self.macro_status_momentum_index_direction_relation_up_right_df[macro_cd][index_cd] = right_up_cnt / self.index_len
                self.macro_status_momentum_index_direction_relation_down_right_df[macro_cd][index_cd] = right_down_cnt / self.index_len

    # 현재는 평균만 계산할 수 있음
    def set_matching_momentum_statistic(self, type='mean', weights_info=None, threshold=0.0):
        key = type if weights_info is None else type+'_'+weights_info[0]
        weights = np.repeat(1/self.macro_cnt, self.macro_cnt)

        self.momentum_direction_up_right_series[key] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
        self.momentum_direction_down_right_series[key] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)

        for index_cd in self.index_list:
            weights = weights_info[1][index_cd] if weights_info is not None else weights
            for date_cd in self.index_timeseries:
                momentum_up_right = np.repeat(0, self.macro_cnt)
                momentum_down_right = np.repeat(0, self.macro_cnt)
                for idx, macro_cd in enumerate(self.macro_list):
                    momentum_up_right[idx] = self.momentum_direction_up_right_series[macro_cd][index_cd][date_cd] if math.isnan(self.momentum_direction_up_right_series[macro_cd][index_cd][date_cd]) == False else 0
                    momentum_down_right[idx] = self.momentum_direction_down_right_series[macro_cd][index_cd][date_cd] if math.isnan(self.momentum_direction_down_right_series[macro_cd][index_cd][date_cd]) == False else 0

                if type == 'mean':
                    self.momentum_direction_up_right_series[key][index_cd][date_cd] = round(sum(momentum_up_right*weights), 2) if sum(momentum_up_right*weights) > threshold else 0
                    self.momentum_direction_down_right_series[key][index_cd][date_cd] = round(sum(momentum_down_right*weights), 2) if sum(momentum_down_right*weights) > threshold else 0

    def save_log(self):
        if platform.system() == 'Windows':
            Wrap_Util.SaveExcelFiles(file='%s\\corr ratio.xlsx' % (base_dir)
                                     , obj_dict={'macro_status_index_direction_relation_right_df': self.macro_status_index_direction_relation_right_df
                                                , 'macro_momentum_index_direction_relation_right_df': self.macro_momentum_index_direction_relation_right_df
                                                , 'macro_status_momentum_index_direction_relation_right_df': self.macro_status_momentum_index_direction_relation_right_df
                                                , 'pivoted_macro_status_df': self.pivoted_macro_status_df
                                                , 'pivoted_macro_momentum_df' : self.pivoted_macro_momentum_df
                                                , 'pivoted_index_direction_df': self.pivoted_index_direction_df
                                                , 'pivoted_index_value_df': self.pivoted_index_value_df
                                                , 'pivoted_macro_value_df': self.pivoted_macro_value_df})

    if 0:
        panel = Figure()
        for macro_cd in pivoted_macro_momentum_df.columns:
            for index_cd in pivoted_index_value_df.columns:

                macro_ctry =macro_master_df['ctry'][macro_cd]
                macro_nm = macro_master_df['nm'][macro_cd]
                index_nm = index_master_df['nm'][index_cd]

                plot_df = pd.DataFrame()
                plot_df[macro_cd] = pivoted_macro_status_df[macro_cd]
                plot_df[index_cd] = pivoted_index_value_df[index_cd]
                panel.draw(plot_df, title=macro_ctry+'_'+macro_nm, subplots=[index_cd], figsize=(10,5))

    def do_figure(self, weights_info=None, img_save='n'):
        panel = Figure()
        panel_size = (20, 10)
        sub_plot_row = 3

        macro_ctry = 'world'
        macro_nm = 'mean'
        macro_cd = macro_nm
        panel.draw_multi_graph_with_matching_analysis(data=self.pivoted_index_value_df, analysis=(self.momentum_direction_up_right_series[macro_cd], self.momentum_direction_down_right_series[macro_cd])
                          , anal_value=None, title=macro_ctry+'_'+macro_nm, figsize=panel_size, figshape=(sub_plot_row, math.ceil(self.index_cnt / sub_plot_row))
                          , img_save=img_save)

        if weights_info is not None:
            macro_nm = 'mean'+'_'+weights_info[0]
            macro_cd = macro_nm
            panel.draw_multi_graph_with_matching_analysis(data=self.pivoted_index_value_df, analysis=(self.momentum_direction_up_right_series[macro_cd], self.momentum_direction_down_right_series[macro_cd])
                          , anal_value=None, title=macro_ctry+'_'+macro_nm,figsize=panel_size, figshape=(sub_plot_row, math.ceil(self.index_cnt / sub_plot_row))
                          , img_save=img_save)

        for macro_cd in self.macro_list:
            macro_ctry = self.macro_master_df['ctry'][macro_cd]
            macro_nm = self.macro_master_df['nm'][macro_cd]
            panel.draw_multi_graph_with_matching_analysis(data=self.pivoted_index_value_df, analysis=(self.momentum_direction_up_right_series[macro_cd], self.momentum_direction_down_right_series[macro_cd])
                              , anal_value=self.macro_momentum_index_direction_relation_right_df[macro_cd], title=macro_ctry+'_'+macro_nm, figsize=panel_size, figshape=(sub_plot_row, math.ceil(self.index_cnt/sub_plot_row))
                              , img_save=img_save)


if __name__ == '__main__':
    db = WrapDB()
    db.connet(host="127.0.0.1", port=3306, database="macro_cycle", user="root", password="ryumaria")

    ele = FinancialCycle(db)

    # 선행 속성을 가지고 있는 매크로 데이터 리스트 읽기
    ele.get_macro_master()
    # 매크로 데이터의 시계열 값 읽기
    ele.get_macro_value()
    # 매크로 데이터의 기준값 대비 상태 확인
    ele.set_macro_status()
    # 매크로 데이터의 모멘텀 확인
    ele.set_macro_momentum()

    # 주요 국가별 지수 데이터 리스트 읽기
    ele.get_index_master()
    # 지수 데이터의 시계열 값 읽
    ele.get_index_value()
    # 지수 데이터의 방향성(상승, 하락) 상태 확인
    ele.set_index_direction()
    # 지수 데이터의 월단위 수익률 계산
    ele.set_index_yield()

    # 매크로 데이터의 기준값 대비 상태와 지수 데이터의 방향성이 동일한 경우 확인
    ele.calc_matching_status_ratio()
    # 매크로 데이터의 모멘텀과 지수 데이터의 방향성이 동일한 경우 확인
    ele.calc_matching_momentum_ratio()
    print("################## macro & index matching momentum ratio ##################")
    for idx_col, index_cd in enumerate(ele.index_list):
        if idx_col == 0:
            txt_str = str(list(ele.macro_master_df['nm'])).replace(',', '\t').replace('[','').replace(']','') + '\n'
        for idx_row, macro_cd in enumerate(ele.macro_list):
            if idx_row == 0:
                txt_str = txt_str + index_cd + ':\t'
            txt_str = txt_str + str(round(ele.macro_momentum_index_direction_relation_right_df[macro_cd][index_cd], 2)) + '\t'
        txt_str = txt_str + '\n'
    print(txt_str)
    print("###########################################################################")
    ele.set_matching_momentum_series()
    # 매크로 데이터의 기준값 대비 상태, 모멘텀과 지수 데이터의 방향성이 동일한 경우 확인
    ele.calc_matching_status_momentum_ratio()
    # 매크로 데이터들의 평균 모멘텀 적용
    ele.set_matching_momentum_statistic(type='mean', threshold=0.0)

    # 지수별 최적화된 매크로 데이터들의 가중 평균 모멘텀 적용
    right_up_case = copy.deepcopy(ele.momentum_direction_up_right_series)
    right_down_case = copy.deepcopy(ele.momentum_direction_down_right_series)
    macro_list = copy.deepcopy(ele.macro_list)
    index_list = copy.deepcopy(ele.index_list)
    timeseries = copy.deepcopy(ele.index_timeseries)
    weights_list = maximize_profit(right_up_case, right_down_case, macro_list, index_list, timeseries, lb=0.05, ub=0.95)
    ele.set_matching_momentum_statistic(type='mean', weights_info=('optimized', weights_list), threshold=0.0)
    print("################## optimized weights ##################")
    print(str(list(ele.macro_master_df['nm'])).replace(',', '\t').replace('[','').replace(']',''))
    for weights_cd in weights_list:
        print(weights_cd + ':\t' + str(weights_list[weights_cd]).replace(',', '\t').replace('[','').replace(']',''))
    print("#######################################################")
    print("################## forecast index's direction ##################")
    for weights_cd in weights_list:
        print(weights_cd + ':\t' + str(round(sum(weights_list[weights_cd]*ele.macro_last_df.values[0]), 2)))
    print("################################################################")
    ele.do_figure(weights_info=('optimized', weights_list), img_save='n')
    ele.save_log()

    db.disconnect()


