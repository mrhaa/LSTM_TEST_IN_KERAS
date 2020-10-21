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

base_dir = (os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

SC_LOG = True

def maximize_hit_ratio(up_right_case=None, down_right_case=None, up_wrong_case=None, down_wrong_case=None, macro_list=None, index_list=None, timeseries=None,lb=0.00, ub=0.1):

    if up_right_case==None and down_right_case==None and up_wrong_case==None and down_wrong_case==None:
        return None

    def profit(x, args):
        right_sum = args

        return -sum(x*right_sum)

    def sum_weight(x):
        return 1-sum(x)

    macro_cnt = len(macro_list)
    weights_list = {}
    for index_cd in index_list:
        up_right_sum = np.repeat(0, macro_cnt)
        down_right_sum = np.repeat(0, macro_cnt)
        up_wrong_sum = np.repeat(0, macro_cnt)
        down_wrong_sum = np.repeat(0, macro_cnt)
        for idx, macro_cd in enumerate(macro_list):
            for time_cd in timeseries:
                if up_right_case is not None:
                    if math.isnan(up_right_case[macro_cd][index_cd][time_cd]) == False:
                        up_right_sum[idx] += up_right_case[macro_cd][index_cd][time_cd]
                # up & down을 구분하지 않고 파라미터가 1개로 전달되는 경우 pass
                if down_right_case is not None:
                    if math.isnan(down_right_case[macro_cd][index_cd][time_cd]) == False:
                        down_right_sum[idx] += down_right_case[macro_cd][index_cd][time_cd]
                # up & down을 구분하지 않고 파라미터가 1개로 전달되는 경우 pass
                if up_wrong_case is not None:
                    if math.isnan(up_wrong_case[macro_cd][index_cd][time_cd]) == False:
                        up_wrong_sum[idx] += up_wrong_case[macro_cd][index_cd][time_cd]
                # up & down을 구분하지 않고 파라미터가 1개로 전달되는 경우 pass
                if down_wrong_case is not None:
                    if math.isnan(down_wrong_case[macro_cd][index_cd][time_cd]) == False:
                        down_wrong_sum[idx] += down_wrong_case[macro_cd][index_cd][time_cd]

        x0 = np.repeat(1 / macro_cnt, macro_cnt)
        lbound = np.repeat(lb, macro_cnt)
        ubound = np.repeat(ub, macro_cnt)
        bnds = tuple(zip(lbound, ubound))
        constraints = {'type': 'eq', 'fun': sum_weight}
        options = {'ftol': 1e-20, 'maxiter': 5000, 'disp': False}

        print(up_right_sum+down_right_sum+up_wrong_sum+down_wrong_sum)
        result = minimize(fun=profit,
                          x0=x0,
                          args=(up_right_sum+down_right_sum+up_wrong_sum+down_wrong_sum),
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
        self.macro_last_df = {}

        self.index_master_df = None
        self.index_value_df = None
        self.pivoted_index_value_df = None
        self.index_cnt = 0
        self.index_len = 0
        self.index_list = None
        self.index_timeseries = None

        # status, momentum, diff
        self.pivoted_macro_property_dfs = {} # 해당 월의 값은 지수 데이터의 다음 월이랑 매핑, shift df를 이용해 로직 적용
        self.pivoted_macro_property_shift_dfs = {} # shift의 단위는 M이고 1개월을 shift 함

        # direction, yield
        self.pivoted_index_property_dfs = {} # 해당 월의 방향은 해당 row에 저장

        # 매크로 데이터의 속성과 지수 데이터의 움직임 관계를 통계냄
        # 가중 평균된 매크로 데이터들을 사용하여 지수 데이터의 움직임 관계를 통계냄
        self.relation_dfs = {}
        self.relation_right_dfs = {}
        self.relation_wrong_dfs = {}
        self.relation_up_dfs = {}
        self.relation_down_dfs = {}
        self.relation_up_right_dfs = {}
        self.relation_down_right_dfs = {}
        self.relation_up_wrong_dfs = {}
        self.relation_down_wrong_dfs = {}

        # 1. 매크로 데이터의 속성과 지수 데이터의 움직임 관계를 시계열로 관리하며 해당 row에 있는 값은 해당 달의 값을 의미
        # (ex. 2/28: 2월에 맞았는지를 의미, 2월의 지수 움직임과 1월의 매크로 통계값)
        # 2. 4가지 경우를 그래프에 지수와 같이 보여주기 위해 (-1, 1)구간을 사용하여 표시
        # (up & down은 매크로 데이터를 기준으로 하며, right은 양수 wrong은 음수로 표시)
        # 3. weighted series는 해당 기간의 상승 ,하락 확률을 분리하여 나타내기 때문에 두가지 정보가 합쳐지면 해당 기간의 시그널이 점수로 계산
        # (up_right & down_wrong, down_right & up_wrong)
        self.relation_series = {}
        self.relation_up_series = {}
        self.relation_down_series = {}
        self.relation_up_right_series = {}
        self.relation_down_right_series = {}
        self.relation_up_wrong_series = {}
        self.relation_down_wrong_series = {}

        self.relation_profit_dfs = {}

    def get_macro_master(self):
        # 매크로 시계열 데이터 셋
        sql = "select a.cd as cd, a.nm as nm, a.ctry as ctry, a.base as base, a.unit as unit" \
              "  from macro_master a" \
              "     , macro_value b" \
              " where a.base is not null" \
              "   and a.cd = b.cd" \
              " group by a.cd, a.nm, a.ctry, a.base, a.unit" \
              " having count(*) > 0"
        self.macro_master_df = self.db.select_query(sql)
        self.macro_master_df.columns = ('cd', 'nm', 'ctry', 'base', 'unit')
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
    # 매크로 데이터가 이전값 보다 경우, r_cd로 분류
    def set_macro_property(self, type='momentum', r_cd=(1,-1), shift=1):
        self.pivoted_macro_property_dfs[type] = pd.DataFrame(columns=self.macro_list, index=self.macro_timeseries)
        for macro_cd in self.macro_list:
            base_value = self.macro_master_df['base'][macro_cd]
            unit = self.macro_master_df['unit'][macro_cd]
            for idx, date_cd in enumerate(self.macro_timeseries):
                if idx > 0:
                    if type == 'status':
                        self.pivoted_macro_property_dfs[type][macro_cd][date_cd] = r_cd[0] if self.pivoted_macro_value_df[macro_cd][date_cd] > base_value else r_cd[1]
                    elif type == 'momentum':
                        self.pivoted_macro_property_dfs[type][macro_cd][date_cd] = r_cd[0] if self.pivoted_macro_value_df[macro_cd][date_cd] > prev_value else r_cd[1]
                    # 매크로 데이터 종류에 따라 변화률을 계산하는 방법이 다름
                    elif type == 'diff':
                        # 인덱스의 경우 변화률을 계산
                        if unit == 'I':
                            self.pivoted_macro_property_dfs[type][macro_cd][date_cd] = self.pivoted_macro_value_df[macro_cd][date_cd] / prev_value - 1
                        # 퍼센트의 경우 차이를 이용해서 퍼센트 포인트를 계산
                        elif unit == 'P':
                            self.pivoted_macro_property_dfs[type][macro_cd][date_cd] = self.pivoted_macro_value_df[macro_cd][date_cd] - prev_value
                prev_value = self.pivoted_macro_value_df[macro_cd][date_cd]

        # 지수 움직임과 시점을 맞추기 위해 1개월 lag
        self.pivoted_macro_property_shift_dfs[type] = self.pivoted_macro_property_dfs[type].shift(shift)

        # 구간 예상을 위해 마지막 매크로 데이터 저장
        self.macro_last_df[type] = self.pivoted_macro_property_dfs[type][-1:]

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

    # 지수가 상승 or 하락, r_cd로 분류
    # 지수 데이터의 단위 수익률
    def set_index_property(self, type='direction', r_cd=(1,-1)):
        self.pivoted_index_property_dfs[type] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
        for index_cd in self.index_list:
            for idx, date_cd in enumerate(self.index_timeseries):
                if idx > 0:
                    if type == 'direction':
                        self.pivoted_index_property_dfs[type][index_cd][date_cd] = r_cd[0] if self.pivoted_index_value_df[index_cd][date_cd] > prev_value else r_cd[1]
                    elif type == 'yield':
                        self.pivoted_index_property_dfs[type][index_cd][date_cd] = self.pivoted_index_value_df[index_cd][date_cd] / prev_value - 1

                prev_value = self.pivoted_index_value_df[index_cd][date_cd]

    # 매크로 데이터가 기준값 이상이고 지수 상승, 매크로 데이터가 기준값 이하이고 지수 하락 경우 COUNT, r_cd로 분류
    def calc_matching_properties_ratio(self, macro_type='momentum', index_type='direction', r_cd=(1,-1)):
        key = macro_type+'_'+index_type

        self.relation_dfs[key] = pd.DataFrame(columns=self.macro_list, index=self.index_list)
        self.relation_right_dfs[key] = pd.DataFrame(columns=self.macro_list, index=self.index_list)
        self.relation_wrong_dfs[key] = pd.DataFrame(columns=self.macro_list, index=self.index_list)
        self.relation_up_dfs[key] = pd.DataFrame(columns=self.macro_list, index=self.index_list)
        self.relation_down_dfs[key] = pd.DataFrame(columns=self.macro_list, index=self.index_list)
        self.relation_up_right_dfs[key] = pd.DataFrame(columns=self.macro_list, index=self.index_list)
        self.relation_down_right_dfs[key] = pd.DataFrame(columns=self.macro_list, index=self.index_list)
        self.relation_up_wrong_dfs[key] = pd.DataFrame(columns=self.macro_list, index=self.index_list)
        self.relation_down_wrong_dfs[key] = pd.DataFrame(columns=self.macro_list, index=self.index_list)

        for macro_cd in self.macro_list:
            for index_cd in self.index_list:
                up_right_cnt = 0
                down_right_cnt = 0
                up_wrong_cnt = 0
                down_wrong_cnt = 0
                for date_cd in self.index_timeseries:
                    # 통계 값에 nan에 의한 오류는 무시
                    # 매크로 상승, 지수 상승
                    if self.pivoted_macro_property_shift_dfs[macro_type][macro_cd][date_cd] == r_cd[0] and self.pivoted_index_property_dfs[index_type][index_cd][date_cd] == r_cd[0]:
                        up_right_cnt += r_cd[0]
                    # 매크로 하락, 지수 하락
                    elif self.pivoted_macro_property_shift_dfs[macro_type][macro_cd][date_cd] == r_cd[1] and self.pivoted_index_property_dfs[index_type][index_cd][date_cd] == r_cd[1]:
                        down_right_cnt += r_cd[0]
                    # 매크로 상승, 지수 하락
                    elif self.pivoted_macro_property_shift_dfs[macro_type][macro_cd][date_cd] == r_cd[0] and self.pivoted_index_property_dfs[index_type][index_cd][date_cd] == r_cd[1]:
                        up_wrong_cnt += r_cd[1]
                    # 매크로 하락, 지수 상승
                    elif self.pivoted_macro_property_shift_dfs[macro_type][macro_cd][date_cd] == r_cd[1] and self.pivoted_index_property_dfs[index_type][index_cd][date_cd] == r_cd[0]:
                        down_wrong_cnt += r_cd[1]

                self.relation_dfs[key][macro_cd][index_cd] = (up_right_cnt+down_right_cnt+up_wrong_cnt+down_wrong_cnt) / self.index_len
                self.relation_right_dfs[key][macro_cd][index_cd] = (up_right_cnt+down_right_cnt) / self.index_len
                self.relation_wrong_dfs[key][macro_cd][index_cd] = (up_wrong_cnt+down_wrong_cnt) / self.index_len
                self.relation_up_dfs[key][macro_cd][index_cd] = (up_right_cnt+up_wrong_cnt) / self.index_len
                self.relation_down_dfs[key][macro_cd][index_cd] = (down_right_cnt+down_wrong_cnt) / self.index_len
                self.relation_up_right_dfs[key][macro_cd][index_cd] = up_right_cnt / self.index_len
                self.relation_down_right_dfs[key][macro_cd][index_cd] = down_right_cnt / self.index_len
                self.relation_up_wrong_dfs[key][macro_cd][index_cd] = up_wrong_cnt / self.index_len
                self.relation_down_wrong_dfs[key][macro_cd][index_cd] = down_wrong_cnt / self.index_len

    # r_cd로 분류
    def set_matching_properties_series(self, macro_type='momentum', index_type='direction', r_cd=(1,-1)):
        key = macro_type+'_'+index_type

        self.relation_series[key] = {}
        self.relation_up_series[key] = {}
        self.relation_down_series[key] = {}
        self.relation_up_right_series[key] = {}
        self.relation_down_right_series[key] = {}
        self.relation_up_wrong_series[key] = {}
        self.relation_down_wrong_series[key] = {}

        for macro_cd in self.macro_list:
            self.relation_series[key][macro_cd] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
            self.relation_up_series[key][macro_cd] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
            self.relation_down_series[key][macro_cd] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
            self.relation_up_right_series[key][macro_cd] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
            self.relation_down_right_series[key][macro_cd] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
            self.relation_up_wrong_series[key][macro_cd] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
            self.relation_down_wrong_series[key][macro_cd] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)

            for index_cd in self.index_list:
                for date_cd in self.index_timeseries:
                    # 통계 값에 nan에 의한 오류는 무시
                    # 매크로 상승, 지수 상승
                    if self.pivoted_macro_property_shift_dfs[macro_type][macro_cd][date_cd] == r_cd[0] and self.pivoted_index_property_dfs[index_type][index_cd][date_cd] == r_cd[0]:
                        self.relation_up_right_series[key][macro_cd][index_cd][date_cd] = r_cd[0]
                        self.relation_up_series[key][macro_cd] = r_cd[0]
                    # 매크로 하락, 지수 하락
                    elif self.pivoted_macro_property_shift_dfs[macro_type][macro_cd][date_cd] == r_cd[1] and self.pivoted_index_property_dfs[index_type][index_cd][date_cd] == r_cd[1]:
                        self.relation_down_right_series[key][macro_cd][index_cd][date_cd] = r_cd[0]
                        self.relation_down_series[key][macro_cd] = r_cd[0]
                    # 매크로 상승, 지수 하락
                    elif self.pivoted_macro_property_shift_dfs[macro_type][macro_cd][date_cd] == r_cd[0] and self.pivoted_index_property_dfs[index_type][index_cd][date_cd] == r_cd[1]:
                        self.relation_up_wrong_series[key][macro_cd][index_cd][date_cd] = r_cd[1]
                        self.relation_up_series[key][macro_cd] = r_cd[1]
                    # 매크로 하락, 지수 상승
                    elif self.pivoted_macro_property_shift_dfs[macro_type][macro_cd][date_cd] == r_cd[1] and self.pivoted_index_property_dfs[index_type][index_cd][date_cd] == r_cd[0]:
                        self.relation_down_wrong_series[key][macro_cd][index_cd][date_cd] = r_cd[1]
                        self.relation_down_series[key][macro_cd] = r_cd[1]

                    # 동일하지 않은 경우 패널티(-1) 발생
                    self.relation_series[key][macro_cd][index_cd][date_cd] = self.pivoted_macro_property_shift_dfs[macro_type][macro_cd][date_cd]*self.pivoted_index_property_dfs[index_type][index_cd][date_cd]

    # 현재는 평균만 계산할 수 있음
    def calc_matching_properties_weighted_statistic_ratio(self, type='mean', weights_info=None, r_cd=(1,-1), macro_type='momentum', index_type='direction'):
        macro_index_key = macro_type+'_'+index_type
        key = type if weights_info is None else type+'_'+weights_info[0]
        weights = np.repeat(1 / self.macro_cnt, self.macro_cnt)

        self.relation_dfs[macro_index_key][key] = pd.Series()
        self.relation_right_dfs[macro_index_key][key] = pd.Series()
        self.relation_wrong_dfs[macro_index_key][key] = pd.Series()
        self.relation_up_dfs[macro_index_key][key] = pd.Series()
        self.relation_down_dfs[macro_index_key][key] = pd.Series()
        self.relation_up_right_dfs[macro_index_key][key] = pd.Series()
        self.relation_down_right_dfs[macro_index_key][key] = pd.Series()
        self.relation_up_wrong_dfs[macro_index_key][key] = pd.Series()
        self.relation_down_wrong_dfs[macro_index_key][key] = pd.Series()

        for index_cd in self.index_list:
            weights = weights_info[1][index_cd] if weights_info is not None and weights_info[1] is not None else weights
            up_right_cnt = 0
            down_right_cnt = 0
            up_wrong_cnt = 0
            down_wrong_cnt = 0
            for date_cd in self.index_timeseries:
                relation_up_right = np.repeat(0, self.macro_cnt)
                relation_down_right = np.repeat(0, self.macro_cnt)
                relation_up_wrong = np.repeat(0, self.macro_cnt)
                relation_down_wrong = np.repeat(0, self.macro_cnt)
                for idx, macro_cd in enumerate(self.macro_list):
                    # 매크로 상승, 지수 상승
                    relation_up_right[idx] = self.relation_up_right_series[macro_index_key][macro_cd][index_cd][date_cd] if math.isnan(self.relation_up_right_series[macro_index_key][macro_cd][index_cd][date_cd]) == False else 0
                    # 매크로 하락, 지수 하락
                    relation_down_right[idx] = self.relation_down_right_series[macro_index_key][macro_cd][index_cd][date_cd] if math.isnan(self.relation_down_right_series[macro_index_key][macro_cd][index_cd][date_cd]) == False else 0
                    # 매크로 상승, 지수 하락
                    relation_up_wrong[idx] = self.relation_up_wrong_series[macro_index_key][macro_cd][index_cd][date_cd] if math.isnan(self.relation_up_wrong_series[macro_index_key][macro_cd][index_cd][date_cd]) == False else 0
                    # 매크로 하락, 지수 상승
                    relation_down_wrong[idx] = self.relation_down_wrong_series[macro_index_key][macro_cd][index_cd][date_cd] if math.isnan(self.relation_down_wrong_series[macro_index_key][macro_cd][index_cd][date_cd]) == False else 0

                if type == 'mean':
                    weighted_count_list = [sum(relation_up_right*weights), sum(relation_down_right*weights), sum(relation_up_wrong*weights), sum(relation_down_wrong*weights)]
                    # max_index = weighted_count_list.index(max([abs(weighted_sum) for weighted_sum in weighted_count_list]))
                    max_index = 0
                    max_value = abs(weighted_count_list[0])
                    for idx, weighted_sum in enumerate(weighted_count_list):
                        if abs(weighted_sum) > max_value:
                            max_index = idx
                            max_value = abs(weighted_sum)

                    # 매크로 상승, 지수 상승
                    if max_index == 0:
                        up_right_cnt += r_cd[0]
                    # 매크로 하락, 지수 하락
                    elif  max_index == 1:
                        down_right_cnt += r_cd[0]
                    # 매크로 상승, 지수 하락
                    elif  max_index == 2:
                        up_wrong_cnt += r_cd[1]
                    # 매크로 하락, 지수 상승
                    elif  max_index == 3:
                        down_wrong_cnt += r_cd[1]

            self.relation_dfs[macro_index_key][key][index_cd] = (up_right_cnt+down_right_cnt+up_wrong_cnt+down_wrong_cnt) / self.index_len
            self.relation_right_dfs[macro_index_key][key][index_cd] = (up_right_cnt+down_right_cnt) / self.index_len
            self.relation_wrong_dfs[macro_index_key][key][index_cd] = (up_wrong_cnt+down_wrong_cnt) / self.index_len
            self.relation_up_dfs[macro_index_key][key][index_cd] = (up_right_cnt+up_wrong_cnt) / self.index_len
            self.relation_down_dfs[macro_index_key][key][index_cd] = (down_right_cnt+down_wrong_cnt) / self.index_len
            self.relation_up_right_dfs[macro_index_key][key][index_cd] = up_right_cnt / self.index_len
            self.relation_down_right_dfs[macro_index_key][key][index_cd] = down_right_cnt / self.index_len
            self.relation_up_wrong_dfs[macro_index_key][key][index_cd] = up_wrong_cnt / self.index_len
            self.relation_down_wrong_dfs[macro_index_key][key][index_cd] = down_wrong_cnt / self.index_len

    # 현재는 평균만 계산할 수 있음
    def set_matching_properties_weighted_statistic_series(self, type='mean', weights_info=None, threshold=0.0, macro_type='momentum', index_type='direction'):
        macro_index_key = macro_type+'_'+index_type
        key = type if weights_info is None else type+'_'+weights_info[0]
        weights = np.repeat(1/self.macro_cnt, self.macro_cnt)

        self.relation_series[macro_index_key][key] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
        self.relation_up_series[macro_index_key][key] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
        self.relation_down_series[macro_index_key][key] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
        self.relation_up_right_series[macro_index_key][key] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
        self.relation_down_right_series[macro_index_key][key] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
        self.relation_up_wrong_series[macro_index_key][key] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)
        self.relation_down_wrong_series[macro_index_key][key] = pd.DataFrame(columns=self.index_list, index=self.index_timeseries)

        for index_cd in self.index_list:
            weights = weights_info[1][index_cd] if weights_info is not None and weights_info[1] is not None else weights
            for date_cd in self.index_timeseries:
                relation_up_right = np.repeat(0, self.macro_cnt)
                relation_down_right = np.repeat(0, self.macro_cnt)
                relation_up_wrong = np.repeat(0, self.macro_cnt)
                relation_down_wrong = np.repeat(0, self.macro_cnt)
                for idx, macro_cd in enumerate(self.macro_list):
                    # 매크로 상승, 지수 상승
                    relation_up_right[idx] = self.relation_up_right_series[macro_index_key][macro_cd][index_cd][date_cd] if math.isnan(self.relation_up_right_series[macro_index_key][macro_cd][index_cd][date_cd]) == False else 0
                    # 매크로 하락, 지수 하락
                    relation_down_right[idx] = self.relation_down_right_series[macro_index_key][macro_cd][index_cd][date_cd] if math.isnan(self.relation_down_right_series[macro_index_key][macro_cd][index_cd][date_cd]) == False else 0
                    # 매크로 상승, 지수 하락
                    relation_up_wrong[idx] = self.relation_up_wrong_series[macro_index_key][macro_cd][index_cd][date_cd] if math.isnan(self.relation_up_wrong_series[macro_index_key][macro_cd][index_cd][date_cd]) == False else 0
                    # 매크로 하락, 지수 상승
                    relation_down_wrong[idx] = self.relation_down_wrong_series[macro_index_key][macro_cd][index_cd][date_cd] if math.isnan(self.relation_down_wrong_series[macro_index_key][macro_cd][index_cd][date_cd]) == False else 0

                if type == 'mean':
                    self.relation_series[macro_index_key][key][index_cd][date_cd] = round(sum(relation_up_right*weights)+sum(relation_down_right*weights)+sum(relation_up_wrong*weights)+sum(relation_down_wrong*weights), 2)
                    self.relation_up_series[macro_index_key][key][index_cd][date_cd] = round(sum(relation_up_right*weights)+sum(relation_down_wrong*weights), 2)
                    self.relation_down_series[macro_index_key][key][index_cd][date_cd] = round(sum(relation_down_right*weights)+sum(relation_up_wrong*weights), 2)
                    self.relation_up_right_series[macro_index_key][key][index_cd][date_cd] = round(sum(relation_up_right*weights), 2) if abs(sum(relation_up_right*weights)) > threshold else 0
                    self.relation_down_right_series[macro_index_key][key][index_cd][date_cd] = round(sum(relation_down_right*weights), 2) if abs(sum(relation_down_right*weights)) > threshold else 0
                    self.relation_up_wrong_series[macro_index_key][key][index_cd][date_cd] = round(sum(relation_up_wrong*weights), 2) if abs(sum(relation_up_wrong*weights)) > threshold else 0
                    self.relation_down_wrong_series[macro_index_key][key][index_cd][date_cd] = round(sum(relation_down_wrong*weights), 2) if abs(sum(relation_down_wrong*weights)) > threshold else 0

    # 현재는 평균만 계산할 수 있음
    def calc_matching_properties_weighted_statistic_profit(self, type='mean', weights_info=None, macro_type='momentum', index_type='direction'):
        macro_index_key = macro_type+'_'+index_type
        key = type if weights_info is None else type+'_'+weights_info[0]
        yield_cd='yield'
        weights = np.repeat(1 / self.macro_cnt, self.macro_cnt)

        self.relation_profit_dfs[macro_index_key] = pd.DataFrame(columns={key}, index=self.index_list)

        for index_cd in self.index_list:
            weights = weights_info[1][index_cd] if weights_info is not None and weights_info[1] is not None else weights
            profit = 0
            for date_cd in self.index_timeseries:
                relation_up_right = np.repeat(0, self.macro_cnt)
                relation_down_right = np.repeat(0, self.macro_cnt)
                relation_up_wrong = np.repeat(0, self.macro_cnt)
                relation_down_wrong = np.repeat(0, self.macro_cnt)
                for idx, macro_cd in enumerate(self.macro_list):
                    # 매크로 상승, 지수 상승
                    relation_up_right[idx] = self.relation_up_right_series[macro_index_key][macro_cd][index_cd][date_cd] if math.isnan(self.relation_up_right_series[macro_index_key][macro_cd][index_cd][date_cd]) == False else 0
                    # 매크로 하락, 지수 하락
                    relation_down_right[idx] = self.relation_down_right_series[macro_index_key][macro_cd][index_cd][date_cd] if math.isnan(self.relation_down_right_series[macro_index_key][macro_cd][index_cd][date_cd]) == False else 0
                    # 매크로 상승, 지수 하락
                    relation_up_wrong[idx] = self.relation_up_wrong_series[macro_index_key][macro_cd][index_cd][date_cd] if math.isnan(self.relation_up_wrong_series[macro_index_key][macro_cd][index_cd][date_cd]) == False else 0
                    # 매크로 하락, 지수 상승
                    relation_down_wrong[idx] = self.relation_down_wrong_series[macro_index_key][macro_cd][index_cd][date_cd] if math.isnan(self.relation_down_wrong_series[macro_index_key][macro_cd][index_cd][date_cd]) == False else 0

                if type == 'mean':
                    weighted_count_list = [sum(relation_up_right*weights), sum(relation_down_right*weights), sum(relation_up_wrong*weights), sum(relation_down_wrong*weights)]
                    #max_index = weighted_count_list.index(max([abs(weighted_sum) for weighted_sum in weighted_count_list]))
                    max_index = 0
                    max_value = abs(weighted_count_list[0])
                    for idx, weighted_sum in enumerate(weighted_count_list):
                        if abs(weighted_sum) > max_value:
                            max_index = idx
                            max_value = abs(weighted_sum)

                    # 매크로 상승, 지수 상승
                    # 매크로 상승, 지수 하락
                    if max_index == 0 or max_index == 2:
                        profit = profit+self.pivoted_index_property_dfs[yield_cd][index_cd][date_cd] if math.isnan(self.pivoted_index_property_dfs[yield_cd][index_cd][date_cd]) == False else 0
                    # 매크로 하락, 지수 하락
                    # 매크로 하락, 지수 상승
                    elif max_index == 1 or max_index == 2:
                        profit = profit - self.pivoted_index_property_dfs[yield_cd][index_cd][date_cd] if math.isnan(self.pivoted_index_property_dfs[yield_cd][index_cd][date_cd]) == False else 0

            self.relation_profit_dfs[macro_index_key][key][index_cd] = profit

    def save_log(self):
        if platform.system() == 'Windows':
            Wrap_Util.SaveExcelFiles(file='%s\\corr ratio.xlsx' % (base_dir)
                                     , obj_dict={'relation_right_dfs(momentum_direction)': self.relation_right_dfs['momentum_direction']
                                                , 'pivoted_macro_property(status)': self.pivoted_macro_property_dfs['status']
                                                , 'pivoted_macro_property(momentum)': self.pivoted_macro_property_dfs['momentum']
                                                , 'pivoted_index_property(direction)': self.pivoted_index_property_dfs['direction']
                                                , 'pivoted_index_property(yield)': self.pivoted_index_property_dfs['yield']
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

        macro_index_key = 'momentum_direction'
        macro_ctry = 'world'
        macro_nm = 'mean'
        macro_cd = macro_nm


        panel.draw_multi_graph_with_matching_analysis(data=self.pivoted_index_value_df
                          , analysis=(self.relation_up_right_series[macro_index_key][macro_cd], self.relation_down_right_series[macro_index_key][macro_cd], self.relation_up_wrong_series[macro_index_key][macro_cd], self.relation_down_wrong_series[macro_index_key][macro_cd])
                          , anal_value=self.relation_right_dfs[macro_index_key][macro_cd], title=macro_ctry+'_'+macro_nm, figsize=panel_size, figshape=(sub_plot_row, math.ceil(self.index_cnt / sub_plot_row))
                          , ylim=(-1,1), img_save=img_save)

        panel.draw_multi_graph_with_matching_analysis(data=self.pivoted_index_value_df, analysis=(self.relation_series[macro_index_key][macro_cd],), anal_value=self.relation_right_dfs[macro_index_key][macro_cd]
                                                      , title=macro_ctry+'_'+macro_nm+'_single', figsize=panel_size, figshape=(sub_plot_row, math.ceil(self.index_cnt / sub_plot_row)), ylim=(-1,1), img_save=img_save)

        if weights_info is not None:
            macro_nm = 'mean'+'_'+weights_info[0]
            macro_cd = macro_nm
            panel.draw_multi_graph_with_matching_analysis(data=self.pivoted_index_value_df
                          , analysis=(self.relation_up_right_series[macro_index_key][macro_cd], self.relation_down_right_series[macro_index_key][macro_cd], self.relation_up_wrong_series[macro_index_key][macro_cd], self.relation_down_wrong_series[macro_index_key][macro_cd])
                          , anal_value=self.relation_right_dfs[macro_index_key][macro_cd], title=macro_ctry+'_'+macro_nm,figsize=panel_size, figshape=(sub_plot_row, math.ceil(self.index_cnt / sub_plot_row))
                          , ylim=(-1,1), img_save=img_save)
            panel.draw_multi_graph_with_matching_analysis(data=self.pivoted_index_value_df , analysis=(self.relation_series[macro_index_key][macro_cd],), anal_value=self.relation_right_dfs[macro_index_key][macro_cd],
                                                          title=macro_ctry+'_'+macro_nm+'_single', figsize=panel_size, figshape=(sub_plot_row, math.ceil(self.index_cnt / sub_plot_row)), ylim=(-1,1), img_save=img_save)

        for macro_cd in self.macro_list:
            macro_ctry = self.macro_master_df['ctry'][macro_cd]
            macro_nm = self.macro_master_df['nm'][macro_cd]
            panel.draw_multi_graph_with_matching_analysis(data=self.pivoted_index_value_df
                              , analysis=(self.relation_up_right_series[macro_index_key][macro_cd], self.relation_down_right_series[macro_index_key][macro_cd], self.relation_up_wrong_series[macro_index_key][macro_cd], self.relation_down_wrong_series[macro_index_key][macro_cd])
                              , anal_value=self.relation_right_dfs[macro_index_key][macro_cd], title=macro_ctry+'_'+macro_nm, figsize=panel_size, figshape=(sub_plot_row, math.ceil(self.index_cnt/sub_plot_row))
                              , ylim=(-1,1), img_save=img_save)


if __name__ == '__main__':
    db = WrapDB()
    db.connet(host="127.0.0.1", port=3306, database="macro_cycle", user="root", password="ryumaria")

    ele = FinancialCycle(db)

    # 선행 속성을 가지고 있는 매크로 데이터 리스트 읽기
    ele.get_macro_master()
    # 매크로 데이터의 시계열 값 읽기
    ele.get_macro_value()
    # 매크로 데이터의 기준값 대비 상태 확인
    ele.set_macro_property(type='status')
    # 매크로 데이터의 모멘텀 확인
    ele.set_macro_property(type='momentum')

    # 주요 국가별 지수 데이터 리스트 읽기
    ele.get_index_master()
    # 지수 데이터의 시계열 값 읽
    ele.get_index_value()
    # 지수 데이터의 방향성(상승, 하락) 상태 확인
    ele.set_index_property(type='direction')
    # 지수 데이터의 월단위 수익률 계산
    ele.set_index_property(type='yield')

    # 매크로 데이터의 기준값 대비 상태와 지수 데이터의 방향성이 동일한 경우 확인
    ele.calc_matching_properties_ratio(macro_type='status', index_type='direction')
    # 매크로 데이터의 모멘텀과 지수 데이터의 방향성이 동일한 경우 확인
    ele.calc_matching_properties_ratio(macro_type='momentum', index_type='direction')

    if SC_LOG == True:
        print("################## macro & index matching momentum ratio ##################")
        for idx_col, index_cd in enumerate(ele.index_list):
            if idx_col == 0:
                txt_str = '\t'+str(list(ele.macro_master_df['nm'])).replace(',', '\t').replace('[','').replace(']','').replace("'",'')+'\n'
            for idx_row, macro_cd in enumerate(ele.macro_list):
                if idx_row == 0:
                    txt_str = txt_str+index_cd+':\t'
                txt_str = txt_str+str(round(ele.relation_right_dfs['momentum_direction'][macro_cd][index_cd], 2))+'\t'
            txt_str = txt_str+'\n'
        print(txt_str)

    ele.set_matching_properties_series(macro_type='momentum', index_type='direction')

    # 매크로 데이터들의 평균 모멘텀 적용
    ele.set_matching_properties_weighted_statistic_series(type='mean', threshold=0.0, macro_type='momentum', index_type='direction')
    ele.calc_matching_properties_weighted_statistic_ratio(type='mean', macro_type='momentum', index_type='direction')
    ele.calc_matching_properties_weighted_statistic_profit(type='mean', macro_type='momentum', index_type='direction')

    if SC_LOG == True:
        print("################## macros momentum & index direction matching equal weights ratio ##################")
        for index_cd in ele.relation_right_dfs['momentum_direction'].index:
            print(index_cd+':\t'+str(round(ele.relation_right_dfs['momentum_direction']['mean'][index_cd], 2)))

        print("################## macros momentum & index direction matching equal weights profit ##################")
        for index_cd in ele.relation_profit_dfs['momentum_direction'].index:
            print(index_cd+':\t'+str(round(ele.relation_profit_dfs['momentum_direction']['mean'][index_cd], 2)))

    # 지수별 최적화된 매크로 데이터들의 가중 평균 모멘텀 적용
    # 결과 데이터들이 서로 연관되어 어떤 조합으로 최적화를 해도 동일한 결과 나옴
    up_right_case = copy.deepcopy(ele.relation_up_right_series['momentum_direction'])
    down_right_case = copy.deepcopy(ele.relation_down_right_series['momentum_direction'])
    up_wrong_case = copy.deepcopy(ele.relation_up_wrong_series['momentum_direction'])
    down_wrong_case = copy.deepcopy(ele.relation_down_wrong_series['momentum_direction'])

    macro_list = copy.deepcopy(ele.macro_list)
    index_list = copy.deepcopy(ele.index_list)
    timeseries = copy.deepcopy(ele.index_timeseries)

    weights_list = maximize_hit_ratio(up_right_case, down_right_case, up_wrong_case, down_wrong_case, macro_list, index_list, timeseries, lb=0.1, ub=0.6)
    ele.set_matching_properties_weighted_statistic_series(type='mean', weights_info=('optimized', weights_list), threshold=0.0, macro_type='momentum', index_type='direction')
    ele.calc_matching_properties_weighted_statistic_ratio(type='mean', weights_info=('optimized', weights_list), macro_type='momentum', index_type='direction')
    ele.calc_matching_properties_weighted_statistic_profit(type='mean', weights_info=('optimized', weights_list), macro_type='momentum', index_type='direction')

    if SC_LOG == True:
        print("################## macros momentum & index direction matching optimized weights ratio ##################")
        for index_cd in ele.relation_right_dfs['momentum_direction'].index:
            print(index_cd+':\t'+str(round(ele.relation_right_dfs['momentum_direction']['mean_optimized'][index_cd], 2)))

        print("################## macros momentum & index direction matching optimized weights profit ##################")
        for index_cd in ele.relation_profit_dfs['momentum_direction'].index:
            print(index_cd+':\t'+str(round(ele.relation_profit_dfs['momentum_direction']['mean_optimized'][index_cd], 2)))

        print("################## optimized weights ##################")
        print('\t'+str(list(ele.macro_master_df['nm'])).replace(',', '\t').replace('[','').replace(']','').replace("'",''))
        for weights_cd in weights_list:
            print(weights_cd+':\t'+str(weights_list[weights_cd]).replace(',', '\t').replace('[','').replace(']',''))

        print("################## forecast macro's momentum ##################")
        print(str(list(ele.macro_master_df['nm'])).replace(',', '\t').replace('[', '').replace(']','').replace("'", ''))
        txt_str = ""
        for macro_cd in macro_list:
            txt_str = txt_str+str(ele.macro_last_df['momentum'][macro_cd].values[0])+'\t'
        print(txt_str)

        print("################## forecast index's direction ##################")
        for weights_cd in weights_list:
            print(weights_cd+':\t'+str(round(sum(weights_list[weights_cd]*ele.macro_last_df['momentum'].values[0]), 2)))

    ele.do_figure(weights_info=('optimized', weights_list), img_save='y')
    ele.save_log()

    db.disconnect()
