#_*_ coding: utf-8 _*_

#import win32com.client
from Test_MariaDB import WrapDB
from Test_Figure import Figure
import Wrap_Util
import pandas as pd
import numpy as np
import math
import platform
import os
import copy

PRINT_SC = True
LOG = True
SINGLE_FIGURE = False
MULTI_FIGURE = True

base_dir = (os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

if 1:
    db = WrapDB()
    db.connet(host="127.0.0.1", port=3306, database="macro_cycle", user="root", password="ryumaria")

    # 매크로 시계열 데이터 셋
    sql = "select a.cd as cd, a.nm as nm, a.ctry as ctry, a.base as base" \
          "  from macro_master a" \
          "     , macro_value b" \
          " where a.base is not null" \
          "   and a.cd = b.cd" \
          " group by a.cd, a.nm, a.ctry, a.base" \
          " having count(*) > 0"
    macro_master_df = db.select_query(sql)
    macro_master_df.columns = ('cd', 'nm', 'ctry', 'base')
    macro_master_df.set_index('cd', inplace=True)

    sql = "select cd, date, value" \
          "  from macro_value"
    macro_value_df = db.select_query(sql)
    macro_value_df.columns = ('cd', 'date', 'value')
    pivoted_macro_value_df = macro_value_df.pivot(index='date', columns='cd', values='value')

    # 매크로 데이터가 기준값 이상인 경우 1 or 0
    pivoted_macro_status_df = pd.DataFrame(columns=pivoted_macro_value_df.columns, index=pivoted_macro_value_df.index)
    pivoted_macro_momentum_df = pd.DataFrame(columns=pivoted_macro_value_df.columns, index=pivoted_macro_value_df.index)
    for macro_cd in pivoted_macro_value_df.columns:
        base_value = macro_master_df['base'][macro_cd]
        for idx, date_cd in enumerate(pivoted_macro_value_df.index):
            pivoted_macro_status_df[macro_cd][date_cd] = 1 if pivoted_macro_value_df[macro_cd][date_cd] > base_value else 0
            if idx > 0:
                pivoted_macro_momentum_df[macro_cd][date_cd] = 1 if pivoted_macro_value_df[macro_cd][date_cd] > prev_value else 0
            prev_value = pivoted_macro_value_df[macro_cd][date_cd]
    # 지수 움직임과 시점을 맞추기 위해 1개월 lag
    pivoted_macro_status_sh1m_df = pivoted_macro_status_df.shift(1)
    pivoted_macro_momentum_sh1m_df = pivoted_macro_momentum_df.shift(1)

    # 지수 시계열 데이터 셋팅
    sql = "select cd, nm, ctry" \
          "  from index_master"
    index_master_df = db.select_query(sql)
    index_master_df.columns = ('cd', 'nm', 'ctry')
    index_master_df.set_index('cd', inplace=True)

    sql = "select cd, date, value" \
          "  from index_value"
    index_value_df = db.select_query(sql)
    index_value_df.columns = ('cd', 'date', 'value')
    pivoted_index_value_df = index_value_df.pivot(index='date', columns='cd', values='value')

    # 지수가 상승한 경우 1 or 0
    pivoted_index_status_df = pd.DataFrame(columns=pivoted_index_value_df.columns, index=pivoted_index_value_df.index)
    for index_cd in pivoted_index_value_df.columns:
        for idx, date_cd in enumerate(pivoted_index_value_df.index):
            if idx > 0:
                pivoted_index_status_df[index_cd][date_cd] = 1 if pivoted_index_value_df[index_cd][date_cd] > prev_value else 0
            prev_value = pivoted_index_value_df[index_cd][date_cd]

    # 매크로 데이터가 기준값 이상이고 지수가 상승, 매크로 데이터가 기준값 이하이고 지수가 하락인 경우 COUNT
    result_status_up = pd.DataFrame(columns=macro_master_df['nm'], index=index_master_df['nm'])
    result_status_down = pd.DataFrame(columns=macro_master_df['nm'], index=index_master_df['nm'])
    result_momentum_up = pd.DataFrame(columns=macro_master_df['nm'], index=index_master_df['nm'])
    result_momentum_down = pd.DataFrame(columns=macro_master_df['nm'], index=index_master_df['nm'])
    result_status_momentum_up = pd.DataFrame(columns=macro_master_df['nm'], index=index_master_df['nm'])
    result_status_momentum_down = pd.DataFrame(columns=macro_master_df['nm'], index=index_master_df['nm'])
    result_status = pd.DataFrame(columns=macro_master_df['nm'], index=index_master_df['nm'])
    result_momentum = pd.DataFrame(columns=macro_master_df['nm'], index=index_master_df['nm'])
    result_status_momentum = pd.DataFrame(columns=macro_master_df['nm'], index=index_master_df['nm'])

    # 매크로 데이터 모멘텀 발견한 경우
    result_momentum_up_right = {}
    result_momentum_down_right = {}

    for macro_cd in pivoted_macro_status_sh1m_df.columns:
        macro_nm = macro_master_df['nm'][macro_cd]
        result_momentum_up_right[macro_nm] = pd.DataFrame(columns=pivoted_index_status_df.columns, index=pivoted_index_status_df.index)
        result_momentum_down_right[macro_nm] = pd.DataFrame(columns=pivoted_index_status_df.columns, index=pivoted_index_status_df.index)
        for index_cd in pivoted_index_status_df.columns:
            index_nm = index_master_df['nm'][index_cd]
            right_status_up_cnt = 0
            right_status_down_cnt = 0
            right_momentum_up_cnt = 0
            right_momentum_down_cnt = 0
            right_status_momentum_up_cnt = 0
            right_status_momentum_down_cnt = 0
            for date_cd in pivoted_index_status_df.index:
                # 통계 값에 nan에 의한 오류는 무시
                if pivoted_macro_status_sh1m_df[macro_cd][date_cd] == 1 and pivoted_index_status_df[index_cd][date_cd] == 1:
                    right_status_up_cnt += 1
                elif pivoted_macro_status_sh1m_df[macro_cd][date_cd] == 0 and pivoted_index_status_df[index_cd][date_cd] == 0:
                    right_status_down_cnt += 1
                if pivoted_macro_momentum_sh1m_df[macro_cd][date_cd] == 1 and pivoted_index_status_df[index_cd][date_cd] == 1:
                    right_momentum_up_cnt += 1
                    result_momentum_up_right[macro_nm][index_cd][prve_date_cd] = 1
                elif pivoted_macro_momentum_sh1m_df[macro_cd][date_cd] == 0 and pivoted_index_status_df[index_cd][date_cd] == 0:
                    right_momentum_down_cnt += 1
                    result_momentum_down_right[macro_nm][index_cd][prve_date_cd] = 1
                if pivoted_macro_status_sh1m_df[macro_cd][date_cd] == 1 and pivoted_macro_momentum_sh1m_df[macro_cd][date_cd] == 1 and pivoted_index_status_df[index_cd][date_cd] == 1:
                    right_status_momentum_up_cnt += 1
                elif pivoted_macro_status_sh1m_df[macro_cd][date_cd] == 0 and pivoted_macro_momentum_sh1m_df[macro_cd][date_cd] == 0 and pivoted_index_status_df[index_cd][date_cd] == 0:
                    right_status_momentum_down_cnt += 1

                prve_date_cd = date_cd

            result_status_up[macro_nm][index_nm] = right_status_up_cnt
            result_status_down[macro_nm][index_nm] = right_status_down_cnt
            result_momentum_up[macro_nm][index_nm] = right_momentum_up_cnt
            result_momentum_down[macro_nm][index_nm] = right_momentum_down_cnt
            result_status_momentum_up[macro_nm][index_nm] = right_status_momentum_up_cnt
            result_status_momentum_down[macro_nm][index_nm] = right_status_momentum_down_cnt
            result_status[macro_nm][index_nm] = right_status_up_cnt + right_status_down_cnt
            result_momentum[macro_nm][index_nm] = right_momentum_up_cnt + right_momentum_down_cnt
            result_status_momentum[macro_nm][index_nm] = right_status_momentum_up_cnt + right_status_momentum_down_cnt

    if LOG:
        if platform.system() == 'Windows':
            Wrap_Util.SaveExcelFiles(file='%s\\corr ratio.xlsx' % (base_dir)
                                     , obj_dict={'result_status': result_status, 'result_momentum': result_momentum, 'result_status_momentum': result_status_momentum
                                                 , 'pivoted_macro_status_df': pivoted_macro_status_df, 'pivoted_macro_momentum_df' : pivoted_macro_momentum_df
                                                 , 'pivoted_index_status_df': pivoted_index_status_df
                                                 , 'pivoted_index_value_df': pivoted_index_value_df, 'pivoted_macro_value_df': pivoted_macro_value_df})
        if PRINT_SC:
            print('################ corr ratio ################')
            print(result_status_up)
            print(result_momentum_up)
            print(result_status_momentum_up)
            print(result_status_down)
            print(result_momentum_down)
            print(result_status_momentum_down)
            print('############################################')

    if SINGLE_FIGURE:
        panel = Figure()
        for macro_cd in pivoted_macro_momentum_df.columns:
            for index_cd in pivoted_index_value_df.columns:

                macro_ctry =macro_master_df['ctry'][macro_cd]
                macro_nm = macro_master_df['nm'][macro_cd]
                index_nm = index_master_df['nm'][index_cd]

                plot_df = pd.DataFrame()
                plot_df[macro_nm] = pivoted_macro_status_df[macro_cd]
                plot_df[index_nm] = pivoted_index_value_df[index_cd]
                if PRINT_SC:
                    print('############################################')
                    print(plot_df)
                    print('############################################')
                panel.draw(plot_df, title=macro_ctry+'_'+macro_nm, subplots=[index_nm], figsize=(10,5))

    if MULTI_FIGURE:
        panel = Figure()
        for macro_cd in pivoted_macro_momentum_df.columns:

            macro_ctry = macro_master_df['ctry'][macro_cd]
            macro_nm = macro_master_df['nm'][macro_cd]

            plot_df = copy.deepcopy(pivoted_index_value_df)
            plot_df[macro_nm] = pivoted_macro_momentum_df[macro_cd]

            #panel.draw_multi(plot_df, title=macro_ctry+'_'+macro_nm, subplot_nm=macro_nm, figsize=(20,10), figshape=(2, math.ceil(len(pivoted_index_value_df.columns)/2)))
            panel.draw_multi2(pivoted_index_value_df, result_momentum_up_right[macro_nm], result_momentum_down_right[macro_nm], title=macro_ctry+'_'+macro_nm, subplot_nm=macro_nm, figsize=(20, 10), figshape=(2, math.ceil(len(pivoted_index_value_df.columns) / 2)), img_save='y')

db.disconnect()


