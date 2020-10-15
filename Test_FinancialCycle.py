#_*_ coding: utf-8 _*_

#import win32com.client
from Test_MariaDB import WrapDB
import pandas as pd
import numpy as np
import math


db = WrapDB()
db.connet(host="127.0.0.1", port=3306, database="macro_cycle", user="root", password="ryumaria")

# 매크로 시계열 데이터 셋
sql = "select cd, nm, ctry, base" \
      "  from macro_master" \
      " where base is not null"
macro_master_df = db.select_query(sql)
macro_master_df.columns = ('cd', 'nm', 'ctry', 'base')
macro_master_df.set_index('cd', inplace=True)

sql = "select cd, date, value" \
      "  from macro_value"
macro_value_df = db.select_query(sql)
macro_value_df.columns = ('cd', 'date', 'value')
pivoted_macro_value_df = macro_value_df.pivot(index='date', columns='cd', values='value')

pivoted_macro_status_df = pd.DataFrame(columns=pivoted_macro_value_df.columns, index=pivoted_macro_value_df.index)
for macro_cd in pivoted_macro_value_df.columns:
    base_value = macro_master_df['base'][macro_cd]
    for date_cd in pivoted_macro_value_df.index:
        pivoted_macro_status_df[macro_cd][date_cd] = 1 if pivoted_macro_value_df[macro_cd][date_cd] > base_value else 0


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

pivoted_index_status_df = pd.DataFrame(columns=pivoted_index_value_df.columns, index=pivoted_index_value_df.index)
for index_cd in pivoted_index_value_df.columns:
    for idx, date_cd in enumerate(pivoted_index_value_df.index):
        if idx > 0:
            pivoted_index_status_df[index_cd][date_cd] = 1 if pivoted_index_value_df[index_cd][date_cd] > prev_value else 0
        prev_value = pivoted_index_value_df[index_cd][date_cd]


result = pd.DataFrame(columns=pivoted_macro_status_df.columns, index=pivoted_index_status_df.columns)
for macro_cd in pivoted_macro_status_df.columns:
    for index_cd in pivoted_index_status_df.columns:
        right_cnt = 0
        for date_cd in pivoted_index_status_df.index:
            if pivoted_macro_status_df[macro_cd][date_cd] == pivoted_index_status_df[index_cd][date_cd] and math.isnan(pivoted_macro_status_df[macro_cd][date_cd]) == False and math.isnan(pivoted_index_status_df[index_cd][date_cd]) == False:
                right_cnt += 1
        result[macro_cd][index_cd] = round(right_cnt/len(pivoted_index_status_df.index)*100, 2)

db.disconnect()


