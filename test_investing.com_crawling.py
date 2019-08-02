#_*_ coding: utf-8 _*_

from Test_MariaDB import WrapDB
from Wrap_Folione import Preprocess
import Wrap_Util
import pandas as pd
import statsmodels.api as sm
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import numpy as np
import json
import sys
import math
import copy
import warnings
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import crawling
from crawling import *
import re
from datetime import date


warnings.filterwarnings("ignore")


def getRealValue(s):
    try:
        if len(s) == 0:
            value = 'NULL'
        else:
            s = s.replace(',', '')
            if s[-1] == 'K':
                value = float(s[:-1]) * 1000
            elif s[-1] == 'M':
                value = float(s[:-1]) * 1000000
            elif s[-1] == 'B':
                value = float(s[:-1]) * 1000000000
            elif s[-1] == '%':
                value = float(s[:-1]) / 100
            elif s[-1] == '-':
                value = 0.0
            else:
                value = float(s)

        return value

    except (ValueError, TypeError) as e:
        print('에러정보 : ', e, file=sys.stderr)

        return None

# Wrap운용팀 DB Connect
db = WrapDB()
db.connet(host="127.0.0.1", port=3306, database="investing.com", user="root", password="ryumaria")

calendar_map = {'Jan': 1,'Feb': 2,'Mar': 3,'Apr': 4,'May': 5,'Jun': 6,'Jul': 7,'Aug': 8,'Sep': 9,'Oct': 10,'Nov': 11,'Dec': 12,'Q1': 1,'Q2': 2,'Q3': 3,'Q4': 4}

if 1:
    datas = db.select_query("SELECT cd, nm_us, link, ctry, period"
                            "  FROM economic_events"
                            " WHERE imp_us in (1,2,3)")
    datas.columns = ['cd', 'nm_us', 'link', 'ctry', 'period']
    #print(type(datas))

    for data in datas.iterrows():
        cd = data[1]['cd']
        nm = data[1]['nm_us']
        link = data[1]['link']
        ctry = data[1]['ctry']
        period = data[1]['period']

        '''
        if cd < 1811:
            print('continue: ', nm, link)
            continue
        '''
        results = crawling.InvestingEconomicEventCalendar(link, cd)
        print(nm, link, type(results))
        #print(results)

        # 통계시점에 대한 정보가 없는 경우에 이전 데이터에 대한 정보를 사용해서 추정
        pre_statistics_time = 0
        # 시계역 역순(가장 최근 데이터가 처음)
        for result in results:
            try:
                date_rlt = result['date']
                date_splits = re.split('\W+', date_rlt)
                date_str = str(date(int(date_splits[2]), calendar_map[date_splits[0]], int(date_splits[1])))
                
                # 통계시점에 대한 정보가 없는 경우 주기가 monthly인 경우 처리
                statistics_time = 'NULL' if len(date_splits) <= 3 else calendar_map[date_splits[3]]
                if period == 'M':
                    # 첫 데이터인 경우
                    if pre_statistics_time == 0:
                        pre_statistics_time = statistics_time + 1 if statistics_time < 12 else 1

                    if statistics_time == 'NULL':
                        statistics_time = pre_statistics_time - 1 if pre_statistics_time > 1 else 12

                    pre_statistics_time = statistics_time

                time = result['time']
                # GDP처럼 추정치가 먼저 발표되는 경우는 시간 뒤에 'P'가 붙는다
                if len(time) > 5:
                    print(nm, result['date'], result['time'])
                    break

                bold = result['bold']
                fore = result['fore']
                prev = result['prev']

                # 단위가 K, M, %으로 다양하여 실제 수치로 변경
                bold_flt = getRealValue(result['bold'])
                fore_flt = getRealValue(result['fore'])

                sql = "INSERT INTO economic_events_schedule (event_cd, release_date, release_time, statistics_time, bold_value, fore_value) " \
                      "VALUES (%s, '%s', '%s', %s, %s, %s) ON DUPLICATE KEY UPDATE release_time = '%s', statistics_time = %s, bold_value = %s, fore_value = %s"
                sql_arg = (cd, date_str, time, statistics_time, bold_flt, fore_flt, time, statistics_time, bold_flt, fore_flt)

                if(db.insert_query(sql, sql_arg) == False):
                    print(sql % sql_arg)
            except (TypeError, KeyError) as e:
                print('에러정보 : ', e, file=sys.stderr)
                print(date_splits, pre_statistics_time)

if 0:
    i = crawling.InvestingEconomicCalendar('https://www.investing.com/economic-calendar/')
    i.getEvents()


if 0:
    master_list = db.select_query("SELECT cd, nm_us, curr_id, smlID"
                            "  FROM index_master")
    master_list.columns = ['cd', 'nm_us', 'curr_id', 'smlID']


    for master in master_list.iterrows():
        # first set Headers and FormData
        ihd = IndiceHistoricalData('https://www.investing.com/instruments/HistoricalDataAjax')

        header = {	'name' : master[1]['cd'],
                    'curr_id': master[1]['curr_id'],
                    'smlID': master[1]['smlID'],
                    'header' : master[1]['nm_us'],
                    'sort_col' : 'date',
                    'action' : 'historical_data'}
        ihd.setFormData(header)

        # second set Variables
        ihd.updateFrequency('Daily')
        ihd.updateStartingEndingDate('1/1/2010', '7/25/2019')
        ihd.setSortOreder('ASC')
        ihd.downloadData()
        #ihd.printData()

        results = ihd.observations
        for result in results.iterrows():
            try:
                cd = master[1]['cd']

                date_rlt = result[1]['Date']
                date_splits = re.split('\W+', date_rlt)
                date_str = str(date(int(date_splits[2]), calendar_map[date_splits[0]], int(date_splits[1])))

                close = result[1]['Price']
                open = result[1]['Open']
                high = result[1]['High']
                low = result[1]['Low']

                vol = getRealValue(result[1]['Vol.'])

                sql = "INSERT INTO index_price (idx_cd, date, close, open, high, low, vol) " \
                      "VALUES ('%s', '%s', %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE close = %s, open = %s, high = %s, low = %s, vol = %s"
                sql_arg = (cd, date_str, close, open, high, low, vol, close, open, high, low, vol)
                #print(sql % sql_arg)

                if (db.insert_query(sql, sql_arg) == False):
                    print(sql % sql_arg)
            except (TypeError, KeyError) as e:
                print('에러정보 : ', e, file=sys.stderr)
                print(date_splits, pre_statistics_time)
        #ihd.saveDataCSV()