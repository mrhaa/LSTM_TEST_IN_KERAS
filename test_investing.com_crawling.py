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
import re
from datetime import date


warnings.filterwarnings("ignore")



# Wrap운용팀 DB Connect
db = WrapDB()
db.connet(host="127.0.0.1", port=3306, database="investing.com", user="root", password="ryumaria")

calendar_map = {'Jan': 1,'Feb': 2,'Mar': 3,'Apr': 4,'May': 5,'Jun': 6,'Jul': 7,'Aug': 8,'Sep': 9,'Oct': 10,'Nov': 11,'Dec': 12}

if 1:
    datas = db.select_query("SELECT cd, nm_kr, link, ctry"
                            "  FROM economic_calendar ")
    datas.columns = ['cd', 'nm_kr', 'link', 'ctry']
    #print(type(datas))

    for data in datas.iterrows():
        cd = data[1]['cd']
        nm = data[1]['nm_kr']
        link = data[1]['link']
        ctry = data[1]['ctry']

        results = crawling.InvestingEconomicEventCalendar(link, cd)
        print(nm, link, type(results))
        #print(results)

        for result in reversed(results):
            date_rlt = result['date']
            date_splits = re.split('\W+', date_rlt)
            print(date_rlt, date_splits, date(int(date_splits[2]), calendar_map[date_splits[0]], int(date_splits[1])))
            time = result['time']
            bold = result['bold']
            fore = result['fore']
            prev = result['prev']

else:
    i = crawling.InvestingEconomicCalendar('https://www.investing.com/economic-calendar/')
    i.getEvents()



