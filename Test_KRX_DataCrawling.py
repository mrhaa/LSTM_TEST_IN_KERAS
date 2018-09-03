
import pandas as pd
import numpy as np
import requests as rq

from bs4 import BeautifulSoup
from io import BytesIO
from datetime import datetime, timedelta

def get_stock_master(stock_master_info):
    r = rq.post(stock_master_info['url'], data=stock_master_info['data'])
    f = BytesIO(r.content)
    dfs = pd.read_html(f, header=0, parse_dates=['상장일'])
    df = dfs[0].copy()

    # 숫자를 앞자리가 0인 6자리 문자열로 변환
    df['종목코드'] = df['종목코드'].astype(np.str).str.zfill(6)

    return df

def get_stock_master_OTP(stock_master_info):
    # STEP 01: Generate OTP
    r = rq.post(stock_master_info['url'], data=stock_master_info['data'])
    code = r.content

    # STEP 02: download
    down_url = 'http://file.krx.co.kr/download.jspx'
    down_data = {
        'code': code,
    }

    r = rq.post(down_url, down_data)
    f = BytesIO(r.content)

    usecols = ['종목코드', '기업명', '업종코드', '업종']
    df = pd.read_excel(f, converters={'종목코드': str, '업종코드': str}, usecols=usecols)
    df.columns = ['code', 'name', 'sector_code', 'sector']

    return df

def get_stock_historical_OTP(stock_historical_info):
    # STEP 01: Generate OTP
    r = rq.post(stock_historical_info['url'], stock_historical_info['data'])
    code = r.content  # 리턴받은 값을 아래 요청의 입력으로 사용.

    # STEP 02: download
    down_url = 'http://file.krx.co.kr/download.jspx'
    down_data = {
        'code': code,
    }

    r = rq.post(down_url, down_data)
    df = pd.read_excel(BytesIO(r.content), header=0, thousands=',')

    return df

def get_stock_finance_OTP(stock_finance_info):
    # STEP 01: Generate OTP
    r = rq.post(stock_finance_info['url'], stock_finance_info['data'])
    code = r.content  # 리턴받은 값을 아래 요청의 입력으로 사용.

    # STEP 02: download
    down_url = 'http://file.krx.co.kr/download.jspx'
    down_data = {
        'code': code,
    }

    r = rq.post(down_url, down_data)
    df = pd.read_excel(BytesIO(r.content), header=0, thousands=',')

    return df

if 1:
    stock_master_info = {'url': 'http://kind.krx.co.kr/corpgeneral/corpList.do'
                      , 'data': {'method':'download'
                                , 'orderMode':'1'          # 정렬컬럼
                                , 'orderStat':'D'          # 정렬 내림차순
                                , 'searchType':'13'        # 검색유형: 상장법인
                                , 'fiscalYearEnd':'all'    # 결산월: 전체
                                , 'location':'all'         # 지역: 전체
                            }
                        }

    df_master_data = get_stock_master(stock_master_info)
    #print(df_master_data.head())
    column_str = ''
    for column in df_master_data.columns:
        column_str = column_str + column + '\t'
    print(column_str)

    for idx, row in enumerate(df_master_data.index):
        row_str = ''
        for column in df_master_data.columns:
            row_str = row_str + str(df_master_data[column][row]) + '\t'
        print(row_str)

# Not Working
if 0:
    stock_master_OTP_info = {'url': 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
                        , 'data': {'name': 'fileDown'
                                , 'filetype': 'xls'
                                , 'url': 'MKD/04/0406/04060100/mkd04060100_01'
                                , 'market_gubun': 'ALL'  # ALL:전체
                                , 'isu_cdnm': '전체'
                                , 'sort_type': 'A'  # 정렬 : A 기업명
                                , 'std_ind_cd': '01'
                                , 'cpt': '1'
                                , 'in_cpt': ''
                                , 'in_cpt2': ''
                                , 'pagePath': '/contents/MKD/04/0406/04060100/MKD04060100.jsp'
                            }  # Query String Parameters
                        }

    df_master_data = get_stock_master_OTP(stock_master_OTP_info)
    print(df_master_data.head())



if 0:
    stock_historical_OTP_info = {'url': 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
                    , 'data': {'name': 'fileDown'
                                , 'filetype': 'xls'
                                , 'url': 'MKD/04/0404/04040200/mkd04040200_01'
                                , 'market_gubun': 'ALL'  # 시장구분: ALL=전체
                                , 'indx_ind_cd': ''
                                , 'sect_tp_cd': ''
                                , 'schdate': datetime(2018, 8, 30).strftime('%Y%m%d')
                                , 'pagePath': '/contents/MKD/04/0404/04040200/MKD04040200.jsp'
                            }
                        }

    old_to = 2
    for loop in range(1, old_to, 1):
        stock_historical_OTP_info['data']['schdate'] = (datetime.now() - timedelta(days=loop)).strftime('%Y%m%d')

        df_historical_data = get_stock_historical_OTP(stock_historical_OTP_info)
        df_historical_data['날짜'] = stock_historical_OTP_info['data']['schdate']

        if loop == 1:
            column_str = ''
            for column in df_historical_data.columns:
                column_str = column_str + column + '\t'
            print(column_str)

        if len(df_historical_data):
            #print(df_historical_data)
            print(loop, stock_historical_OTP_info['data']['schdate'])


if 0:
    stock_finance_OTP_info = {'url': 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
                    , 'data': {'name': 'fileDown'
                            , 'filetype' : "xls"
                            , 'url' : "MKD/13/1301/13010104/mkd13010104_02"
                            , 'type' : "ALL"
                            , 'period_selector' : "day"
                            , 'fromdate' : "20170101"
                            , 'todate' : "20180801"
                            , 'pagePath' : "/contents/MKD/13/1301/13010104/MKD13010104.jsp"
                            }
                        }

    df_finance_data = get_stock_finance_OTP(stock_finance_OTP_info)
    #print(df_finance_data.head())
    column_str = ''
    for column in df_finance_data.columns:
        column_str = column_str + column + '\t'
    print(column_str)

    for idx, row in enumerate(df_finance_data.index):
        row_str = ''
        for column in df_finance_data.columns:
            row_str = row_str + str(df_finance_data[column][row]) + '\t'
        #print(row_str)
