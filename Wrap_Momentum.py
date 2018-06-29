#_*_ coding: utf-8 _*_

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import math
from datetime import datetime
from decimal import Decimal, DecimalException
from scipy.stats.kde import gaussian_kde
from scipy.stats import norm
from Test_MariaDB import WrapDB

print_log = True

if __name__ == '__main__':

    # Wrap운용팀 DB Connect
    db = WrapDB()
    db.connet(host="127.0.0.1", port=3306, database="WrapDB_2", user="root", password="maria")
    data_infos = db.get_data_info()

    data = db.get_quantiwise_datas(data_infos[0])
    data.columns = ["아이템코드", "아이템명", "날짜", "시가", "종가", "거래량", "시가총액", "기관_순매수", "외인_순매수"]

    pivoted_datas = {}
    pivoted_datas["시가"] = data.pivot(index="날짜", columns="아이템명", values="시가")
    pivoted_datas["종가"] = data.pivot(index="날짜", columns="아이템명", values="종가")
    pivoted_datas["거래량"] = data.pivot(index="날짜", columns="아이템명", values="거래량")
    pivoted_datas["시가총액"] = data.pivot(index="날짜", columns="아이템명", values="시가총액")
    pivoted_datas["기관_순매수"] = data.pivot(index="날짜", columns="아이템명", values="기관_순매수")
    pivoted_datas["외인_순매수"] = data.pivot(index="날짜", columns="아이템명", values="외인_순매수")

    mean_window = 20
    pivoted_datas["평균_거래량"] = pivoted_datas["거래량"].rolling(mean_window).mean()
    pivoted_datas["평균_기관_순매수"] = pivoted_datas["기관_순매수"].rolling(mean_window).mean()
    pivoted_datas["평균_외인_순매수"] = pivoted_datas["외인_순매수"].rolling(mean_window).mean()

    # 통계 분석 변수
    case_statistics = {"normal": {"count": 0, "rate": 0.0, "date": 0.0}, "loss_cut": {"count": 0, "rate": 0.0, "date": 0.0},"open": {"count": 0, "rate": 0.0, "date": 0.0}}

    # 글로벌 셋팅 파라미터
    volume_surpise_multiple = 20.0
    use_loss_cut = True
    loss_cut_ratio = -0.30 # -30%에서 손절
    position_in_threshold = 0.0
    position_out_threshold = -10.0

    # 1차 시간순으로 처리
    for idx, row_nm in enumerate(pivoted_datas["거래량"].index):
        
        # 종목별 상태 관련 변수 
        if idx == 0:
            event_dates_dict = {}
            status = {}
            buy_price = {}
            sell_price = {}
            buy_day = {}
            sell_day = {}

        # Start Day 보다 이전 데이터 또는 마지막 데이터(무결성 문제 가능)는 PASS
        if datetime.strptime(row_nm,'%Y-%m-%d').date() < datetime.strptime("2006-01-01",'%Y-%m-%d').date() or row_nm == pivoted_datas["거래량"].index[-1]:
            continue

        # 2차 종목순으로 처리
        for column_nm in pivoted_datas["거래량"].columns:

            # 종목별 상태 관련 변수 초기화
            if idx == 0:
                event_dates_dict[column_nm] = {}
                status[column_nm] = False  # False: No Position, True: Position
                buy_price[column_nm] = 0.0
                sell_price[column_nm] = 0.0
                buy_day[column_nm] = "1111-11-11"
                sell_day[column_nm] = "9999-99-99"

            try:
                # 데이터 존재 여부 및 시가총액 필터링
                if pivoted_datas["거래량"][column_nm][idx] != None:# and pivoted_datas["시가총액"][column_nm][idx] > 500000000000: #최초 DB 생성시 시가종행 50억에 거래금액 10억 이상으로 제한함.

                    # open 상태로 완료된 시뮬레이션
                    #if row_nm == pivoted_datas["거래량"].index[-1]:
                    if row_nm == pivoted_datas["거래량"].index[-2]:
                        if status[column_nm] == True:
                            status[column_nm] = False
                            sell_day[column_nm] = row_nm
                            sell_price[column_nm] = float(pivoted_datas["종가"][column_nm][sell_day[column_nm]])

                            if print_log == True:
                                print("open", '\t', column_nm, '\t', buy_day[column_nm], '\t', sell_day[column_nm], '\t', buy_price[column_nm], '\t', sell_price[column_nm], '\t'
                                      , sell_price[column_nm] / buy_price[column_nm] - 1, '\t', (datetime.strptime(sell_day[column_nm], '%Y-%m-%d').date() - datetime.strptime(buy_day[column_nm],'%Y-%m-%d').date()).days)

                            case_statistics["open"]["count"] += 1
                            case_statistics["open"]["rate"] += sell_price[column_nm] / buy_price[column_nm] - 1
                            case_statistics["open"]["date"] += (datetime.strptime(sell_day[column_nm], '%Y-%m-%d').date() - datetime.strptime(buy_day[column_nm],'%Y-%m-%d').date()).days

                    # 과거 평균 거래량 보다 비상적으로 많은 거래가 발생하는 경우
                    # 외인과 기관에서 순매수 발생
                    if status[column_nm] == False:
                        #print(column_nm, row_nm, pivoted_datas["거래량"].index[-2], pivoted_datas["거래량"][column_nm][idx], pivoted_datas["평균_거래량"][column_nm][idx-1], pivoted_datas["평균_기관_순매수"][column_nm][idx], pivoted_datas["평균_외인_순매수"][column_nm][idx])
                        if pivoted_datas["거래량"][column_nm][idx] > volume_surpise_multiple * pivoted_datas["평균_거래량"][column_nm][idx-1] \
                            and (pivoted_datas["평균_기관_순매수"][column_nm][idx] + pivoted_datas["평균_외인_순매수"][column_nm][idx] > position_in_threshold)\
                            and (pivoted_datas["종가"][column_nm][idx] / pivoted_datas["시가"][column_nm][idx] > 1.0):
                            #and pivoted_datas["평균_외인_순매수"][column_nm][idx] > 1.0:


                            # 거래정지 해지에 의한 거래량 증가 필터링
                            # 과거 최근 일자에 거래정지 기간이 있어 평균 거래량이 작게 계산된 경우 PASS
                            trade_stop_happened = False
                            for loop in range(1, mean_window+1):
                                if pivoted_datas["거래량"][column_nm][idx - loop] == 0:
                                    trade_stop_happened = True
                                    break
                            if trade_stop_happened == True:
                                continue

                            # Event Noise 판단 기간: 5영업일 내에 거래량 증가가 또 발생하는 경우 Noise로 판단
                            if len(event_dates_dict[column_nm]) == 0 or idx > list(event_dates_dict[column_nm].keys())[-1] + 5:
                                event_dates_dict[column_nm][idx] = row_nm

                                # 역사적 저점 판단: 50영업일
                                a = float(pivoted_datas["종가"][column_nm][idx]) - float(pivoted_datas["종가"][column_nm][max(idx - 50, 0):idx].min())
                                b = float(pivoted_datas["종가"][column_nm][max(idx - 50, 0):idx].max()) - float(pivoted_datas["종가"][column_nm][max(idx - 50, 0):idx].min())
                                if 1:#a / b >= 0.5:
                                    status[column_nm] = True
                                    buy_price[column_nm] = float(pivoted_datas["시가"][column_nm][idx+1])
                                    buy_day[column_nm] = row_nm

                    elif status[column_nm] == True :
                        # loss cut
                        if use_loss_cut == True and float(pivoted_datas["종가"][column_nm][row_nm]) / buy_price[column_nm] - 1 <= loss_cut_ratio:
                            status[column_nm] = False
                            sell_price[column_nm] = float(pivoted_datas["시가"][column_nm][idx+1])
                            sell_day[column_nm] = row_nm

                            if print_log == True:
                                print("loss_cut", '\t', column_nm, '\t', buy_day[column_nm], '\t', sell_day[column_nm], '\t', buy_price[column_nm], '\t', sell_price[column_nm], '\t'
                                      , sell_price[column_nm] / buy_price[column_nm] - 1, '\t', (datetime.strptime(sell_day[column_nm], '%Y-%m-%d').date() - datetime.strptime(buy_day[column_nm],'%Y-%m-%d').date()).days, '\t'
                                      , pivoted_datas["평균_기관_순매수"][column_nm][buy_day[column_nm]], '\t',pivoted_datas["평균_기관_순매수"][column_nm][sell_day[column_nm]], '\t'
                                      , pivoted_datas["평균_외인_순매수"][column_nm][buy_day[column_nm]], '\t', pivoted_datas["평균_외인_순매수"][column_nm][sell_day[column_nm]])

                            case_statistics["loss_cut"]["count"] += 1
                            case_statistics["loss_cut"]["rate"] += sell_price[column_nm] / buy_price[column_nm] - 1
                            case_statistics["loss_cut"]["date"] += (datetime.strptime(sell_day[column_nm], '%Y-%m-%d').date() - datetime.strptime(buy_day[column_nm],'%Y-%m-%d').date()).days

                        # 외인과 기관의 순매도 발생
                        #if pivoted_datas["평균_기관_순매수"][column_nm][idx] < -10.0:
                        #if pivoted_datas["평균_외인_순매수"][column_nm][idx] < -10.0 or pivoted_datas["평균_외인_순매수"][column_nm][idx] < -10.0:
                        if pivoted_datas["평균_외인_순매수"][column_nm][idx] + pivoted_datas["평균_외인_순매수"][column_nm][idx] < position_out_threshold:
                            status[column_nm] = False
                            sell_price[column_nm] = float(pivoted_datas["시가"][column_nm][idx+1])
                            sell_day[column_nm] = row_nm

                            if print_log == True:
                                print("normal", '\t', column_nm, '\t', buy_day[column_nm], '\t', sell_day[column_nm], '\t', buy_price[column_nm], '\t', sell_price[column_nm], '\t'
                                      , sell_price[column_nm] / buy_price[column_nm] - 1, '\t', (datetime.strptime(sell_day[column_nm], '%Y-%m-%d').date() - datetime.strptime(buy_day[column_nm],'%Y-%m-%d').date()).days, '\t'
                                      , pivoted_datas["평균_기관_순매수"][column_nm][buy_day[column_nm]], '\t',pivoted_datas["평균_기관_순매수"][column_nm][sell_day[column_nm]], '\t'
                                      , pivoted_datas["평균_외인_순매수"][column_nm][buy_day[column_nm]], '\t', pivoted_datas["평균_외인_순매수"][column_nm][sell_day[column_nm]])

                            case_statistics["normal"]["count"] += 1
                            case_statistics["normal"]["rate"] += sell_price[column_nm] / buy_price[column_nm] - 1
                            case_statistics["normal"]["date"] += (datetime.strptime(sell_day[column_nm], '%Y-%m-%d').date() - datetime.strptime(buy_day[column_nm],'%Y-%m-%d').date()).days

            except IndexError:
                pass
            except (ValueError, DecimalException):
                pass

    # print log
    print("normal", '\t', case_statistics["normal"]["count"], '\t', case_statistics["normal"]["rate"] / case_statistics["normal"]["count"], '\t'
          , case_statistics["normal"]["date"] / case_statistics["normal"]["count"])
    if case_statistics["loss_cut"]["count"] > 0:
        print("loss_cut", '\t', case_statistics["loss_cut"]["count"], '\t', case_statistics["loss_cut"]["rate"] / case_statistics["loss_cut"]["count"], '\t'
              , case_statistics["loss_cut"]["date"] / case_statistics["loss_cut"]["count"])
    print("open", '\t', case_statistics["open"]["count"], '\t',case_statistics["open"]["rate"] / case_statistics["open"]["count"], '\t'
          , case_statistics["open"]["date"] / case_statistics["open"]["count"])
    print("total", '\t', case_statistics["normal"]["count"] + case_statistics["loss_cut"]["count"], '\t'
          , (case_statistics["normal"]["rate"] + case_statistics["loss_cut"]["rate"]) / (case_statistics["normal"]["count"] + case_statistics["loss_cut"]["count"]))


    if 0:
        plt.style.use("ggplot")

        d1 = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.1, 0.1, 0.2, 0.3, 0.4, 0.1, 0.2, 0.3, 0.4, 0.3, 0.4, 0.3, 0.4, 0.4]
        d1_np = np.array(d1)

        # Estimating the pdf and plotting
        KDEpdf = gaussian_kde(d1_np)
        x = np.linspace(-1.5, 1.5, 1500)

        plt.plot(x, KDEpdf(x), 'r', label="KDE estimation", color="blue")
        plt.hist(d1_np, normed=1, color="cyan", alpha=.8)
        # plt.plot(x, norm.pdf(x, 0, 0.1), label="parametric distribution", color="red")
        plt.legend()
        plt.title("Returns: parametric and estimated pdf")
        plt.show()


    db.disconnect()
