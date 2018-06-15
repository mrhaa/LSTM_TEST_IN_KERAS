#_*_ coding: utf-8 _*_

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime
from decimal import Decimal, DecimalException
from scipy.stats.kde import gaussian_kde
from scipy.stats import norm
from Test_MariaDB import WrapDB

if __name__ == '__main__':

    # Wrap운용팀 DB Connect
    db = WrapDB()
    db.connet(host="127.0.0.1", port=3306, database="WrapDB_2", user="root", password="maria")
    data_infos = db.get_data_info()

    data = db.get_quantiwise_datas(data_infos[0])
    data.columns = ["아이템코드", "아이템명", "날짜", "시가", "종가", "거래량", "시가총액"]

    pivoted_datas = {}
    pivoted_datas["시가"] = data.pivot(index="날짜", columns="아이템명", values="시가")
    pivoted_datas["종가"] = data.pivot(index="날짜", columns="아이템명", values="종가")
    pivoted_datas["거래량"] = data.pivot(index="날짜", columns="아이템명", values="거래량")
    pivoted_datas["시가총액"] = data.pivot(index="날짜", columns="아이템명", values="시가총액")

    mean_window = 20
    pivoted_datas["평균_거래량"] = pd.rolling_mean(pivoted_datas["거래량"], mean_window)

    event_dates_dict = {}
    volume_surpise_multiple = 2.0
    for column_nm in pivoted_datas["거래량"].columns:
        event_dates_dict[column_nm] = {}
        status = False # False: No Position, True: Position
        buy_price = 0.0
        sell_price = 0.0
        buy_day = "9999-99-99"
        sell_day = "9999-99-99"
        for idx, row_nm in enumerate(pivoted_datas["거래량"].index):
            try:
                # 데이터 존재 여부 및 시가총액 필터링
                if pivoted_datas["거래량"][column_nm][idx] is not None and pivoted_datas["시가총액"][column_nm][idx] > 500000000000:

                    # open 상태로 완료된 시뮬레이션
                    if row_nm == pivoted_datas["거래량"].index[-1] and status == True:
                        print("open", '\t', column_nm, '\t', buy_day, '\t', float(pivoted_datas["종가"][column_nm][idx]))
                        break

                    # 과거 평균 거래량 보다 비상적으로 많은 거래가 발생하는 경우
                    if pivoted_datas["거래량"][column_nm][idx] > volume_surpise_multiple * pivoted_datas["평균_거래량"][column_nm][idx-1]:
                    
                        # 거래정지 해지에 의한 거래량 증가 필터링
                        trade_stop_happened = False
                        for loop in range(1, mean_window+1):
                            if pivoted_datas["거래량"][column_nm][idx - loop] == 0:
                                trade_stop_happened = True
                                break

                        # event noise 판단 기간: 5영업일
                        if trade_stop_happened == False and (len(event_dates_dict[column_nm]) == 0 or idx > list(event_dates_dict[column_nm].keys())[-1] + 5):
                            event_dates_dict[column_nm][idx] = row_nm

                            # 역사적 저점 판단: 50영업일
                            a = float(pivoted_datas["종가"][column_nm][idx]) - float(pivoted_datas["종가"][column_nm][max(idx - 50, 0):idx].min())
                            b = float(pivoted_datas["종가"][column_nm][max(idx - 50, 0):idx].max()) - float(pivoted_datas["종가"][column_nm][max(idx - 50, 0):idx].min())
                            if status == False and a / b >= 0.5:
                                status = True
                                buy_price = float(pivoted_datas["시가"][column_nm][idx+1])
                                buy_day = row_nm

                            elif status == True:
                                status = False
                                sell_price = float(pivoted_datas["시가"][column_nm][idx+1])
                                sell_day = row_nm
                                print("normal", '\t', column_nm, '\t', buy_day, '\t', sell_day, '\t', buy_price, '\t', sell_price, '\t', sell_price / buy_price - 1, '\t', (datetime.strptime(sell_day, '%Y-%m-%d').date() - datetime.strptime(buy_day, '%Y-%m-%d').date()).days)

                    # loss cut: -10%
                    elif status == True and float(pivoted_datas["종가"][column_nm][row_nm]) / buy_price - 1 <= -0.1:
                        status = False
                        sell_price = float(pivoted_datas["시가"][column_nm][idx+1])
                        sell_day = row_nm
                        print("loss cut", '\t', column_nm, '\t', buy_day, '\t', sell_day, '\t', buy_price, '\t', sell_price, '\t',sell_price / buy_price - 1, '\t', (datetime.strptime(sell_day, '%Y-%m-%d').date() - datetime.strptime(buy_day,'%Y-%m-%d').date()).days)


            except IndexError:
                pass
            except (ValueError, DecimalException):
                pass



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


