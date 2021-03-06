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
import Wrap_Util

print_log = False
print_analysis_log = True
use_data_pickle = False

if __name__ == '__main__':

    mean_window = 20
    element_list = ["시가", "종가", "고가", "저가", "거래량", "시가총액", "기관_순매수", "외인_순매수", "평균_거래량", "변동성_거래량", "정규화_거래량", "평균_기관_순매수", "평균_외인_순매수"]

    pivoted_datas = {}
    if use_data_pickle == False:

        # Wrap운용팀 DB Connect
        db = WrapDB()
        db.connet(host="127.0.0.1", port=3306, database="WrapDB_2", user="root", password="ryumaria")
        data_infos = db.get_data_info()

        data = db.get_quantiwise_datas(data_infos[0])
        data.columns = ["아이템코드", "아이템명", "날짜", "시가", "종가", "고가", "저가", "거래량", "시가총액", "기관_순매수", "외인_순매수"]

        pivoted_datas["시가"] = data.pivot(index="날짜", columns="아이템명", values="시가")
        pivoted_datas["종가"] = data.pivot(index="날짜", columns="아이템명", values="종가")
        pivoted_datas["고가"] = data.pivot(index="날짜", columns="아이템명", values="고가")
        pivoted_datas["저가"] = data.pivot(index="날짜", columns="아이템명", values="저가")
        pivoted_datas["거래량"] = data.pivot(index="날짜", columns="아이템명", values="거래량")
        pivoted_datas["시가총액"] = data.pivot(index="날짜", columns="아이템명", values="시가총액")
        pivoted_datas["기관_순매수"] = data.pivot(index="날짜", columns="아이템명", values="기관_순매수")
        pivoted_datas["외인_순매수"] = data.pivot(index="날짜", columns="아이템명", values="외인_순매수")

        pivoted_datas["평균_거래량"] = pivoted_datas["거래량"].rolling(mean_window).mean()
        pivoted_datas["변동성_거래량"] = pivoted_datas["거래량"].rolling(mean_window).std()
        pivoted_datas["정규화_거래량"] = pivoted_datas["거래량"]
        for idx, row in enumerate(pivoted_datas["거래량"].index):
            for column in pivoted_datas["거래량"].columns:
                if idx < mean_window or pivoted_datas["거래량"][column][row] == 0.0 or math.isnan(pivoted_datas["거래량"][column][row]) == True:
                    pivoted_datas["정규화_거래량"][column][row] = 0.0
                else:
                    pivoted_datas["정규화_거래량"][column][row] = (float(pivoted_datas["거래량"][column][row]) - float(pivoted_datas["변동성_거래량"][column][row])) / float(pivoted_datas["정규화_거래량"][column][row])

        pivoted_datas["평균_기관_순매수"] = pivoted_datas["기관_순매수"].rolling(mean_window).mean()
        pivoted_datas["평균_외인_순매수"] = pivoted_datas["외인_순매수"].rolling(mean_window).mean()

        # pickle 파일 저장
        for nm in element_list:
            Wrap_Util.SavePickleFile(file='.\\momentum_%s.pickle' % (nm),obj=pivoted_datas[nm])

        Wrap_Util.SaveExcelFiles(file='.\\momentum_pickle_data.xlsx'
            ,obj_dict={"정규화_거래량": pivoted_datas["정규화_거래량"], "평균_기관_순매수": pivoted_datas["평균_기관_순매수"], "평균_외인_순매수": pivoted_datas["평균_외인_순매수"]})

    else:
        # pickle 파일 로드
        for nm in element_list:
            pivoted_datas[nm] = Wrap_Util.ReadPickleFile(file='.\\momentum_%s.pickle' % (nm))


    # 통계 분석 변수
    case_statistics = {"normal": {"count": 0, "rate": 0.0, "date": 0.0}
        , "normal_gain": {"count": 0, "rate": 0.0, "date": 0.0}
        , "normal_loss": {"count": 0, "rate": 0.0, "date": 0.0}
        , "loss_cut": {"count": 0, "rate": 0.0, "date": 0.0}
        ,"open": {"count": 0, "rate": 0.0, "date": 0.0}}

    # 글로벌 셋팅 파라미터
    volume_surpise_multiple = 10.0
    volume_surpise_threshold = 0.8
    use_loss_cut = False
    loss_cut_ratio = -0.30 # -30%에서 손절
    position_in_threshold = 0.0
    position_out_threshold = -5.0


    debug_df = pd.DataFrame(index=pivoted_datas["거래량"].index, columns=pivoted_datas["거래량"].columns)

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

                        continue

                    # 과거 평균 거래량 보다 비상적으로 많은 거래가 발생하는 경우
                    # 외인과 기관에서 순매수 발생
                    if status[column_nm] == False:

                        # 포지션 진입전 수익률은 0
                        debug_df[column_nm][row_nm] = 0

                        #print(column_nm, row_nm, pivoted_datas["거래량"].index[-2], pivoted_datas["거래량"][column_nm][idx], pivoted_datas["평균_거래량"][column_nm][idx-1], pivoted_datas["평균_기관_순매수"][column_nm][idx], pivoted_datas["평균_외인_순매수"][column_nm][idx])
                        #if pivoted_datas["거래량"][column_nm][idx] > volume_surpise_multiple * pivoted_datas["평균_거래량"][column_nm][idx-1] \
                        if pivoted_datas["정규화_거래량"][column_nm][idx] > volume_surpise_threshold \
                            and pivoted_datas["종가"][column_nm][idx] / pivoted_datas["시가"][column_nm][idx] > 1.0 \
                            and pivoted_datas["고가"][column_nm][idx] / pivoted_datas["종가"][column_nm][idx] > 1.01 \
                            and pivoted_datas["평균_외인_순매수"][column_nm][idx] > position_in_threshold:
                            #and pivoted_datas["평균_기관_순매수"][column_nm][idx] > position_in_threshold:
                            # and (pivoted_datas["평균_기관_순매수"][column_nm][idx] + pivoted_datas["평균_외인_순매수"][column_nm][idx] > position_in_threshold):
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

                        # 포지션 진입후 수익률 추이
                        debug_df[column_nm][row_nm] = float(pivoted_datas["종가"][column_nm][idx]) / float(pivoted_datas["종가"][column_nm][idx-1]) - 1

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
                        #if pivoted_datas["평균_기관_순매수"][column_nm][idx] < -10.0 or pivoted_datas["평균_외인_순매수"][column_nm][idx] < -10.0:
                        #if pivoted_datas["평균_기관_순매수"][column_nm][idx] + pivoted_datas["평균_외인_순매수"][column_nm][idx] < position_out_threshold:
                        if pivoted_datas["평균_외인_순매수"][column_nm][idx] < position_out_threshold:

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

                            if sell_price[column_nm] > buy_price[column_nm]:
                                case_statistics["normal_gain"]["count"] += 1
                                case_statistics["normal_gain"]["rate"] += sell_price[column_nm] / buy_price[column_nm] - 1
                                case_statistics["normal_gain"]["date"] += (datetime.strptime(sell_day[column_nm],'%Y-%m-%d').date() - datetime.strptime(buy_day[column_nm], '%Y-%m-%d').date()).days
                            else:
                                case_statistics["normal_loss"]["count"] += 1
                                case_statistics["normal_loss"]["rate"] += sell_price[column_nm] / buy_price[column_nm] - 1
                                case_statistics["normal_loss"]["date"] += (datetime.strptime(sell_day[column_nm],'%Y-%m-%d').date() - datetime.strptime(buy_day[column_nm], '%Y-%m-%d').date()).days


                                # 정상 포지션 정리 되었지만 손실 난 경우 분석
                                if print_analysis_log == True:
                                    for loss_row_nm in pivoted_datas["종가"].index[idx:]:
                                        if loss_row_nm <= pivoted_datas["거래량"].index[-2] \
                                            and pivoted_datas["종가"][column_nm][loss_row_nm] > max(buy_price[column_nm], sell_price[column_nm]):
                                            print(column_nm, "\t", buy_day[column_nm], "\t", buy_price[column_nm], "\t"
                                                  , sell_day[column_nm], "\t", sell_price[column_nm], "\t"
                                                  , loss_row_nm, "\t", pivoted_datas["종가"][column_nm][loss_row_nm])
                                            break

            except IndexError:
                pass
            except (ValueError, DecimalException):
                pass

    # print log
    if case_statistics["normal"]["count"] > 0:
        print("normal", '\t', case_statistics["normal"]["count"], '\t', case_statistics["normal"]["rate"] / case_statistics["normal"]["count"], '\t'
              , case_statistics["normal"]["date"] / case_statistics["normal"]["count"])
    if case_statistics["normal_gain"]["count"] > 0:
        print("normal_gain", '\t', case_statistics["normal_gain"]["count"], '\t', case_statistics["normal_gain"]["rate"] / case_statistics["normal_gain"]["count"], '\t'
              , case_statistics["normal_gain"]["date"] / case_statistics["normal_gain"]["count"])
    if case_statistics["normal_loss"]["count"] > 0:
        print("normal_loss", '\t', case_statistics["normal_loss"]["count"], '\t', case_statistics["normal_loss"]["rate"] / case_statistics["normal_loss"]["count"], '\t'
              , case_statistics["normal_loss"]["date"] / case_statistics["normal_loss"]["count"])
    if case_statistics["loss_cut"]["count"] > 0:
        print("loss_cut", '\t', case_statistics["loss_cut"]["count"], '\t', case_statistics["loss_cut"]["rate"] / case_statistics["loss_cut"]["count"], '\t'
              , case_statistics["loss_cut"]["date"] / case_statistics["loss_cut"]["count"])
    if case_statistics["open"]["count"] > 0:
        print("open", '\t', case_statistics["open"]["count"], '\t',case_statistics["open"]["rate"] / case_statistics["open"]["count"], '\t'
              , case_statistics["open"]["date"] / case_statistics["open"]["count"])
    print("total", '\t', case_statistics["normal"]["count"] + case_statistics["loss_cut"]["count"], '\t'
              , (case_statistics["normal"]["rate"] + case_statistics["loss_cut"]["rate"]) / (case_statistics["normal"]["count"] + case_statistics["loss_cut"]["count"]))

    Wrap_Util.SaveExcelFiles(file='.\\momentum_algorithm_log.xlsx', obj_dict={'debug_df': debug_df})


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


    if use_data_pickle == False:
        db.disconnect()
