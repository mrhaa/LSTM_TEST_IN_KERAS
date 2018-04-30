#_*_ coding: utf-8 _*_

import win32com.client
from Test_MariaDB import WrapDB
import pandas as pd
import xlsxwriter
import openpyxl
from xlutils.copy import copy as xl_copy


item_dic = {"S&P500":1
,"STOXX50":2
,"FTSE100":3
,"TOPIX":4
,"Nikkei225":5
,"상해종합":6
,"홍콩H":7
,"KOSPI":8
,"SENSEX":9
,"IBOVESPA":10
,"RTSI":11
,"S&P/TSX":12
,"MSCI ACWI":13
,"MSCI World":14
,"MSCI EM":15
,"MSCI ACWI(Local)":16
,"MSCI World(Local)":17
,"MSCI EM(Local)":18
,"달러 인덱스":19
,"Bloomberg dollar spot Index":20
,"JPM 이머징통화지수":21
,"OITP 인덱스":22
,"USD/EUR":23
,"JPY/USD":24
,"CNY/USD":25
,"CNH/USD":26
,"KRW/USD":27
,"KRW/JPY":28
,"USD/AUD":29
,"CHF/AUD":30
,"미국 REER":31
,"유로존 REER":32
,"일본 REER":33
,"중국 REER":34
,"인도 REER":35
,"한국 REER":36
,"브라질 REER":37
,"FED자산":38
,"ECB 자산":39
,"BOJ자산":40
,"미국 M3 YoY":41
,"미국 M2 YoY":42
,"미국 M1 YoY":43
,"유럽 M3 YoY":44
,"유럽 M2 YoY":45
,"유럽 M1 YoY":46
,"한국 M3 YoY":47
,"한국 M2 YoY":48
,"한국 M1 YoY":49
,"중국 M1 YoY":50
,"KOSPI외국인수매수(대금)":51
,"KOSPI외국인수매수(수량)":52
,"JPM 글로벌 금리":53
,"EMBI+스프레드":54
,"미국 연방기금 목표금리":55
,"미 연방금리선물":56
,"FED 5Y FWD BEI":57
,"미국장단기스프레드":58
,"달러 리보 3M":59
,"미국 국채 2년물 금리":60
,"미국 국채 10년물 금리":61
,"미국 국채 30년물 금리":62
,"US Libor - OIS Spread":63
,"TIPS 10년물 금리":64
,"CS 하이일드 인덱스":65
,"미국 하이일드 스프레드":66
,"미국 신용스프레드":67
,"미국회사채 Baa 10년 스프레드(역측)":68
,"유로 리보 3M":69
,"독일 국채 2년물 금리":70
,"독일 10년 국채금리":71
,"SHIBOR 금리":72
,"일본 2년 국채금리":73
,"중국 기준금리":74
,"중국 국채 2년물 금리":75
,"중국 국채 10년물 금리":76
,"중국 예금금리":77
,"중국 대출금리(1년 기준)":78
,"중국 1년물 예금":79
,"한국 기준금리":80
,"인도 기준금리":81
,"CPI 주거비":82
,"고용비용 지수":83
,"ISM 고용(계조)":84
,"ISM 비제조업지수(계조)":85
,"ISM 신규수주(계조)":86
,"ISM 재고(계조)":87
,"ISM 제조업지수(계조)":88
,"Margin Debt":89
,"Margin Debt YoY":90
,"MBA 주택융자 신청지수":91
,"MS 금융환경지수":92
,"Net Credit Balance":93
,"S&P US REITS 지수":94
,"St Louis 연방 금융환경지수":95
,"St Louis 연방 스트레스지수":96
,"MARKIT 미국 서비스업 PMI":97
,"MARKIT 미국 제조업 PMI":98
,"MARKT 미국 종합 PMI":99
,"미국 금융환경지수(Bloomberg)":100
,"미국 금융환경지수(Chicago Fed)":101
,"ZEW 미국경기지수":102
,"개인근로소득(원지수)":103
,"개인소비지출 YoY":104
,"경기동행지수 YoY":105
,"경기동행지수(원지수)":106
,"나스닥 은행주지수":107
,"뉴욕 엠파이어 스테이트지수":108
,"미국 10년 투기적포지션":109
,"미국 Core CPI":110
,"미국 CORE PCE":111
,"미국 CPI YoY":112
,"미국 ESI":113
,"미국 FED 노동환경지수":114
,"미국 NAHB 주택시장지수":115
,"미국 개인근로소득 YoY":116
,"미국 경기기대지수 원지표":117
,"미국 경기선행지수 YOY":118
,"미국 경상수지":119
,"미국 내구재 신규수주 YoY":120
,"미국 내구재주문":121
,"미국 명목성장률":122
,"미국 무역수지":123
,"미국 민간소비 YoY":124
,"미국 비농가취업자수(단위: 천)":125
,"미국 산업생산 YoY":126
,"미국 생산자 물가지수(식품/에너지 제외) YoY":127
,"미국 설비가동률":128
,"미국 소매판매(자동차 제외)":129
,"미국 소비자기대지수":130
,"미국 수입 가격 지수 YoY":131
,"미국 수출 YoY":132
,"미국 시간당 평균임금 MoM":133
,"미국 시간당 평균임금 YoY":134
,"미국 실업률(%)":135
,"미국 제조업체 신규수주 합계 MoM(계조)":136
,"미국 총 설비가동률 대비 설비가동률(%) 계절조정":137
,"미국 파산신고":138
,"미국 산업생산":139
,"미시간 소비자기대지수":140
,"민간주택허가":141
,"시카고 제조업 PMI":142
,"아틀란타 연준 임금상승률":143
,"자본재수주(국방, 항공제외) YoY(3개월 이평)":144
,"자본재수주(국방,항공제외)(YoY)":145
,"제조업주당평균근로시간":146
,"존슨 레드북 소매판매지수":147
,"주간신규실업수당청구건수":148
,"미국 명목 PCE":149
,"미국 수입 YoY":150
,"Citi G10 서프라이즈 지수":151
,"Citi Macro Risk Index":152
,"CPB 선진국 산업생산":153
,"CPB 선진국 수출":154
,"IMF 세계 교역 증가율 YoY":155
,"JPM 글로벌 서비스업 PMI":156
,"JPM 글로벌 제조업 PMI":157
,"JPM 글로벌 종합 PMI":158
,"OECD ASIA 5 경기선행지수":159
,"OECD G7 경기선행지수":160
,"OECD 경기선행지수":161
,"글로벌 실질 GDP":162
,"발틱 해운임지수":163
,"선진국 CPI":164
,"선진국 ESI":165
,"선진국 수입물량":166
,"선진국 수입물량(%)":167
,"신흥국 CPI":168
,"신흥국 ESI":169
,"신흥국 수입물량 YoY":170
,"OECD 소비자물가":171
,"WTI 유가":172
,"Brent유가":173
,"두바이유가":174
,"CPB 세계교역":175
,"CPB 세계생산":176
,"CPB 이머징 산업생산":177
,"CPB 이머징 수출":178
,"CRB 금속지수":179
,"CRB 상품선물지수":180
,"ISM 종합지수":181
,"LME Index":182
,"MSCI AC ASIA(일본 제외, LOCAL)":183
,"MSCI CHINA":184
,"MSCI 선진국부동산지수 ":185
,"MSCI 선진유럽":186
,"MSCI 신흥국 통화지수":187
,"OECD+6NME 경기선행지수":188
,"VIX 지수":189
,"구리":190
,"금":191
,"납":192
,"돼지":193
,"미국 BEI 5년 ":194
,"미국 PD 일평균 국채거래량":195
,"미국 S&P 케이스쉴러 주택가격":196
,"미국 나스닥지수":197
,"미국 다우 운송지수":198
,"유럽 은행주지수":199
,"항셍 A/H 프리미엄":200
,"S&P500 P/B":201
,"STOXX50 P/B":202
,"FTSE100 P/B":203
,"TOPIX P/B":204
,"Nikkei225 P/B":205
,"상해종합 P/B":206
,"홍콩H P/B":207
,"KOSPI P/B":208
,"SENSEX P/B":209
,"IBOVESPA P/B":210
,"RTSI P/B":211
,"S&P/TSX P/B":212
,"S&P500 P/E":213
,"STOXX50 P/E":214
,"FTSE100 P/E":215
,"TOPIX P/E":216
,"Nikkei225 P/E":217
,"상해종합 P/E":218
,"홍콩H P/E":219
,"KOSPI P/E":220
,"SENSEX P/E":221
,"IBOVESPA P/E":222
,"RTSI P/E":223
,"S&P/TSX P/E":224
,"S&P500 EPS":225
,"STOXX50 EPS":226
,"FTSE100 EPS":227
,"TOPIX EPS":228
,"Nikkei225 EPS":229
,"상해종합 EPS":230
,"홍콩H EPS":231
,"KOSPI EPS":232
,"SENSEX EPS":233
,"IBOVESPA EPS":234
,"RTSI EPS":235
,"S&P/TSX EPS":236
,"BoK BSI":237
,"KOSPI & KOSDAQ 외국인 순매수":238
,"KOSPI Credit Balance":239
,"교역조건대용치":240
,"도소매판매액지수":241
,"전경련 BSI":242
,"한국 CDS 5Y":243
,"한국 Core CPI":244
,"한국 CPI":245
,"한국 ESI":246
,"한국 GDP 성장률":247
,"한국 GDP 지출(디플레이터) 건설투자 YoY":248
,"한국 PPI":249
,"한국 가계부채":250
,"한국 가계부채 YoY":251
,"한국 가계수입전망 CSI":252
,"한국 건설수주 YoY":253
,"한국 경기선행지수(원지수)":254
,"한국 경기선행지수(YOY)":255
,"한국 경기선행지수 순환변동치":256
,"한국 경기동행지수":257
,"한국 경기후행지수":258
,"한국 경기선행지수 순환변동치 차이":259
,"한국 국내 기계수주 YoY":260
,"한국 금리수준 전망 CSI":261
,"한국 내수출하지수":262
,"한국 대 중국 수출증가율":263
,"한국 무역수지(USD백만)":264
,"한국 본원소득수지(USD백만)":265
,"한국 산업생산 YoY":266
,"한국 상품수지(USD백만)":267
,"한국 서비스수지(USD백만)":268
,"한국 서비스업 생산지수(계조)":269
,"한국 서비스업 활동지수 YoY":270
,"한국 설비투자(계조) YoY":271
,"한국 설비투자추계 YoY":272
,"한국 소매판매":273
,"한국 소매판매액 YoY":274
,"한국 소비자심리지수":275
,"한국 수입 YoY":276
,"한국 수출 YoY":277
,"한국 수출물가지수 YoY":278
,"한국 수출출하지수":279
,"한국 순상품교역조건":280
,"한국 실질 GDP":281
,"한국 이전소득수지(USD백만)":282
,"한국 자본수지":283
,"한국 재고순환지표":284
,"한국 전산업생산":285
,"한국 제조업 가동률(계조)":286
,"한국 제조업 내수출하지수(원지수)":287
,"한국 제조업 생산능력지수 YoY":288
,"한국 제조업 생산지수 YoY":289
,"한국 제조업가동률":290
,"한국 취업률":291
,"한국 평균 가동률":292
,"한국 평균 가동률 YoY":293
,"한국내수용소비재출하":294
,"한국소매판매":295
,"한국인 방문객수":296
,"70개 주요도시 신규주택 가격 YoY":297
,"China Business Cycle Singal":298
,"중국 Caixin  제조업 PMI":299
,"중국 Caixin 비제조업 PMI":300
,"중국 CPI YoY":301
,"중국 ESI":302
,"중국 NBS 제조업 PMI":303
,"중국 PPI":304
,"중국 경기기대지수":305
,"중국 경기선행지수":306
,"중국 경상수지대금":307
,"중국 고정자산투자누계 YoY":308
,"중국 금융기관 대출증가율":309
,"중국 단기외채":310
,"중국 본원소득수지":311
,"중국 부동산경기지수":312
,"중국 블룸버그 월간 GDP 서베이":313
,"중국 비금융기업 해외채무 잔액":314
,"중국 수출 YoY":315
,"중국 예금지급준비율":316
,"중국 위안화 월간 신규대출":317
,"중국 이전소득수지":318
,"중국 자동차 판매 YoY":319
,"중국 전력생산증가율 YoY":320
,"중국 철강제품 출하":321
,"중국 핵심소비자 물가 YoY":322
,"중국CPI-PPI":323
,"중국산업생산 증가율":324
,"중국 NBS 서비스업 PMI":325
,"중국 수입 YoY":326
,"ZEW 일본 경기지수":327
,"일본 Core CPI":328
,"일본 CPI":329
,"일본 ESI":330
,"일본 가계저축률":331
,"일본 경기선행지수":332
,"일본 단칸 경기현황(전체 기업 전체 업종)":333
,"일본 산업생산 YoY":334
,"일본 소매판매 YoY":335
,"일본 소비자신뢰지수":336
,"일본 전년대비 시간당 평균임금":337
,"EU27소비자신뢰지수":338
,"EU27소비자신뢰지수: 일반경제상황과거1년":339
,"EU27소비자신뢰지수:금융상황과거1년":340
,"EU27소비자신뢰지수:주요구매현재":341
,"SENTIX 경제전망 6개월 선행주요지수":342
,"SENTIX 투자자신뢰지수":343
,"ZEW 독일경기지수":344
,"ZEW 유로권 경기기대지수":345
,"ZEW 유로존 경기지수 NET CHANGE":346
,"독일 IFO 기업전망":347
,"독일 IFO 기업평가":348
,"독일 IFO 기업환경":349
,"독일 ZEW경기판단지수":350
,"독일 산업생산 YoY":351
,"독일 제조업 PMI":352
,"독일 제조업주문 증가율":353
,"유럽 CPI":354
,"유럽 GDP 성장률":355
,"유럽 산업생산(건설 제외) YoY":356
,"유럽 소매판매 YoY":357
,"유럽 소비자심리":358
,"유럽 수입 YoY":359
,"유럽 수출 YoY":360
,"유럽 정책불확실성지수":361
,"유럽기대인플레이션(Inflation swap forward 5Y5Y)":362
,"유로존 Core CPI":363
,"유로존 CPI":364
,"유로존 Credit Impulse":365
,"유로존 ESI":366
,"유로존 서비스업 Markit PMI":367
,"유로존 설비가동률 YoY":368
,"유로존 소비자신뢰지수":369
,"유로존 신규수주":370
,"유로존 실업률(%)":371
,"유로존 제조업 Markit PMI":372
,"유로존 종합 Markit PMI":373
,"러시아 수출 YoY":374
,"러시아 원유 수출(천톤)":375
,"러시아 원유 수출(USD백만) YTD":376
,"미국 OECD 경기선행지수":377
,"유럽 OECD 경기선행지수":378
,"일본 OECD 경기선행지수":379
,"중국 OECD 경기선행지수":380
,"한국 OECD 경기선행지수":381
,"브라질 OECD 경기선행지수":382
,"러시아 OECD 경기선행지수":383
,"인도 OECD 경기선행지수":384
,"MOVE 지수":385
,"TED Spread":386
,"BBA-10Y Spread":387
}

group_dic = {"경제_한국":"10005"
,"경제_러시아":"10010"
,"경제_글로벌":"10011"
,"가격지표":"10012"
,"밸류에이션_PB":"10013"
,"밸류에이션_PE":"10014"
,"경제_미국":"10009"
,"주가지수":"10001"
,"경제_유로":"10008"
,"환율":"10002"
,"경제_일본":"10007"
,"금리":"10004"
,"경제_중국":"10006"
,"유동성":"10003"
,"기업이익_EPS":"10015"}


import timeit
import copy
from openpyxl import load_workbook


start_time = timeit.default_timer()
wb = load_workbook(filename='통합지표_류상진_.xlsx', read_only=False, data_only=False)
ws = wb['데이터_작업']
end_time = timeit.default_timer()
print ("엑셀 read: ", "\t", str(end_time - start_time))
start_time = end_time

all_columns = ws.columns


# Wrap운용팀 DB Connect
db = WrapDB()
db.connet(host="127.0.0.1", port=3306, database="WrapDB_1", user="root", password="maria")

# 엑셀 load 된 데이터 처리 
dates = None
values = None
for idx, column in enumerate(all_columns):

    # 2개 column이 pair로 되어 있다.

    # 짝수 컬럼은 date
    if idx % 2 == 0:
        dates = copy.copy(column)

    # 홀수 컬럼은 value
    else:
        values = copy.copy(column)

        # insert 되는 item 갯수 확인
        count = 0

        # data pair가 완성되면 처음 4 row는 meta data임
        group_cd = None
        item_cd = None
        item_name = None
        ticker = None
        for idx in range(len(dates)):

            # 그룹명
            if idx == 0:
                continue
                # 재사용을 위해서는 엑셀에 추가 작업 필요
                group_cd = group_dic[values[idx].value]
                #print (values[idx].value, group_cd)

            # 상품명
            elif idx == 1:
                item_cd = item_dic[values[idx].value]
                item_nm = values[idx].value
                #print (values[idx].value, item_cd)

            # 블룸버그 티커
            elif idx == 2:
                ticker = values[idx].value
                #print (ticker)
            
            # 블룸버그 필드
            elif idx == 3:
                pass

            # idx 4부터는 실제 데이터
            else:
                
                # Null 셀이면 다음 item으로 패스
                if values[idx].value == None:
                    break

                #print(str(dates[idx].value)[:10], "\t", item_cd, "\t", values[idx].value, "\t", group_cd)
                s_date = str(dates[idx].value)[:10]
                f_value = values[idx].value

                count += db.insert_bloomberg_value(item_cd, s_date, f_value)
                #break
        #break

        # insert된 리스트 정보 프린트
        print (item_cd, "\t", item_nm, "\t", count)

        '''
        # 첫번째 pair 만 테스트
        if idx == 1:
            print ("aaaaaaaaaaaaaaaaaaa")
            break
        '''
end_time = timeit.default_timer()
print ("DB insert: ", "\t", str(end_time - start_time))

# Wrap운용팀 DB Disconnect
db.disconnect()
