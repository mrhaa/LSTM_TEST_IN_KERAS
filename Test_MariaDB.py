#_*_ coding: utf-8 _*_

import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import time
import datetime

# DB 접속 정보를 dict type으로 준비한다.
config = {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "WrapDB_1",
    "user": "root",
    "password": "ryumaria"

}

ts = time.time()

class WrapDB(object):

    def __init__(self):
        self.conn = None
        self.cursor = None

    def connet(self, host="127.0.0.1", port=3306, database="WrapDB", user="root", password="maria"):
        try:
            #DB연결 설정
            config["host"] = host
            config["port"] = port
            config["database"] = database
            config["user"] = user
            config["password"] = password

            # DB 연결객체
            # config dict type 매칭
            self.conn = mysql.connector.connect(**config)
            print("DB 연결")

            # DB 작업객체
            self.cursor = self.conn.cursor()

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("아이디 혹은 비밀번호 오류")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("DB 오류")
            else:
                print("기타 오류")

            # cursor 닫기
            if self.cursor:
                self.cursor.close()

            # 연결 객체 닫기
            if self.conn:
                self.conn.close()
        else:
            print("정상 수행")

    def disconnect(self):

        # cursor 닫기
        if self.cursor:
            self.cursor.close()

        # 연결 객체 닫기
        if self.conn:
            self.conn.close()

    def select(self):
        sql = "SELECT a.date, a.value, b.value" \
              "  FROM value a, value b" \
              " WHERE a.date = b.date" \
              "   AND a.item_cd = 1" \
              "   AND b.item_cd = 2" \
              " ORDER BY a.date"
        sql_arg = None
        #print(sql)
        # 수행
        self.cursor.execute(sql, sql_arg)

        data = self.cursor.fetchall()

        return pd.DataFrame(data)

    def get_data_info(self):
        sql = "SELECT a.cd, a.nm, count(*), min(date), max(date)" \
              "  FROM item as a" \
              "  LEFT JOIN value AS b" \
              "    ON a.cd = b.item_cd" \
              " WHERE a.use_yn = 1" \
              " GROUP BY a.cd, a.nm" \
              " HAVING COUNT(*) > 1"
        sql_arg = None

        # 수행
        self.cursor.execute(sql, sql_arg)

        data = self.cursor.fetchall()

        return pd.DataFrame(data)

    def get_bloomberg_datas(self, data_list, start_date = None, end_date = None):

        if start_date == None and end_date == None:
            sql = "SELECT a.cd, a.nm, b.date, b.value"\
                  "  FROM item AS a, value AS b"\
                  " WHERE a.cd = b.item_cd"\
                  "   AND a.cd in (%s)"
        else:
            sql = "SELECT a.cd, a.nm, b.date, b.value" \
                  "  FROM item AS a, value AS b" \
                  " WHERE a.cd = b.item_cd" \
                  "   AND a.cd in (%s)" \
                  "   AND b.date >= '%s'" \
                  "   AND b.date <= '%s'"

        sql_arg = None

        target_list = None
        for idx, ele in enumerate(data_list):
            if idx == 0:
                target_list = str(ele)
            else:
                target_list += ", " + str(ele)

        if start_date == None and end_date == None:
            sql = sql % (target_list)
        else:
            sql = sql % (target_list, start_date, end_date)
        #print (sql)
        # 수행
        self.cursor.execute(sql, sql_arg)

        data = self.cursor.fetchall()

        return pd.DataFrame(data)

    def get_quantiwise_datas(self, data_list, start_date = None, end_date = None):

        if start_date == None and end_date == None:
            sql = "SELECT a.cd, a.nm, b.date, b.open, b.close, b.volume, b.market_capitalization, b.company_net_buy, b.foreigner_net_buy"\
                  "  FROM item AS a, value AS b"\
                  " WHERE a.cd = b.item_cd"\
                  "   AND a.cd in (%s)"
        else:
            sql = "SELECT a.cd, a.nm, b.date, b.open, b.close, b.volume, b.market_capitalization, b.company_net_buy, b.foreigner_net_buy" \
                  "  FROM item AS a, value AS b" \
                  " WHERE a.cd = b.item_cd" \
                  "   AND a.cd in (%s)" \
                  "   AND b.date >= '%s'" \
                  "   AND b.date <= '%s'"

        sql_arg = None

        target_list = None
        for idx, ele in enumerate(data_list):
            if idx == 0:
                target_list = str(ele)
            else:
                target_list += ", " + str(ele)

        if start_date == None and end_date == None:
            sql = sql % (target_list)
        else:
            sql = sql % (target_list, start_date, end_date)
        #print (sql)
        # 수행
        self.cursor.execute(sql, sql_arg)

        data = self.cursor.fetchall()

        return pd.DataFrame(data)

    def insert(self):
        sql = "INSERT INTO member (name, addr) VALUES (%s, %s) ON DUPLICATE KEY UPDATE name=%s, addr=%s"
        sql_arg = (u"김영일", u"염창동", u"박효근", u"신길동")

        # 수행
        self.cursor.execute(sql, sql_arg)

        # DB 반영
        self.conn.commit();

        return True

    def insert_bloomberg_value(self, item_cd, date, value):

        # 최종 update 시간
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        sql = "INSERT INTO value (date, item_cd, value, create_tm, update_tm) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE value=%s, update_tm=%s"
        sql_arg = (date, item_cd, value, timestamp, timestamp, value, timestamp)

        # 수행
        self.cursor.execute(sql, sql_arg)

        # DB 반영
        self.conn.commit();

        return 1

    def insert_quantiwise_value(self, item_cd, date, value, type):

        # 최종 update 시간
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        # '주식_시가','주식_종가','주식_거래량','주식_시가총액'
        if type == '주식_시가':
            sql = "INSERT INTO value (date, item_cd, open, create_tm, update_tm)" \
                  "VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE open=%s, update_tm=%s"
        elif type == '주식_종가':
            sql = "INSERT INTO value (date, item_cd, close, create_tm, update_tm)" \
                  "VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE close=%s, update_tm=%s"
        elif type == '주식_고가':
            sql = "INSERT INTO value (date, item_cd, high, create_tm, update_tm)" \
                  "VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE high=%s, update_tm=%s"
        elif type == '주식_저가':
            sql = "INSERT INTO value (date, item_cd, low, create_tm, update_tm)" \
                  "VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE low=%s, update_tm=%s"
        elif type == '주식_거래량':
            sql = "INSERT INTO value (date, item_cd, volume, create_tm, update_tm)" \
                  "VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE volume=%s, update_tm=%s"
        elif type == '주식_시가총액':
            sql = "INSERT INTO value (date, item_cd, market_capitalization, create_tm, update_tm)" \
                  "VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE market_capitalization=%s, update_tm=%s"
        elif type == '주식_기관순매수' and value > 0.0:
            sql = "INSERT INTO value (date, item_cd, company_net_buy, create_tm, update_tm)" \
                  "VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE company_net_buy=%s, update_tm=%s"
        elif type == '주식_외인순매수' and value > 0.0:
            sql = "INSERT INTO value (date, item_cd, foreigner_net_buy, create_tm, update_tm)" \
                  "VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE foreigner_net_buy=%s, update_tm=%s"
        #print (sql)
        sql_arg = (date, item_cd, value, timestamp, timestamp, value, timestamp)

        # 수행
        self.cursor.execute(sql, sql_arg)

        # DB 반영
        self.conn.commit();

        return 1

    def get_factors_nm_cd(self):
        sql = "SELECT nm, cd" \
              "  FROM item"
        sql_arg = None

        # 수행
        self.cursor.execute(sql, sql_arg)

        data = self.cursor.fetchall()

        tmp_df = pd.DataFrame(data)
        nm_cd_map = {}
        for idx in tmp_df.index:
            nm_cd_map[tmp_df[0].values[idx]] = tmp_df[1].values[idx]

        return nm_cd_map

    def insert_folione_signal(self, table_nm, date_info, target_cd, factor_info, signal_cd, etc):

        # 최종 update 시간
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        # Factor의 갯수가 1~10개로 유동적임
        sql = "INSERT INTO %s (start_dt, end_dt, curr_dt, target_cd, factors_num, multi_factors_nm" % (table_nm)
        for idx, factor_cd in enumerate(factor_info['factors_cd']):
            sql = sql + ", factor_cd" + str(idx)
        sql = sql + ", window_size, signal_cd, model_profit, bm_profit, update_tm) "
        sql = sql + "VALUES (%s, %s, %s, %s, %s, %s"
        for idx, factor_cd in enumerate(factor_info['factors_cd']):
            sql = sql + ", %s"
        sql = sql + ", %s, %s, %s, %s, %s)"
        sql = sql +  "ON DUPLICATE KEY UPDATE signal_cd=%s, model_profit=%s, bm_profit=%s, update_tm=%s"

        sql_arg = [date_info['start_dt'], date_info['end_dt'], date_info['curr_dt'], int(target_cd), factor_info['factors_num'], factor_info['multi_factors_nm']]
        for idx, factor_cd in enumerate(factor_info['factors_cd']):
            sql_arg += [int(factor_info['factors_cd'][idx])]
        sql_arg += [etc['window_size'], signal_cd, etc['model_profit'], etc['bm_profit'], timestamp]
        sql_arg += [signal_cd, etc['model_profit'], etc['bm_profit'], timestamp]
        sql_arg = tuple(sql_arg)

        #print(sql)
        #print(sql_arg)
        # 수행
        self.cursor.execute(sql, sql_arg)

        # DB 반영
        self.conn.commit();

        return 1

    def delete_folione_signal(self, table_nm, target_cd, start_dt, end_dt, window_size):
        sql = "DELETE " \
              "FROM %s " \
              "WHERE target_cd = %s" \
              "  AND start_dt = '%s'" \
              "  AND end_dt = '%s'" \
              "  AND window_size = %s" % (table_nm, int(target_cd), start_dt, end_dt, window_size)

        # 수행
        self.cursor.execute(sql)

        # DB 반영
        self.conn.commit();

        return 1