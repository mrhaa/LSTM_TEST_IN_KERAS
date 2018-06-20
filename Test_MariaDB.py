#_*_ coding: utf-8 _*_

import mysql.connector
from mysql.connector import errorcode
import pandas as pd

# DB 접속 정보를 dict type으로 준비한다.
config = {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "WrapDB_1",
    "user": "root",
    "password": "maria"

}


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
        print (sql)
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
        sql = "INSERT INTO value (date, item_cd, value) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE value=%s"
        sql_arg = (date, item_cd, value, value)

        # 수행
        self.cursor.execute(sql, sql_arg)

        # DB 반영
        self.conn.commit();

        return 1

    def insert_quantiwise_value(self, item_cd, date, value, type):
        # '주식_시가','주식_종가','주식_거래량','주식_시가총액'
        if type == '주식_시가':
            sql = "INSERT INTO value (date, item_cd, open) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE open=%s"
        elif type == '주식_종가':
            sql = "INSERT INTO value (date, item_cd, close) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE close=%s"
        elif type == '주식_거래량':
            sql = "INSERT INTO value (date, item_cd, volume) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE volume=%s"
        elif type == '주식_시가총액':
            sql = "INSERT INTO value (date, item_cd, market_capitalization) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE market_capitalization=%s"
        elif type == '주식_기관순매수':
            sql = "INSERT INTO value (date, item_cd, company_net_buy) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE company_net_buy=%s"
        elif type == '주식_외인순매수':
            sql = "INSERT INTO value (date, item_cd, foreigner_net_buy) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE foreigner_net_buy=%s"
        #print (sql)
        sql_arg = (date, item_cd, value, value)

        # 수행
        self.cursor.execute(sql, sql_arg)

        # DB 반영
        self.conn.commit();

        return 1
