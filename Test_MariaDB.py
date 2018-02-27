#_*_ coding: utf-8 _*_

import mysql.connector
from mysql.connector import errorcode
import pandas as pd

# DB 접속 정보를 dict type으로 준비한다.
config = {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "WrapDB",
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

        # 내역 출력
        data = []
        for r in self.cursor.fetchall():
            data.append([r[0], r[1], r[2]])
        #print(data)

        dfData = pd.DataFrame(data)

        return dfData

    def insert(self):
        sql = "INSERT INTO member (name, addr) VALUES (%s, %s) ON DUPLICATE KEY UPDATE name=%s, addr=%s"
        sql_arg = (u"김영일", u"염창동", u"박효근", u"신길동")

        # 수행
        self.cursor.execute(sql, sql_arg)

        # DB 반영
        self.conn.commit();

        return True
