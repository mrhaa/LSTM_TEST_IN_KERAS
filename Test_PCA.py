#_*_ coding: utf-8 _*_


import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import Test_Figure

import mysql.connector
from mysql.connector import errorcode

# DB 접속 정보를 dict type으로 준비한다.
config = {
    "user": "root",
    "password": "maria",
    "host": "127.0.0.1",
    "database": "WrapDB",
    "port": 3306
}

features = ["MSCI World", "MSCI EM"]
target = ["MSCI ACWI"]

try:
    # DB 연결객체
    # config dict type 매칭
    conn = mysql.connector.connect(**config)
    print("DB 연결")

    # DB 작업객체
    cursor = conn.cursor()

    sql = "SELECT a.date, a.value, b.value, c.value" \
          "  FROM item a_, item b_, item c_, value a, value b, value c" \
          " WHERE a_.nm = '%s'" \
          "   AND b_.nm = '%s'" \
          "   AND c_.nm = '%s'" \
          "   AND a_.cd = a.item_cd" \
          "   AND b_.cd = b.item_cd" \
          "   AND c_.cd = c.item_cd" \
          "   AND a.date = b.date" \
          "   AND a.date = c.date" \
          "   AND b.date = c.date" \
          " ORDER BY a.date" % (features[0], features[1], target[0])
    sql_arg = None
    print (sql)
    # 수행
    cursor.execute(sql, sql_arg)

    # 내역 출력
    data = []
    for r in cursor.fetchall():
        data.append([r[0], r[1], r[2], r[3]])
    #print(data)

    df = pd.DataFrame(data = data, columns = ['date'] + features + target)
    print(df)

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("아이디 혹은 비밀번호 오류")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("DB 오류")
    else:
        print("기타 오류")
else:
    print("정상 수행")
finally:
    # cursor 닫기
    cursor.close()
    # 연결 객체 닫기
    conn.close()

# Separating out the features
x = df.loc[:, features].values
# Separating out the target
y = df.loc[:, target].values
for idx, value in enumerate(y):
    print (idx, value)
    y[idx] = y[idx] / y[idx]
    print(idx, y[idx])
# Standardizing the features
x = StandardScaler().fit_transform(x)

oriDf = pd.DataFrame(data = x, columns = features)
oriFinalDf = pd.concat([oriDf, df[target]], axis = 1)

pca = PCA(n_components=2)
#principalComponents = pca.fit_transform(x)
pca.fit(x)
principalComponents = pca.transform(x)
principalDf = pd.DataFrame(data = principalComponents, columns = ['principal component 1', 'principal component 2'])

finalDf = pd.concat([principalDf, df[['target']]], axis = 1)

classes = ['up', 'down']
classes_color = ['r', 'b']
Test_Figure.Figure_2D(oriFinalDf, 'Original', ['1', '2'], 'target', classes, classes_color)
Test_Figure.Figure_2D(finalDf, '2 Component PCA', ['principal component 1', 'principal component 2'], 'target', classes, classes_color)

print('End')

