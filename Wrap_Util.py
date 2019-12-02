#_*_ coding: utf-8 _*_

import pickle
import xlsxwriter
import openpyxl
import pandas as pd
import numpy as np


def SavePickleFile(file='test.pickle', obj=None):

    f = open(file, 'wb')
    pickle.dump(obj, f)
    f.close()

    return True


def ReadPickleFile(file='test.pickle'):

    f = open(file, 'rb')
    obj = pickle.load(f)
    f.close()

    return obj


def SaveExcelFiles(file='test.xlsx', obj_dict=None):

    # 만약 해당 파일이 존재하지 않는 경우 생성
    workbook = xlsxwriter.Workbook(file)
    workbook.close()

    # Create a Pandas Excel writer using Openpyxl as the engine.
    writer = pd.ExcelWriter(file, engine='openpyxl')
    # 주의: 파일이 암호화 걸리면 workbook load시 에러 발생
    writer.book = openpyxl.load_workbook(file)
    # Pandas의 DataFrame 클래스를 그대로 이용해서 엑셀 생성 가능
    for obj_nm in obj_dict:
        obj_dict[obj_nm].to_excel(writer, sheet_name=obj_nm)
    writer.save()

    return True

def SaveCSVFiles(file='test.csv', obj=None):

    obj.to_csv(file, mode='w', header=True)

    return True


def GetSlope(X, Y):
    Ym1 = list()
    Xmn = list()
    # y 데이터셋
    #for theft in Y.values:
    for theft in Y:
        Ym1.append([float(theft)])

    # X 데이터셋
    #for all in X.values:
    for all in X:
        Xmn.append([1.0, float(all)])

    Y = np.array(Ym1, dtype=np.float32)
    X = np.array(Xmn, dtype=np.float32)

    use_lstsq_func = True
    beta_hat = None
    try:

        if use_lstsq_func == True:
            beta_hat = np.linalg.lstsq(X, Y)
        else:
            XtX = X.T.dot(X)
            I_XtX = np.linalg.inv(XtX)
            beta_hat = I_XtX.dot(X.T).dot(Y)

            beta_hat = np.dot(np.dot(np.linalg.inv(np.dot(X.T, X)), X.T), Y)
    except np.linalg.LinAlgError as err:
        print(err)

    if use_lstsq_func == True:
        angle = beta_hat[0][1][0]
    else:
        angle = beta_hat[1][0]
        bias = beta_hat[0][0]

    return angle