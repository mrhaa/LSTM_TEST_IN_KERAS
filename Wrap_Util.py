#_*_ coding: utf-8 _*_

import pickle
import xlsxwriter
import openpyxl
import pandas as pd


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