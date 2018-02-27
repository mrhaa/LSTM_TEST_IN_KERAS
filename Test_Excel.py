#_*_ coding: utf-8 _*_

import win32com.client
from Test_MariaDB import WrapDB
import pandas as pd
import xlsxwriter
import openpyxl
from xlutils.copy import copy as xl_copy

db = WrapDB()
db.connet()

data = db.select()
#print(data)

db.disconnect()

'''
excel = win32com.client.Dispatch("Excel.Application")
excel.Visible = False
wb = excel.Workbooks.Add()
ws = wb.Worksheets("Sheet1")
ws.Cells(1, 1).Value = "hello world"
wb.SaveAs('test.xlsx')
excel.Quit()
'''

'''
# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter('test.xlsx', engine='xlsxwriter')
# Convert the dataframe to an XlsxWriter Excel object.
data.to_excel(writer, sheet_name='Sheet2')
# Close the Pandas Excel writer and output the Excel file.
writer.save()
'''
'''
workbook = xlsxwriter.Workbook('test.xlsx')
#worksheet1 = workbook.add_worksheet()
worksheet2 = workbook.add_worksheet()
workbook.close()
'''


# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter('test.xlsx', engine='openpyxl')
writer.book = openpyxl.load_workbook('test.xlsx')
data.to_excel(writer, sheet_name='sheet2')
writer.save()



'''
excel = win32com.client.Dispatch("Excel.Application")
excel.Visible = False
wb = excel.Workbooks.Open('test.xlsx')
ws = wb.ActiveSheet
print(ws.Cells(1,1).Value)
excel.Quit()
'''

