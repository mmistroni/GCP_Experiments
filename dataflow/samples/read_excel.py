import xlrd
import openpyxl
import pandas as pd

def read_with_xlrd(filename):
    wb = xlrd.open_workbook(filename)
    worksheet = wb.sheet_by_index(6)
    for i in range(3, 20):
        for j in range(0, 20):
            print(worksheet.cell_value(i, j), end='\t')

def read_with_pandas(filename):
    excel_data = pd.read_excel(filename, sheet_name=6, header=2)
    data = pd.DataFrame(excel_data)
    colnames = data.columns
    return data[colnames[1:]]