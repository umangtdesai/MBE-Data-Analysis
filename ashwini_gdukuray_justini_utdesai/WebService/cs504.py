import pandas
import xlrd
import csv
import sqlite3
import xlsxwriter
import os


workbook1 = xlrd.open_workbook('/datasets/Massachusetts1.xlsx')
worksheet1 = workbook1.sheet_by_name('Sheet1')
num_rows1 = worksheet1.nrows - 1
curr_row1 = 0
row_array1 = []

mbe_dict = {'01801': [16, 15], '01880': [0, 13], '01890': [1, 4], '01901': [2, 2], '01902': [3, 1], '01904': [5, 2], '01905': [3, 1], '01906': [4, 2], '01907': [1, 1], '01945': [2, 8], '01970': [3, 9], '02026': [8, 5], '02108': [3, 7], '02109': [9, 18], '02110': [11, 19], '02111': [21, 5], '02113': [0, 2], '02114': [6, 11], '02115': [3, 1], '02116': [15, 14], '02118': [9, 6], '02119': [52, 1], '02120': [9, 0], '02121': [36, 0], '02122': [15, 3], '02124': [42, 1], '02125': [14, 1], '02126': [32, 0], '02127': [3, 4], '02128': [6, 5], '02129': [5, 11], '02130': [17, 6], '02131': [10, 6], '02132': [6, 2], '02134': [10, 3], '02135': [7, 3], '02136': [26, 4], '02138': [5, 6], '02139': [10, 12], '02140': [0, 4], '02141': [3, 3], '02142': [2, 1], '02143': [4, 9], '02144': [3, 5], '02145': [5, 4], '02148': [11, 7], '02149': [6, 5], '02150': [7, 6], '02151': [8, 0], '02152': [2, 6], '02155': [4, 7], '02169': [15, 11], '02170': [2, 3], '02171': [7, 4], '02176': [1, 2], '02180': [4, 3], '02186': [14, 6], '02199': [2, 2], '02210': [6, 15], '02215': [0, 3], '02420': [4, 6], '02421': [2, 6], '02445': [2, 3], '02446': [4, 10], '02451': [5, 5], '02452': [1, 4], '02453': [4, 5], '02458': [0, 8], '02459': [2, 8], '02460': [2, 4], '02461': [3, 2], '02464': [0, 4], '02465': [2, 3], '02466': [0, 3], '02467': [0, 1], '02468': [0, 1], '02472': [11, 7], '02474': [0, 6], '02476': [0, 4], '02478': [1, 10]}


while curr_row1 < num_rows1:
    row1 = worksheet1.row(curr_row1)
    row_array1 += row1
    curr_row1 += 1

workbook2 = xlrd.open_workbook('/datasets/Massachusetts1.xlsx')
worksheet2 = workbook2.sheet_by_name('Sheet2')
num_rows2 = worksheet2.nrows - 1
curr_row2 = 0
row_array2 = []

while curr_row2 < num_rows2:
    row2 = worksheet2.row(curr_row2)
    row_array2 += [row2]
    curr_row2 += 1


# Create array with info on zips only in massZips
finalZips = [["Zip", "Latitude", "Longitude", "MBEs", "nonMBEs"]]

def func(x,y, dict):
    counter = 0
    counter2 = 0
    row_x = len(x.col_values(0))
    row_y = len(y.col_values(0))
    col_x = x.row_len(0)
    col_y = y.row_len(0)
    for i in range(1, row_x):
        for j in range(1, row_y):
            x_cell = x.cell(i,0).value
            y_cell = y.cell(j,0).value
            if x_cell == y_cell:
                mbe_or_nmbe = mbe_dict.get(y_cell)
                if mbe_or_nmbe == None:
                    mbe = 0
                    nonmbe = 0
                else:
                    mbe = mbe_or_nmbe[0]
                    nonmbe = mbe_or_nmbe[1]
                zip_code = y.cell(i, 0).value.encode("utf-8")
                latitude = y.cell(i, 1).value
                longitude = y.cell(i, 2).value

                finalZips.append([zip_code,latitude,longitude, mbe, nonmbe])

    return finalZips

func(worksheet1, worksheet2, mbe_dict)

#workbook.close()

# Convert to nested array to csv: ***uncomment to generate csv
# with open("/Users/ashwinikulkarni/Desktop/cs504project/zipLongLat.csv", "wb") as f:
#     writer = csv.writer(f)
#     writer.writerows(finalZips)
