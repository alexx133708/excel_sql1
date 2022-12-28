import csv
import tqdm
import pandas as pd
import pymysql
import sqlalchemy
from sqlalchemy import create_engine

engine = sqlalchemy.create_engine('mssql+pyodbc://GAME\\SQLEXPRESS/new_db?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')

root = 'E:\\bigdata\\original\\FExport2017_.csv'
i = 1
data = []

with open(root, 'r', encoding='utf-8') as file:
    fulldata = file.readlines()
    while len(fulldata) != 0:
        for i,row in tqdm.tqdm(enumerate(fulldata)):
            print(row.replace('\n', '').split(','))
            data.append(row.replace('\n', '').split(','))
            print(len(row.split(',')))
            if i == 1000:
                break
        headers = data[0]
        csv_file = pd.DataFrame(data, columns=headers)
        print(csv_file.head())
        csv_file.fillna(value='_', inplace=True)
        csv_file.to_sql("Sell_list", engine, if_exists='append', index=False)
        for i,row in enumerate(fulldata):
            if i <= 1000:
                fulldata.pop(i)
                if len(fulldata) == 0:
                    break
