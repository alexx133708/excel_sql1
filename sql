import csv
from mysql.connector import connect, Error
import tqdm
import pandas as pd
# import pymysql
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine


# conn_str = (
#     r'DRIVER={SQL Server};'
#     r'SERVER=192.168.1.75:3306'
#     r'DATABASE=sales;'
#     r'USER=alex2;'
#     r'PASSWORD=Alex@0209;')
# conn = pyodbc.connect(conn_str)
pswd = 'Alex@0209'
connection = create_engine(url=f"mysql+mysqldb://alex3@192.168.1.75/sales", echo=False)
# engine = sqlalchemy.create_engine("mysql+pyodbc://alex2:Alex@0209@alexkamensky.ru/phpmyadmin/sales&driver=ODBC+Driver+17+for+SQL+Server")

# try:
#     with connect(
#         host="192.168.1.75",
#         user="alex2",
#         password="Alex@0209",
#         database="sales"
#     ) as connection:
#         print(connection)
# except Error as e:
#     print(e)
print(connection)
table_name = 'Sell_list'
root = 'E:\\bigdata\\original\\FExport2017__index.csv'
data = []

# with open(root, 'r', encoding='utf-8') as file:
#     fulldata = file.readlines()
#     count = len(fulldata) // 1000
#     while count != 0:
#         print(count)
#         count -= 1
#         for i, row in tqdm.tqdm(enumerate(fulldata)):
#             row.replace('\n', '').split(';')
#             data.append(row.replace('\n', '').split(';'))
#             row = row.split(';')
#             if i == 1000:
#                 break
#         csv_file = pd.DataFrame(data)
#         csv_file.fillna(value='_', inplace=True)
#         csv_file.to_sql(table_name, connection, if_exists='append', index=False)
#         for i,row in enumerate(fulldata):
#             if i < 1000:
#                 fulldata.pop(i)
#                 if len(fulldata) == 0:
#                     print(len(fulldata))
#                     break

connection.execute("SELECT `Month`, SUM(`SalesValue`) FROM `Sell_list_5m` GROUP BY `Month`")
print(connection.execute().fetchall())
connection.execute("SELECT `Year`, SUM(`SalesValue`) FROM `Sell_list_5m` GROUP BY `Year`")
print(connection.execute().fetchall())
connection.execute("SELECT `Month`, SUM(`SalesValue`) FROM `Sell_list_5m` GROUP BY `Month` DESC")
print(connection.execute().fetchall())
connection.execute("SELECT `Year`, SUM(`SalesValue`) FROM `Sell_list_5m` GROUP BY `Year` DESC")
print(connection.execute().fetchall())

