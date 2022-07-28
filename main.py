import csv
import datetime
import os
from random import choice, randint
from operator import itemgetter
import pandas as pd
import pyodbc
import tqdm
import time
from time import sleep
import logging


xlsx_file = "C:\\bigdata\\original\\bigdata2.xlsx"
work_folder = "C:\\bigdata\\csv_files\\"
months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
years = [2017, 2018, 2019, 2020, 2021]


def menu():
    zamanal = False
    year_range_list = ['1', '2', '3', '4', '5']
    while True:
        dataset_range = input("\033[32m {}".format('Выберите размер датасета: \n'
                              '1. 1000 строк\n'
                              '2. 1млн. строк\n'
                              '3. 20млн. строк\n:'))
        if dataset_range == '1':
            dataset_range = 1000
            break
        elif dataset_range == '2':
            dataset_range = 1000000
            break
        elif dataset_range == '3':
            dataset_range = 20000000
            break
        else:
            print("\033[31m {}".format('Просто 1, 2 или 3. Что сложного?)'))
            sleep(1)
            zamanal = True
    while True:
        year_range = input("\033[32m {}".format('Введите количество годов в датасете\n(только от 1 до 5 лет можно):'))
        if not year_range in year_range_list:
            if zamanal == False:
                print("\033[31m {}".format('Просто от 1 до 5. Что сложного?)'))
                sleep(1)
            if zamanal == True:
                print("\033[31m {}".format(f'Ты меня опять за говнокод держишь?\n'
                                           f'Просто от 1 до 5, а не {year_range}.'))
                sleep(1)
        else:
            break
    return {'dataset_range': dataset_range, 'year_range': year_range}


def clear_folder(path):
    print("\033[37m {}".format("чищу каталог"))
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))


def get_csv(xlsx_file, work_folder, logfile):
    date = datetime.datetime.now()
    csv_name = date.strftime("%Y%m%d%H%M%S")
    log.info("open xlsx")
    print("\033[37m {}".format("открываю xlsx"))
    data_xls = pd.read_excel(xlsx_file, sheet_name='GroceryMar Pyat chips energ 10-')
    log.info("xlsx in csv")
    print("xlsx в csv")
    data_xls.to_csv(f'{work_folder}{csv_name}.csv', encoding='utf-8', index=False, header = False, sep= ';')
    return csv_name


def generate_csv(menu_results, csv_name, work_folder):
    print("считаю")
    log.info("calculating")
    with open(f'{work_folder}{csv_name}.csv', encoding='utf-8') as csvf:
        data = str(csvf.read())
    with open(f'{work_folder}{csv_name}.csv',mode="w", encoding='utf-8') as csvf:
        csv_writer = csv.writer(csvf, delimiter=";", lineterminator="\r")
        if menu_results["dataset_range"] == 1000:
            is_write = 0
            for row in tqdm.tqdm(data.splitlines()):
                row = list(row.split(';'))
                if is_write % 1000 == 0:
                    csv_writer.writerow(row)
                is_write += 1
        if menu_results["dataset_range"] == 1000000:
            for row in tqdm.tqdm(data.splitlines()):
                row = list(row.split(';'))
                csv_writer.writerow(row)
        if menu_results["dataset_range"] == 20000000:
            data = data * 20
            for i, row in enumerate(tqdm.tqdm(data.splitlines())):
                print(f'{i}- {row}')
                row = list(row.split(';'))
                csv_writer.writerow(row)


def csv_from_excel(work_folder, csv_name):
    log.info("cleaning csv")
    print("шлифую csv")
    with open(f'{work_folder}{csv_name}.csv', encoding='utf-8') as csvf:
        data = str(csvf.read())
    with open(f"{work_folder}{csv_name}v.csv", mode="w", encoding='utf-8') as w_file:
        csv_writer = csv.writer(w_file, delimiter=";", lineterminator="\r")
        for i, row in enumerate(tqdm.tqdm(data.splitlines())):
            row = row.replace('"', '').replace('\n', '')
            row = list(row.split(';'))
            row[0] = choice(months)
            row[1] = choice(years)
            if row[5] == '':
                log.warning(f"fixed row; row index - {i}")
                row[5] = randint(50, 100)
            if row[6] == '':
                log.warning(f"fixed row; row index - {i}")
                row[6] = randint(100.0, 1000.0)
            if row[7] == '':
                log.warning(f"fixed row; row index - {i}")
                row[7] = float(row[6]) * 1.35
            csv_writer.writerow(row)
    return f'{work_folder}{csv_name}v.csv'

def change_date(work_folder, csv_name):
    with open(f'{work_folder}{csv_name}.csv', encoding='utf-8') as csvf:
        data = str(csvf.read())
    with open(f"{work_folder}{csv_name}v.csv", mode="w", encoding='utf-8') as w_file:
        csv_writer = csv.writer(w_file, delimiter=";", lineterminator="\r")
        for row in tqdm.tqdm(data.splitlines()):
            row = list(row.split(';'))
            row[0] = choice(months)
            row[1] = choice(years)
            csv_writer.writerow(row)



clear_folder(work_folder)
logfile = f'{work_folder}log_1.log'
log = logging.getLogger("my_log")
log.setLevel(logging.INFO)
FH = logging.FileHandler(logfile, encoding='utf-8')
basic_formater = logging.Formatter('%(asctime)s : [%(levelname)s] : %(message)s')
FH.setFormatter(basic_formater)
log.addHandler(FH)
menu_results = menu()
log.info(f"start programm with dataset range - {menu_results['dataset_range']}")
date = datetime.datetime.now()
datestr = date.strftime("%Y%m%d%H%M%S")
csv_name = get_csv(xlsx_file, work_folder, logfile)
date = datetime.datetime.now()
datestr = date.strftime("%Y%m%d%H%M%S")
#clear_folder("C:\\bigdata")
csv_name = get_csv(xlsx_file, work_folder)
csv_namev = csv_from_excel(work_folder, csv_name)
print("подключаюсь к sql")
db_name = f"data{datestr}"
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=ASUSALEX\SQLEXPRESS;'
                      'Database=python_sql;'
                      'Trusted_Connection=yes;')
print("подключился")
cursor = conn.cursor()
cursor.execute(f'''
		CREATE TABLE {db_name} (
			month int,
			year int,
			id int,
			product_name nvarchar(200),
			supplier_name nvarchar(200),
			quantity_sold int,
			purchase_price float, 
			retail_price float
			)                     
            ''')
conn.commit()
data = pd.read_csv(csv_namev, on_bad_lines='skip')
df = pd.DataFrame(data)
for row in tqdm.tqdm(df.itertuples()):
    cursor.execute(f'''
                INSERT INTO {db_name} (month, year, id, product_name, supplier_name,
                                      quantity_sold, purchase_price, retail_price)
                VALUES (?,?,?,?,?,?,?,?)
                ''',
                   row[1],
                   row[2],
                   row[3],
                   row[4],
                   row[5],
                   row[6],
                   row[7],
                   row[8],
                   )
conn.commit()
print('кукукукуку')
