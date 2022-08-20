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


xlsx_file = "E:\\bigdata\\original\\bigdata2.xlsx"
work_folder = "E:\\bigdata\\csv_files\\"
logfile = f'{work_folder}log_1.log'
months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
years = [2021, 2020, 2019, 2018, 2017]
date = datetime.datetime.now()
datestr = date.strftime("%Y%m%d%H%M%S")


def init_log():
    log = logging.getLogger("my_log")
    log.setLevel(logging.INFO)
    FH = logging.FileHandler(logfile, encoding='utf-8')
    basic_formater = logging.Formatter('%(asctime)s : [%(levelname)s] : %(message)s')
    FH.setFormatter(basic_formater)
    log.addHandler(FH)
    return log


def sql_calculate(log, work_folder, csv_name, datestr):
    result_file = open(f'{work_folder}result.txt', mode="w", encoding='utf-8')
    log.info("connecting to sql")
    print("подключаюсь к sql")
    db_name = f"data{datestr}"
    date = datetime.datetime.now()
    datestr = date.strftime("%Y%m%d%H%M%S")
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=WIN-5V0VN6G6SPV\SQLEXPRESS;'
                          'Database=python_sql;'
                          'Trusted_Connection=yes;')
    print("подключился, создаю таблицу")
    log.info('connected to sql')
    log.info('createing table')
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
    log.info('table created')
    print('создал')
    with open(f'{work_folder}{csv_name}.csv', encoding='utf-8') as csvf:
        data = csvf.read()
    print('закидываю данные в таблицу')
    log.info('inserting data into table')
    for row in tqdm.tqdm(data.splitlines()):
        row = list(row.split(';'))
        cursor.execute(f'''
                    INSERT INTO {db_name} (month, year, id, product_name, supplier_name,
                                          quantity_sold, purchase_price, retail_price)
                    VALUES (?,?,?,?,?,?,?,?)
                    ''',
                       row[0],
                       row[1],
                       row[2],
                       row[3],
                       row[4],
                       row[5],
                       row[6],
                       row[7]
                       )
    conn.commit()
    log.info('data was inserted')
    cursor.execute(f"UPDATE {db_name} SET quantity_sold = FLOOR(RAND()*(100))+1 WHERE quantity_sold = ''")
    cursor.execute(f"UPDATE {db_name} SET purchase_price = FLOOR(RAND()*(200))+1 WHERE purchase_price = ''")
    cursor.execute(f"UPDATE {db_name} SET retail_price = quantity_sold * 1.35 WHERE retail_price = ''")
    conn.commit()
    log.info('caluclating and printing results')


    result_file.write('\nТоп за всё время: \n')
    print('Топ за всё время: ')
    cursor.execute(
        f"SELECT DISTINCT G.[product_name], (SELECT SUM(([retail_price] - [purchase_price]) * [quantity_sold]) FROM {db_name} as O WHERE O.[product_name] = G.[product_name]) as profit FROM {db_name} as G ORDER BY [profit] DESC")
    result = cursor.fetchall()
    for i, res in enumerate(result):
        if i >= 10:
            break
        result_file.write(f'\t{i+1}.{res[0]} -> {round(res[1], 2)}\n')
        print(f'\t{i+1}.{res[0]} -> {round(res[1])}')


    for year in years:
        result_file.write('\n======================================')
        print('======================================')
        result_file.write(f'\nТоп за {year} год: \n')
        print(f'Топ за {year} год: ')
        cursor.execute(
            f"SELECT DISTINCT G.[product_name], (SELECT SUM(([retail_price] - [purchase_price]) * [quantity_sold]) FROM {db_name} as O WHERE O.[product_name] = G.[product_name] AND [year] = {year}) as profit FROM {db_name} as G ORDER BY [profit] DESC")
        result = cursor.fetchall()
        for i, res in enumerate(result):
            result_file.write(f'\t{i + 1}.{res[0]} -> {round(res[1])}\n')
            print(f'\t{i + 1}.{res[0]} -> {round(res[1])}')
            if i == 10:
                break

    cursor.execute(
        f"SELECT TOP(1) G.[year], (SELECT SUM(([retail_price] - [purchase_price]) * [quantity_sold]) FROM {db_name} as O WHERE O.[year] = G.[year]) as profit FROM {db_name} as G ORDER BY [profit] DESC")
    result = cursor.fetchall()[0]
    print(f'самый прибыльный год - {result[0]} ({round(result[1], 2)}р)')
    result_file.write(f'самый прибыльный год - {result[0]} ({round(result[1], 2)}р)\n')
    log.info(f'most profit year - {result[0]} ({round(result[1], 2)}rub)')
    cursor.execute(
        f"SELECT TOP(1) G.[year], (SELECT SUM(([retail_price] - [purchase_price]) * [quantity_sold]) FROM {db_name} as O WHERE O.[year] = G.[year]) as profit FROM {db_name} as G ORDER BY [profit]")
    result = cursor.fetchall()[0]
    print(f'самый неприбыльный год - {result[0]} ({round(result[1], 2)}р)')
    result_file.write(f'самый неприбыльный год - {result[0]} ({round(result[1], 2)}р)\n')
    log.info(f'most unprofit year - {result[0]} ({round(result[1], 2)}rub)')
    cursor.execute(
        f"SELECT DISTINCT G.[month], (SELECT SUM(([retail_price] - [purchase_price]) * [quantity_sold]) FROM {db_name} as O WHERE O.[month] = G.[month]) as profit FROM {db_name} as G ORDER BY [profit] DESC")
    result = cursor.fetchall()[0]
    print(f'самый прибыльный месяц - {result[0]} ({round(result[1], 2)}р)')
    result_file.write(f'самый прибыльный месяц - {result[0]} ({round(result[1], 2)}р)\n')
    log.info(f'most profit month - {result[0]} ({round(result[1], 2)}rub)')
    cursor.execute(
        f"SELECT DISTINCT G.[month], (SELECT SUM(([retail_price] - [purchase_price]) * [quantity_sold]) FROM {db_name} as O WHERE O.[month] = G.[month]) as profit FROM {db_name} as G ORDER BY [profit]")
    result = cursor.fetchall()[0]
    print(f'самый неприбыльный месяц - {result[0]} ({round(result[1], 2)}р)')
    result_file.write(f'самый неприбыльный месяц - {result[0]} ({round(result[1], 2)}р)\n')
    log.info(f'most unprofit month - {result[0]} ({round(result[1], 2)}rub)')
    cursor.execute(
        f"SELECT DISTINCT G.[product_name], (SELECT SUM(([retail_price] - [purchase_price]) * [quantity_sold]) FROM {db_name} as O WHERE O.[product_name] = G.[product_name]) as profit FROM {db_name} as G ORDER BY [profit] DESC")
    result = cursor.fetchall()[0]
    print(f'самый прибыльный товар - {result[0]} ({round(result[1], 2)}р)')
    result_file.write(f'самый прибыльный товар - {result[0]} ({round(result[1], 2)}р)\n')
    log.info(f'most profit product - {result[0]} ({round(result[1], 2)}rub)')
    cursor.execute(
        f"SELECT DISTINCT G.[product_name], (SELECT SUM(([retail_price] - [purchase_price]) * [quantity_sold]) FROM {db_name} as O WHERE O.[product_name] = G.[product_name]) as profit FROM {db_name} as G ORDER BY [profit]")
    result = cursor.fetchall()[0]
    print(f'самый неприбыльный товар - {result[0]} ({round(result[1], 2)}р)')
    result_file.write(f'самый неприбыльный товар - {result[0]} ({round(result[1], 2)}р)\n')
    log.info(f'most unprofit product - {result[0]} ({round(result[1], 2)}rub)')
    cursor.execute(
        f"SELECT DISTINCT G.[supplier_name], (SELECT SUM(([retail_price] - [purchase_price]) * [quantity_sold]) FROM {db_name} as O WHERE O.[supplier_name] = G.[supplier_name]) as profit FROM {db_name} as G ORDER BY [profit] DESC")
    result = cursor.fetchall()[0]
    print(f'самый прибыльный поставщик - {result[0]} ({round(result[1], 2)}р)')
    result_file.write(f'самый прибыльный поставщик - {result[0]} ({round(result[1], 2)}р)\n')
    log.info(f'most profit supplier - {result[0]} ({round(result[1], 2)}rub)')
    cursor.execute(
        f"SELECT DISTINCT G.[supplier_name], (SELECT SUM(([retail_price] - [purchase_price]) * [quantity_sold]) FROM {db_name} as O WHERE O.[supplier_name] = G.[supplier_name]) as profit FROM {db_name} as G ORDER BY [profit]")
    result = cursor.fetchall()[0]
    print(f'самый неприбыльный поставщик - {result[0]} ({round(result[1], 2)}р)')
    result_file.write(f'самый неприбыльный поставщик - {result[0]} ({round(result[1], 2)}р)\n')
    log.info(f'most unprofit supplier - {result[0]} ({round(result[1], 2)}rub)')
    result_file.close()


def python_calculate(work_folder, csv_name):
    products_sums_list = {tuple(['','']):''}
    products_sums_list_help = {tuple(['','']):''}
    with open(f'{work_folder}{csv_name}v.csv', mode="r", encoding='utf-8') as v_file:
        data = str(v_file.read())
        for row in tqdm.tqdm(data.splitlines()):
            row = list(row.split(';'))
            for product, price in products_sums_list_help.items():
                if row[3] != product:
                    products_sums_list.update({tuple([row[3], row[1]]):float(round((float(row[7])-float(row[6]))*int(row[5]), 2))})
                if row[3] == product:
                    products_sums_list.update({product: price + float(round((float(row[7]) - float(row[6])) * int(row[5]), 2))})
            products_sums_list_help = products_sums_list.copy()
        products_sums_list_new = {}
        for key, value in products_sums_list.items():
            if not isinstance(value, str):
                products_sums_list_new[key] = value

        products_sums_list = dict(sorted(products_sums_list_new.items(), key=itemgetter(1), reverse=True))
        i = 1
        print()
        with open(f"{work_folder}result.txt", 'w', encoding= 'utf-8') as f:
            f.write('Топ за всё время: \n')
            print('Топ за всё время: ')
            for key in products_sums_list:
                f.write(f'{i}.{key[0]} -> {products_sums_list[key]}\n')
                print(f'{i}.{key[0]} -> {products_sums_list[key]}')
                i += 1
                if i > 10:
                    break
            i = 1
            print()
            f.write('\n======================================')
            print('======================================')
            f.write('\nТоп за 2017-ый год: \n')
            print('Топ за 2017-ый год: ')
            for k,v in products_sums_list.items():
                if k[1] == '2017':
                    f.write(f'    {i}.{k[0]} -> {v}\n')
                    print(f'    {i}.{k[0]} -> {v}')
                    i += 1
                    if i > 10:
                        break
            i = 1
            print()
            f.write('\nТоп за 2018-ый год: \n')
            print('Топ за 2018-ый год: ')
            for k,v in products_sums_list.items():
                if k[1] == '2018':
                    f.write(f'    {i}.{k[0]} -> {v}\n')
                    print(f'    {i}.{k[0]} -> {v}')
                    i += 1
                    if i > 10:
                        break
            i = 1
            print()
            f.write('\nТоп за 2019-ый год: \n')
            print('Топ за 2019-ый год: ')
            for k,v in products_sums_list.items():
                if k[1] == '2019':
                    f.write(f'    {i}.{k[0]} -> {v}\n')
                    print(f'    {i}.{k[0]} -> {v}')
                    i += 1
                    if i > 10:
                        break
            i = 1
            print()
            f.write('\nТоп за 2020-ый год: \n')
            print('Топ за 2020-ый год: ')
            for k, v in products_sums_list.items():
                if k[1] == '2020':
                    f.write(f'    {i}.{k[0]} -> {v}\n')
                    print(f'    {i}.{k[0]} -> {v}')
                    i += 1
                    if i > 10:
                        break
            i = 1
            print()
            f.write('\nТоп за 2021-ый год: \n')
            print('Топ за 2021-ый год: ')
            for k, v in products_sums_list.items():
                if k[1] == '2021':
                    f.write(f'    {i}.{k[0]} -> {v}\n')
                    print(f'    {i}.{k[0]} -> {v}')
                    i += 1
                    if i > 10:
                        break


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
        year_range = input("\033[32m {}".format('\nВведите количество годов в датасете\n(только от 1 до 5 лет можно):'))
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
    while True:
        calc_in = input("\033[32m {}".format('\nГде будем считать?\n1.SQL\n2.Python\n:'))
        if not calc_in in ['1', '2']:
            print("\033[31m {}".format(f'1 или 2, просто 1 или просто 2!'))
            continue
        if calc_in == '1':
            calc_in = 'SQL'
        elif calc_in == '2':
            calc_in = 'Python'
        break
    return {'dataset_range': dataset_range, 'year_range': int(year_range), 'calc_in': calc_in}


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
    print("подгоняю датасет")
    log.info("changing dataset range")
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


def csv_from_excel(work_folder, csv_name, log):
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


def check_prices(csv_name, log):
    log.info('checking prices')
    with open(csv_name, encoding='utf-8') as csvf:
        data = str(csvf.read())
    with open(csv_name, mode="w", encoding='utf-8') as w_file:
        csv_writer = csv.writer(w_file, delimiter=";", lineterminator="\r")
        for row in tqdm.tqdm(data.splitlines()):
            row = row.split(';')
            helper = 0
            if float(row[6]) > float(row[7]):
                log.error(f'the purchase price is higher than the retail price {row[6]} > {row[7]}')
                helper = row[6]
                row[6] = row[7]
                row[7] = helper
            csv_writer.writerow(row)

def change_date(work_folder, csv_name, log, menu_results):
    years1 = years
    while len(years1) != menu_results["year_range"]:
        years1.pop()
    log.info('changing date')
    with open(f"{work_folder}{csv_name}.csv", encoding='utf-8') as csvf:
        data = str(csvf.read())
    with open(f"{work_folder}{csv_name}.csv", mode="w", encoding='utf-8') as w_file:
        csv_writer = csv.writer(w_file, delimiter=";", lineterminator="\r")
        for row in tqdm.tqdm(data.splitlines()):
            row = row.replace('"', '').replace('\n', '')
            row = list(row.split(';'))
            row[0] = choice(months)
            row[1] = choice(years1)
            csv_writer.writerow(row)



clear_folder(work_folder)
log = init_log()
menu_results = menu()
log.info(f"start programm with dataset range - {menu_results['dataset_range']}; years range - {menu_results['year_range']}; calculating in {menu_results['calc_in']}")
csv_name = get_csv(xlsx_file, work_folder, log)
change_date(work_folder, csv_name, log, menu_results)
generate_csv(menu_results, csv_name, work_folder)
csv_namev = csv_from_excel(work_folder, csv_name, log)
check_prices(csv_namev, log)
if menu_results['calc_in'] == 'SQL':
    sql_calculate(log, work_folder, csv_name, datestr)
if menu_results['calc_in'] == 'Python':
    python_calculate(work_folder, csv_name)
