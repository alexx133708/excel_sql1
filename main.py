
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
            dataset_range = 2000000
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


def connect():
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=ASUSALEX\SQLEXPRESS;'
                          'Database=python_sql;'
                          'Trusted_Connection=yes;')
    conn.commit()
    return conn


def drop_table(table_name, cursor):
    cursor.execute(f"drop table {table_name}")
    print("данные удалены")
    cursor.commit()


def clear_folder(path):
    print("чищу каталог")
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            os.remove(os.path.join(root, name))


def get_csv(xlsx_file, work_folder):
    date = datetime.datetime.now()
    csv_name = date.strftime("%Y%m%d%H%M%S")
    print("открываю xlsx")
    data_xls = pd.read_excel(xlsx_file, sheet_name='GroceryMar Pyat chips energ 10-')
    print("xlsx в csv")
    data_xls.to_csv(f'{work_folder}{csv_name}.csv', encoding='utf-8', index=False, header = False)
    return csv_name


def csv_from_excel(work_folder, csv_name):
    print("шлифую csv")
    products_sums_list = {tuple(['','']):''}
    products_sums_list_help = {tuple(['','']):''}
    with open(f'{work_folder}{csv_name}.csv', encoding='utf-8') as csvf:
        data = str(csvf.read())
    with open(f"{work_folder}{csv_name}v.csv", mode="w", encoding='utf-8') as w_file:
        csv_writer = csv.writer(w_file, delimiter=",", lineterminator="\r")
        for row in tqdm.tqdm(data.splitlines()):
            row = row.replace('"', '').replace('\n', '')
            row = list(row.split(','))
            if row[5] == '':
                row[5] = randint(50, 100)
            if row[6] == '':
                row[6] = randint(100.0, 1000.0)
            if row[7] == '':
                row[7] = float(row[6]) * 1.35
            csv_writer.writerow(row)
    with open(f"{work_folder}{csv_name}v.csv", mode="r", encoding='utf-8') as v_file:
        data = str(v_file.read())
        for row in tqdm.tqdm(data.splitlines()):
            row = list(row.split(','))
            row[0] = choice(months)
            row[1] = choice(years)
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
        print('Топ за всё время: ')
        for key in products_sums_list:
            print(f'{i}.{key[0]} -> {products_sums_list[key]}')
            i += 1
            if i > 10:
                break
        i = 1
        print()
        print('======================================')
        print('Топ за 2017-ый год: ')
        for k,v in products_sums_list.items():
            if k[1] == 2017:
                print(f'    {i}.{k[0]} -> {v}')
                i += 1
                if i > 10:
                    break
        i = 1
        print()
        print('Топ за 2018-ый год: ')
        for k,v in products_sums_list.items():
            if k[1] == 2018:
                print(f'    {i}.{k[0]} -> {v}')
                i += 1
                if i > 10:
                    break
        i = 1
        print()
        print('Топ за 2019-ый год: ')
        for k,v in products_sums_list.items():
            if k[1] == 2019:
                print(f'    {i}.{k[0]} -> {v}')
                i += 1
                if i > 10:
                    break
        i = 1
        print()
        print('Топ за 2020-ый год: ')
        for k, v in products_sums_list.items():
            if k[1] == 2020:
                print(f'    {i}.{k[0]} -> {v}')
                i += 1
                if i > 10:
                    break
        i = 1
        print()
        print('Топ за 2021-ый год: ')
        for k, v in products_sums_list.items():
            if k[1] == 2021:
                print(f'    {i}.{k[0]} -> {v}')
                i += 1
                if i > 10:
                    break

    return f'{work_folder}{csv_name}.csv'


def homework1(csv_file):
    product_list = {'':0}
    product_list_help = {'':0}
    count = 1
    summa = 0
    with open(csv_file, mode="r", encoding='utf-8') as r_file:
        data = str(r_file.read())
        csv_writer = csv.writer(r_file, delimiter=",", lineterminator="\r")
        for row in data.splitlines():
            row = row.replace('"', '').replace('\n', '')
            row = list(row.split(','))
            for product, price in product_list_help.items():
                if product != row[3]:
                    product_list.update({row[3]:row[7]})
            product_list_help = product_list.copy()
        product_list.pop('')
        for product, price in product_list.items():
            summa = summa + float(price)
            count += 1
        print('======================================\nСредняя цена товаров:')
        print(f'\t{round(summa/count, 2)}')


def homework2(csv_file):
    suppliers_products = {'':()}
    suppliers_products_help = {'':()}
    max = 0
    with open(csv_file, mode="r", encoding='utf-8') as r_file:
        data = str(r_file.read())
        csv_writer = csv.writer(r_file, delimiter=",", lineterminator="\r")
        for row in data.splitlines():
            row = row.replace('"', '').replace('\n', '')
            row = list(row.split(','))
            if row[4] in suppliers_products.keys():
                suppliers_products.update({row[4]: suppliers_products.get(row[4]) + (row[3],)})
            if not row[4] in suppliers_products.keys():
                suppliers_products.update({row[4]: (row[3],)})
        suppliers_products.pop('')
        print('======================================')
        print('Производитель с самым большим асортиментом: ')
        for supplier, products in suppliers_products.items():
            if max < len(products):
                max = len(products)
        for supplier, products in suppliers_products.items():
            if len(products) == max:
                print(f'\t{supplier} -> {max}  товаров')


menu_results = menu()
sleep(1)
print("\n\033[37m {}".format(f'Короче делаем датасет на {menu_results.get("dataset_range")} строк,\n'
                           f'а количество лет в датасете будет: {menu_results.get("year_range")} '))
# date = datetime.datetime.now()
# datestr = date.strftime("%Y%m%d%H%M%S")
# clear_folder(work_folder)
# csv_name = get_csv(xlsx_file, work_folder)
# csv_namev = csv_from_excel(work_folder, csv_name)
# homework1(csv_namev)
# homework2(csv_namev)
