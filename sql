from sqlalchemy import create_engine, text
import logging

pswd = 'Alex@0209'
connection = create_engine(url=f"mysql+mysqldb://alex3@192.168.1.75/sales", echo=False)
print(connection)

table_name = 'Sell_list'
folder = 'E:\\bigdata\\results\\'
root = 'E:\\bigdata\\original\\FExport2017__index.csv'
while True:
    size = input('1.1000 строк\n2.1000000 строк\n3.5000000 строк\nВыберите размер таблицы:')
    if size == '1':
        size = '1k'
        break
    elif size == '2':
        size = '1m'
        break
    elif size == '3':
        size = '5m'
        break
    else:
        print('1 или 2 дурачок')
logfile = f'{folder}process'
log = logging.getLogger("my_log")
log.setLevel(logging.INFO)
FH = logging.FileHandler(logfile, encoding='utf-8')
basic_formater = logging.Formatter('%(asctime)s : [%(levelname)s] : %(message)s')
FH.setFormatter(basic_formater)
log.addHandler(FH)
log.info('start program---------------------------------------------------------------------------')
log.info(f'dataset_size = {size}')
with open(f'{folder}result.txt', 'w', encoding='utf-8') as res:
    # sql1 = text(f"SELECT `P`.`ProductKey`, `P`.`SKU`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_{size}` AS `S` WHERE `S`.`ProductKey` = `P`.`ProductKey` AND `S`.`Year` = :year) AS `all_sales` FROM `Products` AS `P` ORDER BY `all_sales` DESC LIMIT 10")
    # sql2 = text(f"SELECT `P`.`ProductKey`, `P`.`SKU`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_{size}` AS `S` WHERE `S`.`ProductKey` = `P`.`ProductKey` AND `S`.`Year` = :year) AS `all_sales` FROM `Products` AS `P` ORDER BY `all_sales` LIMIT 10")
    # for year in range(2017, 2023):
    #     log.info(f'query №1 {year} start')
    #     queries1 = connection.execute(sql1, year=year).fetchall()
    #     queries2 = connection.execute(sql2, year=year).fetchall()
    #     log.info(f'query №1 {year} end')
    #     log.info(f'printing query №1 {year} result start')
    #     print(f"Год - {year} ПРИБЫЛЬНЫЕ")
    #     res.write(f"Год - {year} ПРИБЫЛЬНЫЕ\n")
    #     for i in range(len(queries1)):
    #         print("\t",queries1[i])
    #         res.write(f"\t{queries1[i]}\n")
    #     print(f"Год - {year} НЕ ПРИБЫЛЬНЫЕ")
    #     res.write(f"Год - {year} НЕ ПРИБЫЛЬНЫЕ\n")
    #     for i in range(len(queries2)):
    #         print("\t",queries2[i])
    #         res.write(f"\t{queries2[i]}\n")
    #     log.info(f'printing query №1 {year} result end')
    # sql1 = text(f"SELECT `P`.`StoreKey`, `P`.`Chain`, `P`.`StoreAddress`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_{size}` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey` AND `S`.`Year` = :year) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` DESC LIMIT 10")
    # sql2 = text(f"SELECT `P`.`StoreKey`, `P`.`Chain`, `P`.`StoreAddress`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_{size}` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey` AND `S`.`Year` = :year) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` LIMIT 10")
    # for year in range(2017, 2023):
    #     log.info(f'query №2 {year} start')
    #     queries1 = connection.execute(sql1, year=year).fetchall()
    #     queries2 = connection.execute(sql2, year=year).fetchall()
    #     log.info(f'query №2 {year} end')
    #     log.info(f'printing query №2 {year} result start')
    #     print(f"Год - {year} ПРИБЫЛЬНЫЕ")
    #     res.write(f"Год - {year} ПРИБЫЛЬНЫЕ\n")
    #     for i in range(len(queries1)):
    #         print("\t",queries1[i])
    #         res.write(f"\t{queries1[i]}\n")
    #     print(f"Год - {year} НЕ ПРИБЫЛЬНЫЕ")
    #     res.write(f"Год - {year} НЕ ПРИБЫЛЬНЫЕ\n")
    #     for i in range(len(queries2)):
    #         print("\t",queries2[i])
    #         res.write(f"\t{queries2[i]}\n")
    #     log.info(f'printing query №2 {year} result end')
    # sql1 = text(f"SELECT `P`.`StoreRegion`, `P`.`Chain`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_{size}` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey`) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` DESC LIMIT 1")
    # sql2 = text(f"SELECT `P`.`StoreRegion`, `P`.`Chain`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_{size}` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey`) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` LIMIT 1")
    # log.info('query №3 start')
    # queries1 = connection.execute(sql1).fetchall()
    # queries2 = connection.execute(sql2).fetchall()
    # log.info('query №3 end')
    # log.info('printing query №3 result start')
    # print("ПРИБЫЛЬНЫЙ")
    # res.write("ПРИБЫЛЬНЫЙ\n")
    # print("\t",queries1)
    # res.write(f"\t{queries1}\n")
    # print("НЕ ПРИБЫЛЬНЫЕЙ")
    # res.write("НЕ ПРИБЫЛЬНЫЙ\n")
    # print("\t",queries2)
    # res.write(f"\t{queries2}\n")
    # log.info('printing query №3 result end')
    # sql1 = text(f"SELECT `P`.`StoreRegion`, `P`.`Chain`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_{size}` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey` AND `S`.`Month` = :month) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` DESC LIMIT 1")
    # sql2 = text(f"SELECT `P`.`StoreRegion`, `P`.`Chain`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_{size}` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey` AND `S`.`Month` = :month) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` LIMIT 1")
    # for month in range(1, 13):
    #     log.info(f'query №4 {year} start')
    #     queries1 = connection.execute(sql1, month=month).fetchall()
    #     queries2 = connection.execute(sql2, month=month).fetchall()
    #     log.info(f'query №4 {year} end')
    #     log.info(f'printing query №4 {year} result start')
    #     print(f"Месяц - {month} ПРИБЫЛЬНЫЕ")
    #     res.write(f"Месяц - {month} ПРИБЫЛЬНЫЕ\n")
    #     for i in range(len(queries1)):
    #         print("\t",queries1[i])
    #         res.write(f"\t{queries1[i]}\n")
    #     print(f"Месяц - {month} НЕ ПРИБЫЛЬНЫЕ")
    #     res.write(f"Месяц - {month} НЕ ПРИБЫЛЬНЫЕ\n")
    #     for i in range(len(queries2)):
    #         print("\t",queries2[i])
    #         res.write(f"\t{queries2[i]}\n")
    #     log.info(f'printing query №4 {year} result end')
    # sql1 = text(f"SELECT `P`.`ProductKey`, `P`.`SKU`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_{size}` AS `S` WHERE `S`.`ProductKey` = `P`.`ProductKey` AND `S`.`Month` = :month) AS `all_sales` FROM `Products` AS `P` ORDER BY `all_sales` DESC LIMIT 10")
    # sql2 = text(f"SELECT `P`.`ProductKey`, `P`.`SKU`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_{size}` AS `S` WHERE `S`.`ProductKey` = `P`.`ProductKey` AND `S`.`Month` = :month) AS `all_sales` FROM `Products` AS `P` ORDER BY `all_sales` LIMIT 10")
    # for month in range(1, 13):
    #     log.info(f'query №5 {year} start')
    #     queries1 = connection.execute(sql1, month=month).fetchall()
    #     queries2 = connection.execute(sql2, month=month).fetchall()
    #     log.info(f'query №5 {year} end')
    #     log.info(f'printing query №5 {year} result start')
    #     print(f"Месяц - {month} ПРИБЫЛЬНЫЕ")
    #     res.write(f"Месяц - {month} ПРИБЫЛЬНЫЕ\n")
    #     for i in range(len(queries1)):
    #         print("\t", queries1[i])
    #         res.write(f"\t{queries1[i]}\n")
    #     print(f"Месяц - {month} НЕ ПРИБЫЛЬНЫЕ")
    #     res.write(f"Месяц - {month} НЕ ПРИБЫЛЬНЫЕ\n")
    #     for i in range(len(queries2)):
    #         print("\t", queries2[i])
    #         res.write(f"\t{queries2[i]}\n")
    #     log.info(f'printing query №5 {year} result end')
    # sql1 = text(f"SELECT `P`.`Chain`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_{size}` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey` AND `S`.`Year` = :year) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` DESC LIMIT 3")
    # sql2 = text(f"SELECT `P`.`Chain`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_{size}` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey` AND `S`.`Year` = :year) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` LIMIT 3")
    # for year in range(2017, 2023):
    #     log.info(f'query №6 {year} start')
    #     queries1 = connection.execute(sql1, year=year).fetchall()
    #     queries2 = connection.execute(sql2, year=year).fetchall()
    #     log.info(f'query №6 {year} end')
    #     log.info(f'printing query №6 {year} result start')
    #     print(f"Год - {year} ПРИБЫЛЬНЫЕ")
    #     res.write(f"Год - {year} ПРИБЫЛЬНЫЕ\n")
    #     for i in range(len(queries1)):
    #         print("\t",queries1[i])
    #         res.write(f"\t{queries1[i]}\n")
    #     print(f"Год - {year} НЕ ПРИБЫЛЬНЫЕ")
    #     res.write(f"Год - {year} НЕ ПРИБЫЛЬНЫЕ\n")
    #     for i in range(len(queries2)):
    #         print("\t",queries2[i])
    #         res.write(f"\t{queries2[i]}\n")
    #     log.info(f'printing query №6 {year} result end')
    # sql1 = text(f"SELECT DISTINCT `St`.`StoreFormat`, (SELECT ROUND( SUM(`S`.`SalesValue`), 3) FROM `Sell_list_{size}` AS `S` JOIN `Stores` AS `St2` ON `St2`.`StoreKey` = `S`.`StoreKey` AND `St2`.`StoreFormat` = `St`.`StoreFormat` AND `S`.`Year` = :year) AS `all_sales` FROM `Stores` AS `St` ORDER BY `all_sales` DESC")
    # for year in range(2017, 2023):
    #     c = 0
    #     summ = 0
    #     store_format = {}
    #     sorted_dict = {}
    #     log.info(f'query №7 {year} start')
    #     queries1 = connection.execute(sql1, year=year).fetchall()
    #     log.info(f'query №7 {year} end')
    #     log.info(f'printing query №7 {year} result start')
    #     print(f"Год - {year} Доля продаж")
    #     res.write(f"Год - {year} Доля продаж\n")
    #     for i in range(len(queries1)):
    #         if queries1[i][1] != None:
    #             summ = summ + queries1[i][1]
    #     for i in range(len(queries1)):
    #         if queries1[i][1] != None:
    #             print(f'\t{queries1[i][0]} - {round(queries1[i][1]/summ*100)}%')
    #             res.write(f'\t{queries1[i][0]} - {round(queries1[i][1]/summ*100)}%\n')
    #     log.info(f'printing query №7 {year} result end')
    sql1 = text(f"SELECT DISTINCT `StoreFormat` FROM `Stores`")
    queries1 = connection.execute(sql1).fetchall()
    for year in range(2017, 2023):
        query = {}
        summ = 0
        print(f'Год - {year} Доля продаж')
        res.write(f"Год - {year} Доля продаж\n")
        for store_format in queries1:
            sql1 = f"SELECT ROUND(SUM(`SL`.`SalesValue`), 3) as `all_sales` FROM `Sell_list_{size}` `SL` JOIN `Stores` `S` ON `S`.`StoreKey` = `SL`.`StoreKey` WHERE `SL`.`Year` = {year} AND `S`.`StoreFormat` = '{store_format[0]}'"
            queries2 = connection.execute(sql1).fetchall()
            if queries2[0][0] != None and queries2[0][0] != 0:
                query.update({store_format[0]: queries2[0][0]})
        summ = sum(query.values())
        query = dict(sorted(query.items(), key=lambda item: item[1], reverse= True))
        for format, price in query.items():
            print(f'\t{format} - {round(price / summ * 100)}%')
            res.write(f'\t{format} - {round(price / summ * 100)}%\n')
    sql1 = text(f"SELECT DISTINCT `Category` FROM `Products`")
    queries1 = connection.execute(sql1).fetchall()
    for year in range(2017, 2023):
        query = {}
        summ = 0
        print(f'Год - {year} Доля продаж')
        res.write(f"Год - {year} Доля продаж\n")
        for store_format in queries1:
            sql1 = f"SELECT ROUND(SUM(`SL`.`SalesValue`), 3) as `all_sales` FROM `Sell_list_{size}` `SL` JOIN `Products` `P` ON `P`.`ProductKey` = `SL`.`ProductKey` WHERE `SL`.`Year` = {year} AND `P`.`Category` = '{store_format[0]}'"
            queries2 = connection.execute(sql1).fetchall()
            if queries2[0][0] != None and queries2[0][0] != 0:
                query.update({store_format[0]: queries2[0][0]})
        summ = sum(query.values())
        query = dict(sorted(query.items(), key=lambda item: item[1], reverse=True))
        for format, price in query.items():
            print(f'\t{format} - {round(price / summ * 100)}%')
            res.write(f'\t{format} - {round(price / summ * 100)}%\n')
