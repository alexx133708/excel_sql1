from sqlalchemy import create_engine, text

pswd = 'Alex@0209'
connection = create_engine(url=f"mysql+mysqldb://alex3@192.168.1.75/sales", echo=False)

print(connection)
table_name = 'Sell_list'
root = 'E:\\bigdata\\original\\FExport2017__index.csv'

sql1 = text("SELECT `P`.`ProductKey`, `P`.`SKU`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_1k` AS `S` WHERE `S`.`ProductKey` = `P`.`ProductKey` AND `S`.`Year` = :year) AS `all_sales` FROM `Products` AS `P` ORDER BY `all_sales` DESC LIMIT 10")
sql2 = text("SELECT `P`.`ProductKey`, `P`.`SKU`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_1k` AS `S` WHERE `S`.`ProductKey` = `P`.`ProductKey` AND `S`.`Year` = :year) AS `all_sales` FROM `Products` AS `P` ORDER BY `all_sales` LIMIT 10")
for year in range(2017, 2023):
    queries1 = connection.execute(sql1, year=year).fetchall()
    queries2 = connection.execute(sql2, year=year).fetchall()
    print(f"Год - {year} ПРИБЫЛЬНЫЕ")
    for i in range(len(queries1)):
        print("\t",queries1[i])
    print(f"Год - {year} НЕ ПРИБЫЛЬНЫЕ")
    for i in range(len(queries2)):
        print("\t",queries2[i])
sql1 = text("SELECT `P`.`StoreKey`, `P`.`Chain`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_1k` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey` AND `S`.`Year` = :year) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` DESC LIMIT 10")
sql2 = text("SELECT `P`.`StoreKey`, `P`.`Chain`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_1k` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey` AND `S`.`Year` = :year) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` LIMIT 10")
for year in range(2017, 2023):
    queries1 = connection.execute(sql1, year=year).fetchall()
    queries2 = connection.execute(sql2, year=year).fetchall()
    print(f"Год - {year} ПРИБЫЛЬНЫЕ")
    for i in range(len(queries1)):
        print("\t",queries1[i])
    print(f"Год - {year} НЕ ПРИБЫЛЬНЫЕ")
    for i in range(len(queries2)):
        print("\t",queries2[i])
sql1 = text("SELECT `P`.`StoreRegion`, `P`.`Chain`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_1k` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey`) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` DESC LIMIT 1")
sql2 = text("SELECT `P`.`StoreRegion`, `P`.`Chain`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_1k` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey`) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` LIMIT 1")
queries1 = connection.execute(sql1).fetchall()
queries2 = connection.execute(sql2).fetchall()
print("ПРИБЫЛЬНЫЙ")
print("\t",queries1)
print("НЕ ПРИБЫЛЬНЫЕЙ")
print("\t",queries2)
sql1 = text("SELECT `P`.`StoreRegion`, `P`.`Chain`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_1k` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey` AND `S`.`Month` = :month) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` DESC LIMIT 1")
sql2 = text("SELECT `P`.`StoreRegion`, `P`.`Chain`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_1k` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey` AND `S`.`Month` = :month) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` LIMIT 1")
for month in range(1, 13):
    queries1 = connection.execute(sql1, month=month).fetchall()
    queries2 = connection.execute(sql2, month=month).fetchall()
    print(f"Месяц - {month} ПРИБЫЛЬНЫЕ")
    for i in range(len(queries1)):
        print("\t",queries1[i])
    print(f"Месяц - {month} НЕ ПРИБЫЛЬНЫЕ")
    for i in range(len(queries2)):
        print("\t",queries2[i])
sql1 = text("SELECT `P`.`ProductKey`, `P`.`SKU`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_1k` AS `S` WHERE `S`.`ProductKey` = `P`.`ProductKey` AND `S`.`Month` = :month) AS `all_sales` FROM `Products` AS `P` ORDER BY `all_sales` DESC LIMIT 10")
sql2 = text("SELECT `P`.`ProductKey`, `P`.`SKU`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_1k` AS `S` WHERE `S`.`ProductKey` = `P`.`ProductKey` AND `S`.`Month` = :month) AS `all_sales` FROM `Products` AS `P` ORDER BY `all_sales` LIMIT 10")
for month in range(1, 13):
    queries1 = connection.execute(sql1, month=month).fetchall()
    queries2 = connection.execute(sql2, month=month).fetchall()
    print(f"Месяц - {month} ПРИБЫЛЬНЫЕ")
    for i in range(len(queries1)):
        print("\t", queries1[i])
    print(f"Месяц - {month} НЕ ПРИБЫЛЬНЫЕ")
    for i in range(len(queries2)):
        print("\t", queries2[i])
sql1 = text("SELECT `P`.`Chain`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_1k` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey` AND `S`.`Year` = :year) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` DESC LIMIT 3")
sql2 = text("SELECT `P`.`Chain`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_1k` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey` AND `S`.`Year` = :year) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales` LIMIT 3")
for year in range(2017, 2023):
    queries1 = connection.execute(sql1, year=year).fetchall()
    queries2 = connection.execute(sql2, year=year).fetchall()
    print(f"Год - {year} ПРИБЫЛЬНЫЕ")
    for i in range(len(queries1)):
        print("\t",queries1[i])
    print(f"Год - {year} НЕ ПРИБЫЛЬНЫЕ")
    for i in range(len(queries2)):
        print("\t",queries2[i])
#СНИЗУ НЕДОДЕЛАНО ALERT! ALERT!
# sql1 = text("SELECT `P`.`StoreFormat`, (SELECT ROUND(SUM(`S`.`SalesValue`), 2) FROM `Sell_list_1k` AS `S` WHERE `S`.`StoreKey` = `P`.`StoreKey` AND `S`.`Year` = :year) AS `all_sales` FROM `Stores` AS `P` ORDER BY `all_sales`")
# for year in range(2017, 2023):
#     store_format = {}
#     queries1 = connection.execute(sql1, year=year).fetchall()
#     print(f"Год - {year} ПРИБЫЛЬНЫЕ")
#     for i in range(len(queries1)):
#         print("\t",queries1[i])
#     for format, value in queries1:
#         if queries1[0] in store_format.keys():
#             store_format.update({queries1[0]: value + queries1[1]})
#         if queries1[0] not in store_format.keys():
#             store_format.update({queries1[0]: queries1[1]})
