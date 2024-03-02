import sqlite3

def create_db():
    try:
        sqlite_connection = sqlite3.connect('SMART-Bonds.db')
    except sqlite3.Error as error:
        print("Ошибка в CREATE_DB", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()

def create_table_all_bonds():
    try:
        sqlite_connection = sqlite3.connect('SMART-Bonds.db')
        sqlite_create_table_query = '''CREATE TABLE IF NOT EXISTS All_Bonds (
                                    URL TEXT UNIQUE,
                                    Название TEXT,
                                    Котировка_облигации REAL DEFAULT "Информация отсутствует",
                                    Доходность_к_погашению REAL DEFAULT "Информация отсутствует",
                                    Доходность_купона_к_рынку REAL DEFAULT "Информация отсутствует",
                                    Доходность_купона_к_номиналу REAL DEFAULT "Информация отсутствует",
                                    Частота_купона INTEGER DEFAULT "Информация отсутствует",
                                    Дата_погашения DATETIME DEFAULT "Информация отсутствует",
                                    Дней_до_погашения INTEGER DEFAULT "Информация отсутствует",
                                    ISIN TEXT DEFAULT "Информация отсутствует",
                                    Код_Бумаги TEXT DEFAULT "Информация отсутствует",
                                    Только_для_квалов TEXT DEFAULT "Информация отсутствует"
                                    );'''

        cursor = sqlite_connection.cursor()
        cursor.execute(sqlite_create_table_query)
        sqlite_connection.commit()
        cursor.close()
    except sqlite3.Error as error:
        print("Ошибка в CREATE_TABLE_ALL_BONDS", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()

def insert_change_into_table(information_bonds):
    try:
        sqlite_connection = sqlite3.connect('SMART-Bonds.db')
        cursor = sqlite_connection.cursor()

        url = information_bonds[0]
        cursor.execute("SELECT * FROM All_Bonds WHERE url=?", (url,))
        result = cursor.fetchone()

        if result:
            sqlite_insert_change_with_param = """UPDATE All_Bonds SET Название=?, Котировка_облигации=?, 
            Доходность_к_погашению=?, Доходность_купона_к_рынку=?, Доходность_купона_к_номиналу=?, Частота_купона=?,
            Дата_погашения=?, Дней_до_погашения=?, ISIN=?, Код_Бумаги=?, Только_для_квалов=? WHERE URL=?"""
            data_tuple = tuple(information_bonds[1::]) + (url,)
            print('UPDATE----------UPDATE----------UPDATE----------UPDATE----------UPDATE----------UPDATE----------UPDATE')
        else:
            sqlite_insert_change_with_param = """INSERT INTO All_Bonds
                                  (URL, Название, Котировка_облигации, Доходность_к_погашению, 
                                  Доходность_купона_к_рынку, Доходность_купона_к_номиналу, 
                                  Частота_купона, Дата_погашения, Дней_до_погашения, ISIN, 
                                  Код_Бумаги, Только_для_квалов)
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            data_tuple = tuple(information_bonds)
            print('INSERT----------INSERT----------INSERT----------INSERT----------INSERT----------INSERT----------INSERT')

        cursor.execute(sqlite_insert_change_with_param, data_tuple)
        sqlite_connection.commit()
        cursor.close()

    except sqlite3.Error as error:
        print("Ошибка в INSERT_INTO_TABLE", error)
    finally:
        if sqlite_connection:
            sqlite_connection.close()
