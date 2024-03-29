from dotenv import load_dotenv
import sqlite3
import os

load_dotenv()

ABSOLUTE_PATH = os.getenv('ABSOLUTE_PATH')
def create_db():
    try:
        sqlite_connection = sqlite3.connect(ABSOLUTE_PATH)
    except sqlite3.Error as error:
        print("Ошибка в CREATE_DB", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()

def create_table_all_bonds():
    try:
        sqlite_connection = sqlite3.connect(ABSOLUTE_PATH)
        sqlite_create_table_query = '''CREATE TABLE IF NOT EXISTS All_Bonds (
                                    URL TEXT UNIQUE,
                                    NAME TEXT,
                                    Quoting REAL,
                                    Repayment REAL,
                                    Market REAL,
                                    Nominal REAL,
                                    Frequency INTEGER,
                                    Date DATETIME,
                                    Days INTEGER,
                                    ISIN TEXT,
                                    Code TEXT,
                                    Qualification TEXT,
                                    TIME_DATE TEXT
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
        sqlite_connection = sqlite3.connect(ABSOLUTE_PATH)
        cursor = sqlite_connection.cursor()

        url = information_bonds[0]
        cursor.execute("SELECT * FROM All_Bonds WHERE URL=?", (url,))
        result = cursor.fetchone()

        if result:
            sqlite_insert_change_with_param = """UPDATE All_Bonds SET NAME=?, Quoting=?, Repayment=?, 
                                                Market=?, Nominal=?, Frequency=?, Date=?, Days=?, ISIN=?,
                                                Code=?, Qualification=?, TIME_DATE=? WHERE URL=?"""
            data_tuple = tuple(information_bonds[1::]) + (url,)
        else:
            sqlite_insert_change_with_param = """INSERT INTO All_Bonds
                                  (URL, NAME, Quoting, Repayment, 
                                  Market, Nominal, 
                                  Frequency, Date, Days, ISIN, 
                                  Code, Qualification, TIME_DATE)
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            data_tuple = tuple(information_bonds)

        cursor.execute(sqlite_insert_change_with_param, data_tuple)
        sqlite_connection.commit()
        cursor.close()

    except sqlite3.Error as error:
        print("Ошибка в INSERT_INTO_TABLE", error)
    finally:
        if sqlite_connection:
            sqlite_connection.close()

def CT_User_Information():
    """
    Ф-ия создающая таблицу внутри базы данных, подключение к которой происходит через абсолютный путь к файлу.
    Создаёт колонки хранящие в себе:
    ID - id пользователя написавшего команду /start
    NAME - Имя указанное в профиле пользователя
    USER_NAME - Ссылка на профиль пользователя
    ACCESS - Имеет ли данный человек разрешение на пользование ботом -> True/False
    :return:
    """
    try:
        sqlite_connection = sqlite3.connect(ABSOLUTE_PATH)
        sqlite_create_table_query = '''CREATE TABLE IF NOT EXISTS User_information (
                                    ID TEXT UNIQUE,
                                    NAME TEXT,
                                    USER_NAME TEXT,
                                    ACCESS TEXT
                                    );'''
        cursor = sqlite_connection.cursor()
        cursor.execute(sqlite_create_table_query)
        sqlite_connection.commit()
        cursor.close()
    except sqlite3.Error as error:
        print("Ошибка в create_table_User_Information", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()

def CT_User_settings():
    """
    :param ID: ID пользователя который зарегистрировался.
    :param quoting: Параметры котировок облигаций.
    :param repayment: Параметры Доходности к погашению.
    :param nominal: Параметры Доходности купона к номиналу.
    :param market: Параметры Доходности купона к рыночной цене.
    :param frequency: Параметры Частоты купона.
    :param days: Параметры Дней до погашения.
    :param qualification: Статус квал. True / False
    :return:
    """
    try:
        sqlite_connection = sqlite3.connect(ABSOLUTE_PATH)
        sqlite_create_table_query = '''CREATE TABLE IF NOT EXISTS User_settings (
                                        ID TEXT UNIQUE,
                                        quoting TEXT,
                                        repayment TEXT,
                                        nominal TEXT,
                                        market TEXT,
                                        frequency TEXT,
                                        days TEXT,
                                        qualification TEXT
                                        );'''
        cursor = sqlite_connection.cursor()
        cursor.execute(sqlite_create_table_query)
        sqlite_connection.commit()
        cursor.close()
    except sqlite3.Error as error:
        print("Ошибка в create_client_settings", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
