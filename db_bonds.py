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
        sqlite_connection = sqlite3.connect('SMART-Bonds.db')
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
