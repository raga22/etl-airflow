import sqlite3
from sqlite3 import Error

#Constant DB
DBDISASTER = r"/home/pratama/project/week1/dags/db/disaster.db"
#Constant table
TBLDISASTER = """ CREATE TABLE IF NOT EXISTS disaster (
                                    id integer NOT NULL PRIMARY KEY,
                                    keyword text NULL,
                                    location text NULL,
                                    text text NULL,
                                    target int
                                ); """


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
    finally:
        print("success create table")
        conn.close()



# create a database connection
conn = create_connection(DBDISASTER)

# create tables
if conn is not None:
    # disaster
    create_table(conn, TBLDISASTER)
else:
    print("Error! cannot create the database connection.")


