import pandas
import sqlite3
from sqlite3 import Error

def extract_disaster():
    try:
        df = pandas.read_csv("/home/pratama/project/week1/dataset/disaster_data.csv")
        return df
    except Exception as e:
        print(e)
    
def tranform_disaster():
    #in development.......
    return

def load_disaster(dat):
    try:
        conn = sqlite3.connect("/home/pratama/project/week1/dags/db/disaster.db")
        c = conn.cursor()
        dat.to_sql('disaster', conn, if_exists='replace', index = False)
    except Error as e:
        print(e)
    finally:
        print("sukses")


disaster_dataframe = extract_disaster()
load_disaster(disaster_dataframe)