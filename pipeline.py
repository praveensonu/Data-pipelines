from ast import Delete
from pickle import APPEND
import mysql.connector as mysql
from mysql.connector import MySQLConnection
import pandas as pd
import csv
from configparser import ConfigParser
from sqlalchemy import create_engine

#initialzing configs
config_object = ConfigParser()
config_object.read("config.ini")
databseinfo = config_object['mysql_eu45_cred']


#connection to db
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db})"
                        .format(host = databseinfo['host'],
                                db = databseinfo['database'],
                                user = databseinfo['username'],
                                pw = databseinfo['password']))

dbengine = engine.connect()

#getting table into dataframe, we can also get data from multiple tables using sql syntax
df = pd.read_sql('select * from healthcare_stroke_data', dbengine)

#data operations
df['age_group'] = pd.cut(df['age'], bins=[0,30,60,100], labels=["young","middle_aged","old"])
df.age_group.dropna(inplace = True)
df['bmi'].interpolate(inplace=True)
#glucose levels check
def glucose_level(x):
    if x<115:
        return 'Excellent'
    elif x > 115 and x < 180:
        return 'Medium'
    else:
        return 'Need attention'
df['glucose_level'] = df['avg_glucose_level'].apply(glucose_level)


df.to_sql('health_risk_data', engine, if_exists= APPEND, chunksize= 1000)