
import mysql.connector as mysql
from mysql.connector import MySQLConnection
import pandas as pd
import csv
from configparser import ConfigParser
from sqlalchemy import create_engine
from mysql import connector


#initialzing configs
config_object = ConfigParser()
config_object.read("config.ini")
databaseinfo = config_object['mysql_eu45_cred']

#for db operations engine (like quering what db exisits)
con = mysql.connect(
           host = databaseinfo['host'],
            database=databaseinfo['database'],
            user = databaseinfo['username'],
            password = databaseinfo['password'])
cur = con.cursor()
print(cur)
print(type(cur))


##query to check dbs with mysql connector engine
query='show databases' #"show tables" will give tables in db we ussing
# Execute query
cur.execute(query)
# Fetch all the databases
data_bases=cur.fetchall() #check fetchall
for data in data_bases:
    print(data)


#connection to db
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                    .format(host = databaseinfo['host'],
                            db = databaseinfo['database'],
                            user = databaseinfo['username'],
                            pw = databaseinfo['password']))

#for table operations engine
dbengine = engine.connect()

#query to check exisitng dbs with sqlalchemy engine
existing_dbs = engine.execute("SHOW DATABASES;")
existing_dbs = [d[0] for d in existing_dbs]
print(existing_dbs)



#getting table into dataframe, we can also get data from multiple tables using sql syntax
df = pd.read_sql('select * from healthcare_stroke_data', dbengine)

#transformations
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


#making a table in MySQL db using python
def create_staging_table(cursor) -> None:
    cur.execute("""
        DROP TABLE IF EXISTS health_risk_data;
        CREATE UNLOGGED TABLE health_risk_data (
            id                                     BIGINT, 
            gender                                 TEXT,
            age                                    INT,
            hypertension                           INT,
            heart_disease                          INT,
            ever_married                           TEXT,
            work_type                              TEXT,
            Residence_type                         TEXT,
            avg_glucose_level                      FLOAT,
            bmi                                    FLOAT,
            smoking_status                         TEXT,
            stroke                                 INT,
            age_group                              TEXT,
            glucose_level                          TEXT);""")

#loading data into mysql with python
df.to_sql('health_risk_data', engine, if_exists= 'append', chunksize= 1000, index = False) #use if_exists = 'replace' to replace

#creating dataframe
data={'Name':['Karan','Rohit','Sahil','Aryan'],'Age':[23,22,21,24]}
df1=pd.DataFrame(data)

#loading new dataframe into mysql with python
df1.to_sql('dataframe', engine, if_exists= 'replace', chunksize= 1000, index = False)