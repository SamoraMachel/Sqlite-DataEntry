from os import stat
import pandas as pd
import sqlite3

data = pd.read_csv("iHRIS_Current_1701.csv", sep=",", parse_dates=["Start Date"])

sqliteConn = sqlite3.connect("ihris.db")
cursor = sqliteConn.cursor()
 
# Create database 
sql_statement = '''
    create table ihris (
        id int auto_increment primary key,
        gender varchar(10),
        job varchar(30),
        cardre_name varchar(30),
        facility varchar(30),
        start_date datetime,
        employer varchar(10),
        county_name varchar(20),
        sub_county varchar(20),
        facility_code varchar(10),
        NHIF_NO int
    )
'''

try:
    cursor.execute(sql_statement)
    sqliteConn.commit()
    print("--> Created database - ihris.db")
except Exception as e:
    print(e)

counter = 0
for i, j in data.iterrows():
    columns = [i]
    for keys, values in j.items():
        columns.append(values)
    statement = f''' insert into ihris 
        values ({columns[0]}, "{columns[1]}", "{columns[2]}", "{columns[3]}", "{columns[4]}", "{columns[5]}", "{columns[6]}", "{columns[7]}", "{columns[8]}", "{columns[9]}", "{columns[10]}"); 
    '''
    cursor.execute(statement)
    sqliteConn.commit()
    counter += 1

print(f"--> Inserted {counter} rows")