import requests
import json
import mysql.connector
from mysql.connector import Error
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

'''
station_In= input('Please input CSV file ')
Start_In= input('YYYY-MM-DD')
End_In = input('YYYY-MM-DD')


station_df= pd.read_csv(station_In, sheet='sheet 1')
'''

user = os.environ.get('SQLUSER')
password = os.environ.get('SQLPSW')
headers = {'token': os.environ.get('TOKEN')}
data = {'datatype'}
dataset_id = 'GHCND'
start_date = '2018-10-01'
end_date = '2019-10-01'
station_id = 'GHCND:US1NCBC0005'
url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=%s&stationid=%s&units=standard&datatypeid=PRCP&limit=1000&startdate=%s&enddate=%s' % (
    dataset_id, station_id, start_date, end_date)


def connect(Final_data):

    try:
        con = mysql.connector.connect(
            host='localhost', database='GHCND', user=user, password=password, charset='utf8')

        if con.is_connected():
            db_info = con.get_server_info()
            print("Connected to MySQL server version", db_info)
            cursor = con.cursor()
            query = "INSERT INTO GHCND_Precip (date, datatype, station, value) VALUES (%s, %s, %s, %s)"
            f=open('%s.txt' % (station_id),'w+')
            cursor.executemany(query, Final_data)
            for row in cursor:
                print>>f, row[0]
            f.close()
            cursor.execute('select database();')
            record = cursor.fetchone()
            print('You are connected to database:', record)
            con.commit()

    except Error as e:
        print("Error While Connecting to MySQL", e)

    finally:
        if (con.is_connected()):
            cursor.close()
            con.close()
            print('MySQL connection is closed.')
        return

r = requests.get(url=url, headers=headers)
raw = json.loads(r.text)
j = json.dumps(raw)

print(url)
print(r)
#print(raw)

i=0
Final_data = []

while True:
    i += 1
    c_date = raw["results"][i]["date"]
    if (c_date != (end_date+'T00:00:00')):
        date = raw["results"][i]["date"]
        datatype = raw["results"][i]["datatype"]
        station = raw["results"][i]["station"]
        value = raw["results"][i]["value"]
        InValue =(date, datatype, station, value)
        Final_data.append(InValue)
    else:
        i-=1
        break

print(Final_data)
connect(Final_data)
