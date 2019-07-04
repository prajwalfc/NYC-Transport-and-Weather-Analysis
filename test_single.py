import pyspark
from pyspark.sql import SQLContext
import csv
import pandas as pd
import dateutil
import datetime
import calendar

# Clean Taxi Data
def parserTaxi(id, data):
    if id == 0:
        data.next()
    for i in csv.reader(data):
        tpep_pickup_datetime, tpep_dropoff_datetime, pickup_longitude, \
        pickup_latitude, dropoff_longitude, dropoff_latitude, Date, Weekday, HourofDay = i[1], i[2], i[5], i[6], i[9], i[10], \
        datetime.datetime.strptime(i[1], "%Y-%m-%d %H:%M:%S").date(), \
        calendar.day_name[datetime.datetime.strptime(i[1], "%Y-%m-%d %H:%M:%S").weekday()], \
        datetime.datetime.strptime(i[1], "%Y-%m-%d %H:%M:%S").hour
       
        # rows required for the taxi data
        yield tpep_pickup_datetime, tpep_dropoff_datetime, pickup_longitude, \
        pickup_latitude, dropoff_longitude, dropoff_latitude, Date, Weekday, HourofDay

if __name__ == '__main__':
    sc = pyspark.SparkContext()
    sqlContext = SQLContext(sc)
    taxi = sc.textFile('hdfs:///user/pkhatiw000/Data/yellow_tripdata_2016-01.csv', use_unicode=False).cache()
    taxiRDD = taxi.mapPartitionsWithIndex(parserTaxi)
    df = sqlContext.createDataFrame(taxiRDD, ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude', 'Date', 'Weekday', 'HourofDay'])
    df = df.select(['Date', 'HourofDay']).groupBy(['Date', 'HourofDay']).count()
    df.coalesce(1).write.csv('hdfs:///user/pkhatiw000/CleanData/Hourly/yellow_tripdata_2016-1_AGG_Hourly_1'.format(i), header=True)
