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
	for i in range(1, 12):
		if i < 10:
			taxi = sc.textFile('hdfs:///user/pkhatiw000/Data/yellow_tripdata_2016-0{0}.csv'.format(i), use_unicode=False).cache()
		else:
			taxi = sc.textFile('hdfs:///user/pkhatiw000/Data/yellow_tripdata_2016-{0}.csv'.format(i), use_unicode=False).cache()
		taxiRDD = taxi.mapPartitionsWithIndex(parserTaxi)
		df = sqlContext.createDataFrame(taxiRDD, ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude', 'Date', 'Weekday', 'HourofDay'])
		df = df.select(['Date', 'HourofDay']).groupBy(['Date', 'HourofDay']).count()
		df.coalesce(1).write.csv('hdfs:///user/pkhatiw000/CleanData/NewHourly/yellow_tripdata_2016-{0}_AGG_Hourly'.format(i), header=True)
