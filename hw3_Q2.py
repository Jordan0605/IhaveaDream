from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("hw3_Q2")#.set("spark.ui.port", 9999)
sc = SparkContext(conf = conf)
sqc = SQLContext(sc)

a = sqc.read.format("com.databricks.spark.csv") \
	.options(header = 'true', inferschema = 'true') \
   	.load("yellow_tripdata_2016-08.csv")

b = a.select('passenger_count', 'payment_type').filter(a.passenger_count > 0)
c = b.groupBy('payment_type').mean()
c.show()

