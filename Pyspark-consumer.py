"""
As part of our training at the IA School. We have to provide a Big Data project using to stream and plot real time 
charts.

This script will:

- Read product's information from Kafka topic 'products-events'
- Write the information into a Spark SQL table named 'Products' under the 'default' schema
- Start the Spark Thrift Service, which will be used by Tableau Desktop to plot real time charts.

@author Siham Saidoun

@date 06/05/2021
@version V.1
"""


# Import necessary packages
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, StructField 
import pyspark.sql.functions as F

from py4j.java_gateway import java_import


spark = SparkSession \
    .builder \
    .appName("BigData-Project") \
    .enableHiveSupport() \
    .config('spark.sql.hive.thriftServer.singleSession', True) \
    .getOrCreate()

sc = spark.sparkContext 


java_import(sc._gateway.jvm,"")

#Start the Thrift Server using the jvm and passing the same spark session corresponding to pyspark session 
# in the jvm side.
sc._gateway.jvm.org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.startWithContext(spark._jwrapped)


print('Start Building the Schema ....')
schema_product = StructType() \
      .add('Nom',StringType()) \
      .add('Code_barres', StringType()) \
      .add('Nutri_score',  StringType()) \
      .add('NOVA',  StringType()) \
      .add('Eco_Score', StringType()) \
      .add('Sel', StringType()) \
      .add('Sucres', StringType()) \
      .add('Additifs', StringType()) 
      
print(schema_product)
print('End Building Schema')

print('Start Reading from kafka broker...')
inputData = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "products-events") \
  .load() \
  .select(from_json(F.col("value").cast('string'), schema_product).alias("value")) \
  .selectExpr("value.Nom", "value.Code_barres", "value.NOVA", "value.Nutri_score", "value.Eco_Score", "value.Sel", "value.Sucres", "value.Additifs")

query = inputData \
    .writeStream \
    .option("checkpointLocation", './checkpoint/') \
    .toTable('default.Products')

query.awaitTermination()

print('End Reading from kafka broker...')
