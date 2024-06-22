import findspark
findspark.init()
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StringType, StructType, TimestampType
import os
import psycopg2
from sqlalchemy import create_engine

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

conn = psycopg2.connect(
    host="localhost",
    database="impression_db",
    user="root",
    password="root")

# JSON Schema
json_schema = StructType() \
  .add('ImpressionID', StringType()) \
  .add('UserID', StringType()) \
  .add('SessionID', StringType()) \
  .add('AdID', StringType()) \
  .add('AdCategory', StringType()) \
  .add('Timestamp', TimestampType()) \
  .add('OS', StringType()) \
  .add('Browser', StringType()) \
  .add('Location', StringType()) \
  .add('Referrer', StringType()) \
  .add('ImpressionCost', FloatType()) \
  .add('ClickID', StringType()) \
  .add('ClickPosition', StringType()) \
  .add('ClickCost', FloatType())

spark = SparkSession \
  .builder \
  .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
  .config("spark.jars", "postgresql-42.7.1.jar") \
  .appName("impression-event") \
  .getOrCreate()

impression_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "impression-event") \
  .option("startingOffsets", "latest") \
  .load()

impression_json_df = impression_df.selectExpr("cast(value as string) as value")
raw_df = impression_json_df.withColumn("value", from_json(impression_json_df["value"], json_schema)).select("value.*") 

sql = """DROP TABLE IF EXISTS impression CASCADE"""
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()

sql = """CREATE TABLE impression (
        impressionid TEXT,
        userid TEXT,
        sessionid TEXT,
        adid TEXT,
        adcategory TEXT,
        timestamp TIMESTAMP,
        device TEXT,
        os TEXT,
        browser TEXT,
        location TEXT,
        pageid TEXT,
        referrer TEXT,
        impressioncost DECIMAL,
        clickid TEXT NULL,
        clickposition TEXT NULL,
        clickcost DECIMAL NULL 
);"""
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()

def write_to_postgresql(df,epoch_id):
  df.write \
  .format('jdbc') \
  .option("url", "jdbc:postgresql://localhost:5432/impression_db") \
  .option("dbtable", "impression") \
  .option("user", "root") \
  .option("password", "root") \
  .option("driver", "org.postgresql.Driver") \
  .mode('append') \
  .save()

postgresql_stream=raw_df.writeStream \
.foreachBatch(write_to_postgresql) \
.start() \
.awaitTermination()