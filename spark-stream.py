import findspark
findspark.init()
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StringType, StructType, TimestampType, IntegerType
import os
import psycopg2
from sqlalchemy import create_engine
import pandas as pd

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# SQL Schema
impression_schema = StructType() \
  .add('ImpressionID', IntegerType()) \
  .add('UserID', IntegerType()) \
  .add('SessionID', IntegerType()) \
  .add('AdID', IntegerType()) \
  .add('AdCategory', StringType()) \
  .add('ImpressionTimestamp', TimestampType()) \
  .add('Device', StringType()) \
  .add('OS', StringType()) \
  .add('Browser', StringType()) \
  .add('Location', StringType()) \
  .add('PageID', StringType()) \
  .add('Referrer', StringType()) \
  .add('ImpressionCost', FloatType()) \

click_schema = StructType() \
  .add('JoinID', IntegerType()) \
  .add('ClickTimestamp', TimestampType()) \
  .add('ClickID', StringType()) \
  .add('ClickPosition', StringType()) \
  .add('ClickCost', FloatType())


### build spark session with postgresql and kafka packages
spark = SparkSession \
  .builder \
  .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
  .config("spark.jars", "postgresql-42.7.1.jar") \
  .appName("impression-click-event") \
  .getOrCreate()


### read impressions and clicks kafka streams
impression_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "impression-event") \
  .option("startingOffsets", "latest") \
  .load()

click_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "click-event") \
  .option("startingOffsets", "latest") \
  .load()

### data transformation
impression_json_df = impression_df.selectExpr("cast(value as string) as value")
impression_raw_df = impression_json_df.withColumn("value", from_json(impression_json_df["value"], impression_schema)).select("value.*")
impression_raw_df = impression_raw_df.replace("\"NaN\"", None)

click_json_df = click_df.selectExpr("cast(value as string) as value")
click_raw_df = click_json_df.withColumn("value", from_json(click_json_df["value"], click_schema)).select("value.*")
click_raw_df = click_raw_df.replace("\"NaN\"", None)

### apply watermarks on event-time columns
impressionsWithWatermark = impression_raw_df.withWatermark("ImpressionTimestamp", "30 minutes").alias("impression")
clicksWithWatermark = click_raw_df.withWatermark("ClickTimestamp", "30 minutes").alias("click")

### stream-stream left outer join
joined_stream = impressionsWithWatermark.join(
  clicksWithWatermark,
  expr("""
    click.JoinID = impression.ImpressionID AND
    click.ClickTimestamp >= impression.ImpressionTimestamp AND
    click.ClickTimestamp <= impression.ImpressionTimestamp
    """),
    "leftOuter"
)

### connection to postgresql
conn = psycopg2.connect(
  host="localhost",
  database="superset",
  user="superset",
  password="superset")

### drop table if exist to ensure the table is new
sql = """DROP TABLE IF EXISTS joined CASCADE"""
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()

### postgresql create table with schema
sql = """CREATE TABLE joined (
  impressionid INT,
  userid INT,
  sessionid INT,
  adid INT,
  adcategory TEXT,
  impressiontimestamp TIMESTAMP,
  device TEXT,
  os TEXT,
  browser TEXT,
  location TEXT,
  pageid TEXT,
  referrer TEXT,
  joinid INT,
  impressioncost DECIMAL,
  clicktimestamp TIMESTAMP,
  clickid TEXT NULL,
  clickposition TEXT NULL,
  clickcost DECIMAL NULL 
);"""
cursor = conn.cursor()
cursor.execute(sql)
conn.commit()

### write data into postgresql
def write_to_postgresql(df,epoch_id):
  df.write \
  .format('jdbc') \
  .option("url", "jdbc:postgresql://localhost:5432/superset") \
  .option("dbtable", "joined") \
  .option("user", "superset") \
  .option("password", "superset") \
  .option("driver", "org.postgresql.Driver") \
  .mode('append') \
  .save()


### start writing data into postgresql
postgresql_stream=joined_stream.writeStream \
  .foreachBatch(write_to_postgresql) \
  .start() \
  .awaitTermination()