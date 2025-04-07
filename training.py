from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import time

spark = SparkSession.builder.appName("PrepareFakeStreamingFiles").getOrCreate()

# Load your original static orders data
orders_df = spark.read.option("header", True).csv("data/raw/orders/orders.csv")

# Inject processing-time as ArrivalTimestamp
orders_with_ts = orders_df.withColumn("ArrivalTimestamp", current_timestamp())

revenue = spark.read.parquet("data/output/revenue_by_country/*.parquet")
revenue.show()
