from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, avg, count

spark = SparkSession.builder \
    .appName("NYC Taxi Analysis") \
    .getOrCreate()

input_path = "/opt/spark/data/raw/yellow_tripdata_2024-01.parquet"
output_path = "/opt/spark/data/processed/trips_by_hour"

df = spark.read.parquet(input_path)

result = df.withColumn("trip_date", to_date(col("tpep_pickup_datetime"))) \
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
    .groupBy("trip_date", "pickup_hour") \
    .agg(
        count("*").alias("trip_count"),
        avg("fare_amount").alias("avg_fare"),
        avg("trip_distance").alias("avg_distance")
    ) \
    .orderBy("trip_date", "pickup_hour")

result.write.mode("overwrite").parquet(output_path)

result.show(20, truncate=False)

spark.stop()