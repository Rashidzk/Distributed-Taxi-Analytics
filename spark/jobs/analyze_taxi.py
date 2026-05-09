from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, avg, count

spark = SparkSession.builder \
    .appName("NYC Taxi Analysis") \
    .getOrCreate()

input_path = "/opt/spark/data/raw/yellow_tripdata_2024-01.parquet"
parquet_output_path = "/opt/spark/data/processed/trips_by_hour"
csv_output_path = "/opt/spark/data/processed/trips_by_hour_csv"

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
# to paraquet
result.write.mode("overwrite").parquet(parquet_output_path)
# to csv
result.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_output_path)
print("WRITE FINISHED")

spark.stop()