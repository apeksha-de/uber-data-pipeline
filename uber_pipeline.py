# Uber Data Pipeline (PySpark)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, unix_timestamp, round, count

# Initialize Spark session
spark = SparkSession.builder.appName("UberDataPipeline").getOrCreate()

# Load data
df = spark.read.option("header", True).csv("uber-trip.xlsx")  

# Convert pickup_datetime to Date type
df = df.withColumn("pickup_date", to_date(col("pickup_datetime")))

# Remove trips with 0 or negative distance
df = df.filter(col("trip_distance") > 0)

# Calculate trip duration in minutes
df = df.withColumn("trip_duration", round((unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60, 2))

# Group by pickup location and count trips
df_grouped = df.groupBy("pickup_location").agg(count("*").alias("trip_count"))

# Show transformed data
df.show()
df_grouped.show()

# Save transformed data (Parquet format)
df.write.mode("overwrite").parquet("output/uber_trips_transformed")
df_grouped.write.mode("overwrite").parquet("output/uber_trips_grouped")

# Stop Spark Session
spark.stop()
