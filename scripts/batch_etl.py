from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Connect to the Spark master node and set HDFS as our main file system
spark = SparkSession.builder \
    .appName("FlightIQ-Batch-ETL") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

print("\n==========================================")
print("=== Spark Session Created Successfully ===")
print("==========================================\n")

# Load raw flight dataset from HDFS
raw_df = spark.read.csv("hdfs://namenode:9000/raw/flights/flights.csv", header=True, inferSchema=True)
print(f"--> Total Raw Records Read: {raw_df.count()}")

# Fill missing delay values with 0 and create a binary flag for delays over 15 mins
cleaned_df = raw_df.fillna({
    'CARRIER_DELAY': 0,
    'WEATHER_DELAY': 0,
    'NAS_DELAY': 0,
    'SECURITY_DELAY': 0,
    'LATE_AIRCRAFT_DELAY': 0,
    'ARR_DELAY': 0,
    'DEP_DELAY': 0
}).withColumn("IsDelayed", when(col("ARR_DELAY") > 15, 1).otherwise(0))

# Export the processed dataset as partitioned Parquet files for faster querying
cleaned_df.write \
    .mode("overwrite") \
    .partitionBy("YEAR", "MONTH") \
    .parquet("hdfs://namenode:9000/processed/flights_parquet")

print("\n==========================================================")
print("=== ETL Completed: Saved Cleaned Parquet to HDFS! ===")
print("==========================================================\n")

spark.stop()