from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, when
from pyspark.sql.types import IntegerType, DoubleType, StringType

# Create Spark session
spark = SparkSession.builder \
    .appName("FlightIQ_ETL") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

print("Spark Session created successfully")

# Read raw CSV data from HDFS
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://namenode:9000/raw/flights/*.csv")

print(f"Raw data loaded with {df_raw.count()} rows")

# Clean data and convert data types
df_clean = df_raw \
    .withColumn("YEAR", col("YEAR").cast(IntegerType())) \
    .withColumn("MONTH", col("MONTH").cast(IntegerType())) \
    .withColumn("DAY_OF_MONTH", col("DAY_OF_MONTH").cast(IntegerType())) \
    .withColumn("DAY_OF_WEEK", col("DAY_OF_WEEK").cast(IntegerType())) \
    .withColumn("CRS_DEP_TIME", col("CRS_DEP_TIME").cast(IntegerType())) \
    .withColumn("DISTANCE", col("DISTANCE").cast(IntegerType())) \
    .withColumn("ARR_DELAY", coalesce(col("ARR_DELAY"), lit(0))) \
    .withColumn("DEP_DELAY", coalesce(col("DEP_DELAY"), lit(0))) \
    .withColumn("CARRIER_DELAY", coalesce(col("CARRIER_DELAY"), lit(0))) \
    .withColumn("WEATHER_DELAY", coalesce(col("WEATHER_DELAY"), lit(0))) \
    .withColumn("NAS_DELAY", coalesce(col("NAS_DELAY"), lit(0))) \
    .withColumn("SECURITY_DELAY", coalesce(col("SECURITY_DELAY"), lit(0))) \
    .withColumn("LATE_AIRCRAFT_DELAY", coalesce(col("LATE_AIRCRAFT_DELAY"), lit(0))) \
    .filter(col("CANCELLED") == 0) \
    .filter(col("DIVERTED") == 0)

print(f"After cleaning: {df_clean.count()} rows remain")

# Create target column
df_clean = df_clean.withColumn(
    "IS_DELAYED",
    when(col("ARR_DELAY") > 15, 1).otherwise(0)
)

# Save to HDFS as Parquet
df_clean.write \
    .mode("overwrite") \
    .partitionBy("YEAR", "MONTH") \
    .parquet("hdfs://namenode:9000/cleaned/flights_clean")

print("ETL completed successfully")
print("Data saved as Parquet with YEAR and MONTH partitions")

# Write to PostgreSQL
df_clean.write \
    .mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/flightiq") \
    .option("dbtable", "flights_clean") \
    .option("user", "flightiq") \
    .option("password", "flightiq") \
    .option("driver", "org.postgresql.Driver") \
    .save()

print("Data written to PostgreSQL successfully")

# Show sample
print("Sample of cleaned data:")
df_clean.select(
    "YEAR", "MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK",
    "OP_UNIQUE_CARRIER", "ORIGIN", "DEST",
    "ARR_DELAY", "DEP_DELAY", "IS_DELAYED"
).show(10, truncate=False)

spark.stop()
print("Spark session stopped")