from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


spark = SparkSession.builder \
    .appName("FlightIQ-Spark-Streaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka message schema
schema = StructType([
    StructField("YEAR", StringType(), True),
    StructField("MONTH", StringType(), True),
    StructField("DAY_OF_MONTH", StringType(), True),
    StructField("DAY_OF_WEEK", StringType(), True),
    StructField("FL_DATE", StringType(), True),
    StructField("OP_UNIQUE_CARRIER", StringType(), True),
    StructField("OP_CARRIER_FL_NUM", StringType(), True),
    StructField("ORIGIN", StringType(), True),
    StructField("ORIGIN_CITY_NAME", StringType(), True),
    StructField("ORIGIN_STATE_ABR", StringType(), True),
    StructField("DEST", StringType(), True),
    StructField("DEST_CITY_NAME", StringType(), True),
    StructField("DEST_STATE_ABR", StringType(), True),
    StructField("CRS_DEP_TIME", StringType(), True),
    StructField("DEP_TIME", StringType(), True),
    StructField("DEP_DELAY", StringType(), True),
    StructField("DEP_DELAY_NEW", StringType(), True),
    StructField("DEP_DEL15", StringType(), True),
    StructField("CRS_ARR_TIME", StringType(), True),
    StructField("ARR_TIME", StringType(), True),
    StructField("ARR_DELAY", StringType(), True),
    StructField("ARR_DELAY_NEW", StringType(), True),
    StructField("ARR_DEL15", StringType(), True),
    StructField("CANCELLED", StringType(), True),
    StructField("CANCELLATION_CODE", StringType(), True),
    StructField("DIVERTED", StringType(), True),
    StructField("DISTANCE", StringType(), True),
    StructField("CARRIER_DELAY", StringType(), True),
    StructField("WEATHER_DELAY", StringType(), True),
    StructField("NAS_DELAY", StringType(), True),
    StructField("SECURITY_DELAY", StringType(), True),
    StructField("LATE_AIRCRAFT_DELAY", StringType(), True)
])

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "flight-events")  \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka value from binary to string
json_df = kafka_df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

# Select useful streaming information
result = json_df.select(
    "FL_DATE",
    "OP_UNIQUE_CARRIER",
    "ORIGIN",
    "DEST",
    "DEP_DEL15",
    "DISTANCE"
)

# 1) Display streaming data on screen (زي ما كان)
console_query = result.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 10) \
    .start()

# 2) Save streaming data to Parquet files (جديد)
file_query = result.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/opt/spark/work-dir/data/streaming_output") \
    .option("checkpointLocation", "/opt/spark/work-dir/data/streaming_checkpoint") \
    .start()

console_query.awaitTermination()
file_query.awaitTermination()