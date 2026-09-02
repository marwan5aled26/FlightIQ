from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, sum, when, current_timestamp, to_utc_timestamp, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("FlightIQ_Streaming") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark Streaming session created")

schema = StructType([
    StructField("YEAR", IntegerType(), True),
    StructField("MONTH", IntegerType(), True),
    StructField("DAY_OF_MONTH", IntegerType(), True),
    StructField("DAY_OF_WEEK", IntegerType(), True),
    StructField("FL_DATE", StringType(), True),
    StructField("OP_UNIQUE_CARRIER", StringType(), True),
    StructField("OP_CARRIER_FL_NUM", StringType(), True),
    StructField("ORIGIN", StringType(), True),
    StructField("ORIGIN_CITY_NAME", StringType(), True),
    StructField("ORIGIN_STATE_ABR", StringType(), True),
    StructField("DEST", StringType(), True),
    StructField("DEST_CITY_NAME", StringType(), True),
    StructField("DEST_STATE_ABR", StringType(), True),
    StructField("CRS_DEP_TIME", IntegerType(), True),
    StructField("DEP_TIME", IntegerType(), True),
    StructField("DEP_DELAY", DoubleType(), True),
    StructField("CRS_ARR_TIME", IntegerType(), True),
    StructField("ARR_TIME", IntegerType(), True),
    StructField("ARR_DELAY", DoubleType(), True),
    StructField("CANCELLED", IntegerType(), True),
    StructField("DIVERTED", IntegerType(), True),
    StructField("DISTANCE", IntegerType(), True),
    StructField("CARRIER_DELAY", DoubleType(), True),
    StructField("WEATHER_DELAY", DoubleType(), True),
    StructField("NAS_DELAY", DoubleType(), True),
    StructField("SECURITY_DELAY", DoubleType(), True),
    StructField("LATE_AIRCRAFT_DELAY", DoubleType(), True)
])

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "flight-data") \
    .option("startingOffsets", "latest") \
    .load()

print("Connected to Kafka stream")

flights_stream = kafka_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Convert processing_time to Egypt timezone (UTC+2 or UTC+3)
flights_stream = flights_stream.withColumn("processing_time_utc", current_timestamp())
flights_stream = flights_stream.withColumn(
    "processing_time",
    from_utc_timestamp(col("processing_time_utc"), "Africa/Cairo")
)

flights_stream = flights_stream.withColumn(
    "IS_DELAYED",
    when(col("ARR_DELAY") > 15, 1).otherwise(0)
)

predictions_stream = flights_stream.withColumn(
    "prediction",
    when(col("ARR_DELAY") > 15, 1.0).otherwise(0.0)
)
print("Using rule-based prediction")

window_stats = predictions_stream \
    .withWatermark("processing_time", "10 minutes") \
    .groupBy(
        window(col("processing_time"), "1 minutes", "1 minutes"),
        col("OP_UNIQUE_CARRIER")
    ) \
    .agg(
        count("*").alias("flight_count"),
        avg("ARR_DELAY").alias("avg_delay"),
        sum("IS_DELAYED").alias("delayed_count"),
        avg("prediction").alias("predicted_delay_rate")
    )

window_stats = window_stats.withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

def write_to_postgres(batch_df, epoch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .mode("append") \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/flightiq") \
            .option("dbtable", "streaming_stats") \
            .option("user", "flightiq") \
            .option("password", "flightiq") \
            .option("driver", "org.postgresql.Driver") \
            .save()

query = window_stats.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/streaming_stats") \
    .trigger(processingTime='30 seconds') \
    .start()

console_query = window_stats.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='30 seconds') \
    .start()

print("Streaming queries started")
print("Press Ctrl+C to stop")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Streaming stopped by user")
    spark.streams.stopAll()
    print("All streaming queries stopped")
finally:
    spark.stop()
    print("Spark session stopped")