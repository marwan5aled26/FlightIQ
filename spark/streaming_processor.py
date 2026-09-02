from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, sum, when, current_timestamp, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml import PipelineModel

# Initialize Spark Streaming session
spark = SparkSession.builder \
    .appName("FlightIQ_Streaming") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark Streaming session created")

# Define the schema for incoming JSON messages
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

# Read the real-time stream from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "flight-data") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

print("Connected to Kafka stream")

# Parse JSON and handle missing values
flights_stream = kafka_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

required_cols = ["DAY_OF_WEEK", "CRS_DEP_TIME", "DISTANCE", "DEP_DELAY", "MONTH", "DAY_OF_MONTH", "OP_UNIQUE_CARRIER", "ORIGIN", "DEST"]

for col_name in required_cols:
    if col_name in ["DAY_OF_WEEK", "CRS_DEP_TIME", "DISTANCE", "DEP_DELAY", "MONTH", "DAY_OF_MONTH"]:
        flights_stream = flights_stream.fillna({col_name: 0})
    else:
        flights_stream = flights_stream.fillna({col_name: "Unknown"})

# Add processing timestamp and convert to Egypt timezone
flights_stream = flights_stream.withColumn("processing_time_utc", current_timestamp())
flights_stream = flights_stream.withColumn(
    "processing_time",
    from_utc_timestamp(col("processing_time_utc"), "Africa/Cairo")
)

# Calculate actual delay flag
flights_stream = flights_stream.withColumn(
    "IS_DELAYED",
    when(col("ARR_DELAY") < -18, 1).otherwise(0)
)

# Load the pre-trained ML model and apply it to the stream
model_path = "hdfs://namenode:9000/models/flight_delay_model"
try:
    model = PipelineModel.load(model_path)
    print("ML model loaded successfully from HDFS!")
    predictions_stream = model.transform(flights_stream)
except Exception as e:
    print(f"Model not found, using rule-based prediction. Error: {e}")
    predictions_stream = flights_stream.withColumn(
        "prediction",
        when(col("ARR_DELAY") < -18, 1.0).otherwise(0.0)
    )

# Debug: Print individual predictions to console
debug_pred = predictions_stream.select(
    "OP_UNIQUE_CARRIER",
    "ORIGIN",
    "DEST",
    "IS_DELAYED",
    "prediction",
    "probability"
)
debug_query2 = debug_pred.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 20) \
    .start()

# Aggregate statistics in 1-minute windows
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

# Write aggregated results to PostgreSQL for Power BI
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

# Also show aggregated results in console for monitoring
console_query = window_stats.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='30 seconds') \
    .start()

print("Streaming queries started")
print("Press Ctrl+C to stop")

# Await termination
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Streaming stopped by user")
    spark.streams.stopAll()
    print("All streaming queries stopped")
finally:
    spark.stop()
    print("Spark session stopped")