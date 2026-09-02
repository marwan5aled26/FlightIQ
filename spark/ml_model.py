from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, when

# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("FlightIQ_ML_Training") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 50)
print("FlightIQ - ML Model Training")
print("=" * 50)

# 2. Read data from PostgreSQL (historical data)
print("Loading data from PostgreSQL...")
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/flightiq") \
    .option("dbtable", "flights_") \
    .option("user", "flightiq") \
    .option("password", "flightiq") \
    .option("driver", "org.postgresql.Driver") \
    .load()

print(f"Loaded {df.count()} rows")

# 3. Select features and label
feature_cols = [
    "DAY_OF_WEEK",
    "CRS_DEP_TIME",
    "DISTANCE",
    "DEP_DELAY",
    "MONTH",
    "DAY_OF_MONTH"
]

# 4. Convert categorical columns
carrier_indexer = StringIndexer(inputCol="OP_UNIQUE_CARRIER", outputCol="carrier_index")
origin_indexer = StringIndexer(inputCol="ORIGIN", outputCol="origin_index")
dest_indexer = StringIndexer(inputCol="DEST", outputCol="dest_index")

# 5. Assemble features
assembler = VectorAssembler(
    inputCols=feature_cols + ["carrier_index", "origin_index", "dest_index"],
    outputCol="features_raw"
)

# 6. Scale features
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=True
)

# 7. Random Forest Classifier
rf = RandomForestClassifier(
    labelCol="IS_DELAYED",
    featuresCol="features",
    numTrees=50,
    maxDepth=10,
    seed=42
)

# 8. Create Pipeline
pipeline = Pipeline(stages=[
    carrier_indexer,
    origin_indexer,
    dest_indexer,
    assembler,
    scaler,
    rf
])

# 9. Train/Test Split
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

print(f"Training rows: {train_df.count()}")
print(f"Testing rows: {test_df.count()}")

# 10. Train Model
print("Training model...")
model = pipeline.fit(train_df)

# 11. Make Predictions
print("Making predictions...")
predictions = model.transform(test_df)

# 12. Evaluate Model
evaluator = MulticlassClassificationEvaluator(
    labelCol="IS_DELAYED",
    predictionCol="prediction",
    metricName="accuracy"
)

accuracy = evaluator.evaluate(predictions)

print("=" * 50)
print(f"Model Accuracy: {accuracy:.4f}")
print("=" * 50)

# 13. Show predictions
print("Sample predictions:")
predictions.select(
    "OP_UNIQUE_CARRIER",
    "ORIGIN",
    "DEST",
    "IS_DELAYED",
    "prediction",
    "probability"
).show(10, truncate=False)

# 14. Save model to HDFS
model_path = "hdfs://namenode:9000/models/flight_delay_model"
model.write().overwrite().save(model_path)
print(f"Model saved to: {model_path}")

# 15. Also save locally for streaming
local_model_path = "/opt/spark/work-dir/flight_delay_model"
model.write().overwrite().save(local_model_path)
print(f"Model saved locally to: {local_model_path}")

spark.stop()
print("Training completed!")