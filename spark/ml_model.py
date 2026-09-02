from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FlightIQ_ML_Training") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 50)
print("FlightIQ - ML Model Training (from HDFS)")
print("=" * 50)

# Load historical data from HDFS (Parquet format)
df = spark.read.parquet("hdfs://namenode:9000/cleaned/flights_clean")
print(f"Loaded {df.count()} rows")

# Check class distribution and compute weights for imbalanced dataset
print("Class distribution:")
df.groupBy("IS_DELAYED").count().show()

total = df.count()
class_counts = df.groupBy("IS_DELAYED").count().collect()
weights = {}
for row in class_counts:
    weights[row[0]] = float(total) / float(row[1])
print(f"Class weights: {weights}")

df = df.withColumn(
    "weight",
    when(col("IS_DELAYED") == 0, weights[0]).otherwise(weights[1])
)

# Select features for the model
feature_cols = [
    "DAY_OF_WEEK",
    "CRS_DEP_TIME",
    "DISTANCE",
    "DEP_DELAY",
    "MONTH",
    "DAY_OF_MONTH"
]

# Encode categorical columns (handle invalid categories with 'keep')
carrier_indexer = StringIndexer(
    inputCol="OP_UNIQUE_CARRIER",
    outputCol="carrier_index",
    handleInvalid="keep"
)
origin_indexer = StringIndexer(
    inputCol="ORIGIN",
    outputCol="origin_index",
    handleInvalid="keep"
)
dest_indexer = StringIndexer(
    inputCol="DEST",
    outputCol="dest_index",
    handleInvalid="keep"
)

# Assemble features into a single vector and standardize them
assembler = VectorAssembler(
    inputCols=feature_cols + ["carrier_index", "origin_index", "dest_index"],
    outputCol="features_raw"
)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=True
)

# Train a Random Forest classifier using class weights
rf = RandomForestClassifier(
    labelCol="IS_DELAYED",
    featuresCol="features",
    weightCol="weight",
    numTrees=100,
    maxDepth=15,
    seed=42
)

# Build and execute the ML pipeline
pipeline = Pipeline(stages=[
    carrier_indexer,
    origin_indexer,
    dest_indexer,
    assembler,
    scaler,
    rf
])

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"Training rows: {train_df.count()}")
print(f"Testing rows: {test_df.count()}")

print("Training model...")
model = pipeline.fit(train_df)

# Evaluate model accuracy on test data
print("Making predictions...")
predictions = model.transform(test_df)

evaluator = MulticlassClassificationEvaluator(
    labelCol="IS_DELAYED",
    predictionCol="prediction",
    metricName="accuracy"
)
accuracy = evaluator.evaluate(predictions)

print("=" * 50)
print(f"Model Accuracy: {accuracy:.4f}")
print("=" * 50)

# Display sample predictions
print("Sample predictions (all):")
predictions.select(
    "OP_UNIQUE_CARRIER",
    "ORIGIN",
    "DEST",
    "IS_DELAYED",
    "prediction",
    "probability"
).show(20, truncate=False)

print("Sample predictions (only delayed):")
predictions.filter(col("IS_DELAYED") == 1).select(
    "OP_UNIQUE_CARRIER",
    "ORIGIN",
    "DEST",
    "IS_DELAYED",
    "prediction",
    "probability"
).show(10, truncate=False)

# Save the trained model to HDFS and locally
model_path = "hdfs://namenode:9000/models/flight_delay_model"
model.write().overwrite().save(model_path)
print(f"Model saved to: {model_path}")

local_model_path = "/opt/spark/work-dir/flight_delay_model"
model.write().overwrite().save(local_model_path)
print(f"Model saved locally to: {local_model_path}")

spark.stop()
print("Training completed!")