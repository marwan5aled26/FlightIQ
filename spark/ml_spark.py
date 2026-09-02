from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("FlightIQ-Spark-ML") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# 2. Read flights data
data_path = "/opt/spark/work-dir/data/flights.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(data_path)


# 3. Select required columns
df = df.select(
    "MONTH",
    "DAY_OF_MONTH",
    "DAY_OF_WEEK",
    "OP_UNIQUE_CARRIER",
    "OP_CARRIER_FL_NUM",
    "ORIGIN",
    "DEST",
    "CRS_DEP_TIME",
    "DISTANCE",
    "DEP_DEL15"
)


# 4. Remove rows with missing values
df = df.dropna()


# 5. Convert categorical columns to numerical indexes
carrier_indexer = StringIndexer(
    inputCol="OP_UNIQUE_CARRIER",
    outputCol="carrier_index"
)

origin_indexer = StringIndexer(
    inputCol="ORIGIN",
    outputCol="origin_index"
)

dest_indexer = StringIndexer(
    inputCol="DEST",
    outputCol="dest_index"
)


# 6. Assemble features
assembler = VectorAssembler(
    inputCols=[
        "MONTH",
        "DAY_OF_MONTH",
        "DAY_OF_WEEK",
        "OP_CARRIER_FL_NUM",
        "CRS_DEP_TIME",
        "DISTANCE",
        "carrier_index",
        "origin_index",
        "dest_index"
    ],
    outputCol="features"
)


# 7. Machine Learning Model
lr = LogisticRegression(
    featuresCol="features",
    labelCol="DEP_DEL15",
    maxIter=20
)


# 8. Create Pipeline
pipeline = Pipeline(
    stages=[
        carrier_indexer,
        origin_indexer,
        dest_indexer,
        assembler,
        lr
    ]
)


# 9. Train/Test Split
train, test = df.randomSplit([0.8, 0.2], seed=42)

print("Training rows:", train.count())
print("Testing rows:", test.count())


# 10. Train Model
model = pipeline.fit(train)


# 11. Make Predictions
predictions = model.transform(test)


# 12. Show Predictions
predictions.select(
    "DEP_DEL15",
    "prediction",
    "probability"
).show(20, truncate=False)


# 13. Evaluate Model
evaluator = MulticlassClassificationEvaluator(
    labelCol="DEP_DEL15",
    predictionCol="prediction",
    metricName="accuracy"
)

accuracy = evaluator.evaluate(predictions)

print("=" * 50)
print("FlightIQ Spark ML Accuracy:", accuracy)
print("=" * 50)


# 14. Save trained model
model_path = "/opt/spark/work-dir/flightiq_ml_model"

model.write().overwrite().save(model_path)

print("Model saved successfully!")
print("Path:", model_path)


spark.stop()