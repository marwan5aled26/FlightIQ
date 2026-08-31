# ✈️ FlightIQ

## Requirements

Make sure you have:

* Docker
* Docker Compose

---

## 1. Start Docker Containers

From the project root directory:

```bash
docker-compose up -d
```

---

## 2. Start Kafka

Create the Kafka topic:

```bash
docker exec -it flightiq-kafka kafka-topics.sh \
--create \
--topic flight-events \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

---

## 3. Upload Data to HDFS

Copy the dataset to the NameNode container:

```bash
docker cp data/flights.csv flightiq-namenode:/opt/data/
```

Create the HDFS directory and upload the data:

```bash
docker exec -it flightiq-namenode \
bash -c "hdfs dfs -mkdir -p /raw/flights && hdfs dfs -put /opt/data/flights.csv /raw/flights/"
```

---

## 4. Run Spark ETL

Copy the Spark ETL script to the Spark Master:

```bash
docker cp spark/etl_spark.py \
flightiq-spark-master:/opt/spark/work-dir/etl_spark.py
```

Run Spark:

```bash
docker exec -it flightiq-spark-master \
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
/opt/spark/work-dir/etl_spark.py
```

---

## 5. Setup Hive

Copy the Hive SQL script:

```bash
docker cp hive/create_hive_table.sql \
flightiq-hive-server:/opt/hive/scripts/create_hive_table.sql
```

Create the Hive table:

```bash
docker exec -it flightiq-hive-server \
/opt/hive/bin/hive \
-f /opt/hive/scripts/create_hive_table.sql
```

---

## 6. Run Analytical Queries

Copy the analytical queries:

```bash
docker cp hive/analytical_queries.sql \
flightiq-hive-server:/opt/hive/scripts/analytical_queries.sql
```

Run them when needed:

```bash
docker exec -it flightiq-hive-server \
/opt/hive/bin/hive \
-f /opt/hive/scripts/analytical_queries.sql
```

---

## 🛑 Stop the Project

When you're finished:

```bash
docker-compose down
```

---

## 🌐 Services

| Service          | Address                 |
| ---------------- | ----------------------- |
| Kafka            | `localhost:9092`        |
| Spark Master UI  | `http://localhost:8080` |
| HDFS NameNode UI | `http://localhost:9870` |
| Hive Server      | `localhost:10000`       |

---

## 🔄 Quick Workflow

```text
Docker
  ↓
Kafka
  ↓
HDFS
  ↓
Spark ETL
  ↓
Hive
  ↓
Analytics
```

**Run the commands in the order above to start the FlightIQ pipeline.**