# FlightIQ - Local Setup Guide

## Prerequisites

| Software | Version |
|---|---|
| Docker Desktop | Latest |
| Git | Latest |
| Python | 3.8+ |

---

## Step 1: Clone Repository

```bash
git clone https://github.com/marwan5aled26/FlightIQ.git
cd FlightIQ
```

---

## Step 2: Start Docker Containers

```bash
docker-compose up -d
```

Wait 30 seconds for all services to start.

---

## Step 3: Create Kafka Topic

```bash
docker exec -it flightiq-kafka kafka-topics.sh --create --topic flight-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

---

## Step 4: Upload Data to HDFS

```bash
docker cp data/flights.csv flightiq-namenode:/opt/data/
docker exec -it flightiq-namenode bash -c "hdfs dfs -mkdir -p /raw/flights && hdfs dfs -put /opt/data/flights.csv /raw/flights/"
```

---

## Step 5: Run Spark ETL

```bash
docker cp spark/etl_spark.py flightiq-spark-master:/opt/spark/work-dir/etl_spark.py
docker exec -it flightiq-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.5.1 /opt/spark/work-dir/etl_spark.py
```

---

## Step 6: Verify Data in PostgreSQL

```bash
docker exec -it flightiq-postgres psql -U flightiq -d flightiq -c "SELECT COUNT(*) FROM flights_clean;"
```

Expected output:

```text
525370
```

---

## Step 7: Run Kafka Producer

```bash
pip install kafka-python
python kafka/producer.py
```

---

## Step 8: Run Spark Streaming

```bash
docker cp spark/streaming_processor.py flightiq-spark-master:/opt/spark/work-dir/streaming_processor.py
docker exec -it flightiq-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark/work-dir/streaming_processor.py
```

---

## Access Services

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8082 |
| Spark Master | http://localhost:8080 |
| HDFS NameNode | http://localhost:9870 |
| pgAdmin | http://localhost:5050 |
| PostgreSQL | localhost:5432 |

---

## pgAdmin Login

- **Email:** `admin@flightiq.com`
- **Password:** `admin`

Then add a server with the following settings:

| Setting | Value |
|---|---|
| Host | `postgres` |
| Port | `5432` |
| Database | `flightiq` |
| Username | `flightiq` |
| Password | `flightiq` |

---

## Stop Everything

```bash
docker-compose down
```

---

## Troubleshooting

### PostgreSQL connection refused in pgAdmin

Use `postgres` as the host, not `localhost`.

### Permission denied in HDFS

```bash
docker exec -it flightiq-namenode bash -c "hdfs dfs -chmod -R 777 /"
```

### Kafka topic not found

```bash
docker exec -it flightiq-kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Spark cannot find PostgreSQL driver

The `--packages org.postgresql:postgresql:42.5.1` flag handles this automatically.

---

## Team Members

| Name | Role |
|---|---|
| Abdullah Hussein Mohammed Elsayed | ETL + Hive |
| Roqai A Gamal Hosny Mohamed | Kafka + Producer |
| Basmala Atef Mohamed Darwesh | ML + Streaming |
| Marwan Khaled Sayed Boraiy | Power BI + Dashboard |

---

## Repository

https://github.com/marwan5aled26/FlightIQ
