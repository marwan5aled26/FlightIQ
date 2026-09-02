# FlightIQ - ETL + PostgreSQL Setup

## Prerequisites

- Docker Desktop
- Git

---

## 1. Start

```bash
docker-compose up -d
```

> Wait approximately 30 seconds for all services to start.

---

## 2. Upload Data to HDFS

Copy the flights CSV file into the NameNode container:

```bash
docker cp data/flights.csv flightiq-namenode:/tmp/flights.csv
```

Create the HDFS directory and upload the data:

```bash
docker exec -it flightiq-namenode bash -c "hdfs dfs -mkdir -p /raw/flights && hdfs dfs -put /opt/data/flights.csv /raw/flights/"
```

---

## 3. Run ETL

Copy the Spark ETL script into the Spark Master container:

```bash
docker cp spark/etl_spark.py flightiq-spark-master:/opt/spark/work-dir/etl_spark.py
```

Run the ETL pipeline using Spark:

```bash
docker exec -it flightiq-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.5.1 /opt/spark/work-dir/etl_spark.py
```

---

## 4. Verify

Check that the PostgreSQL tables were created successfully:

```bash
docker exec -it flightiq-postgres psql -U flightiq -d flightiq -c "\dt"
```

Check the number of records in the cleaned flights table:

```bash
docker exec -it flightiq-postgres psql -U flightiq -d flightiq -c "SELECT COUNT(*) FROM flights_clean;"
```

### Expected Result

The `flights_clean` table should exist and contain:

```text
525370 rows
```

---

## Services

| Service | URL |
|---|---|
| Spark Master | http://localhost:8080 |
| HDFS NameNode | http://localhost:9870 |
| pgAdmin | http://localhost:5050 |
| PostgreSQL | localhost:5432 |

### pgAdmin Credentials

```text
Email: admin@flightiq.com
Password: admin
```

### PostgreSQL Credentials

```text
Username: flightiq
Password: flightiq
Database: flightiq
Host: postgres
Port: 5432
```

---

## Stop

To stop and remove the running Docker containers:

```bash
docker-compose down
```

---

## Troubleshooting

If you encounter HDFS permission issues, run:

```bash
docker exec -it flightiq-namenode bash -c "hdfs dfs -chmod -R 777 /"
```

> **Note:** Using `777` permissions is suitable for local development/testing environments but is not recommended for production deployments.

---

## Team Members

| Name | Role |
|---|---|
| Abdullah Hussein Mohammed Elsayed | Team Member |
| Roqai A Gamal Hosny Mohamed | Team Member |
| Basmala Atef Mohamed Darwesh | Team Member |
| Marwan Khaled Sayed Boraiy | Team Member |

---

## Repository

[FlightIQ on GitHub](https://github.com/marwan5aled26/FlightIQ)
