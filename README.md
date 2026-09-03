# ✈️ FlightIQ

<div align="center">

# ✈️ FlightIQ

### Real-Time Flight Delay Detection & Prediction Platform

**A Lambda-Architecture Big Data platform combining historical batch analytics with real-time streaming and machine-learning inference.**

**Summer Training Project — Group S26-B8-BIG DATA-G8-M**  

[![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![HDFS](https://img.shields.io/badge/HDFS-Data%20Lake-66CCFF?logo=apache&logoColor=white)](https://hadoop.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboards-F2C811?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![GitHub](https://img.shields.io/badge/GitHub-Version%20Control-181717?logo=github&logoColor=white)](https://github.com/)

</div>

---

## 📌 Project Overview

**FlightIQ** is a Big Data analytics and machine-learning platform designed to analyze historical flight data, process simulated real-time flight events, and predict whether a flight will be delayed by more than **15 minutes**.

The project addresses two major challenges in flight analytics:

- **Huge historical datasets:** Flight records are large and difficult to process efficiently on a normal computer.
- **Lack of real-time intelligence:** Traditional analysis often focuses on historical data and does not provide predictions while flight events are being processed.

FlightIQ combines a **batch pipeline** for historical data with a **streaming pipeline** for live/simulated flight events. Historical data is stored and processed using HDFS and Apache Spark, while Apache Kafka and Spark Structured Streaming handle incoming flight events. A Spark MLlib model is then used to make real-time delay predictions.

The final analytics and predictions can be exposed through **PostgreSQL** and visualized using **Power BI dashboards**.

---

## 🎯 Project Goals

FlightIQ aims to:

- Process large-scale historical flight datasets.
- Clean and transform raw flight data using Spark.
- Store historical data in HDFS using efficient formats such as Parquet.
- Analyze airlines and airports based on delay and on-time performance.
- Simulate real-time flight events from historical records.
- Ingest streaming events through Apache Kafka.
- Process real-time events using Spark Streaming.
- Calculate window-based delay statistics.
- Predict whether a flight will be delayed by more than 15 minutes.
- Store processed results and predictions in PostgreSQL.
- Provide dashboards for historical and real-time analytics.

---

## 🧰 Tools & Technologies

| Category | Technology | Role in FlightIQ |
|---|---|---|
| Programming | **Python 3.9+** | Data simulation, Kafka producer, Spark application scripts |
| Big Data Processing | **Apache Spark 3.5** | Batch ETL, SQL processing, streaming, and ML |
| Stream Processing | **Spark Structured Streaming** | Real-time flight-event processing |
| Machine Learning | **Spark MLlib** | Training and inference for flight-delay prediction |
| Message Broker | **Apache Kafka** | Real-time event ingestion |
| Storage | **HDFS** | Distributed storage for raw and processed historical data |
| Database | **PostgreSQL** | SQL analytics and storage of processed results |
| Visualization | **Power BI** | Historical and real-time dashboards |
| Containerization | **Docker** | Isolated execution of the project services |
| Version Control | **Git & GitHub** | Source-code management and collaboration |

---

## 🤖 Machine Learning

### Prediction Target

The machine-learning component predicts:

> **Will the flight be delayed by more than 15 minutes?**

This is treated as a **binary classification problem**:

```text
0 → No significant delay
1 → Delay > 15 minutes
```

### Features

The proposal identifies features such as:

- Time
- Distance
- Day of the week
- Airline
- Airport

### Model

The project uses **Apache Spark MLlib** for model training, with **Random Forest** as the selected model approach.

The trained model is saved to HDFS:

```text
/models/flight_delay_model
```

The saved model is then loaded by the streaming processor so that incoming flight events can receive predictions in real time.


## 📊 Analytics

FlightIQ is designed to provide analytics including:

### Airline & Airport Performance

- On-Time Performance (OTP %)
- Average delay
- Airline rankings
- Airport rankings

---

# 📈 Data Flow

```text
                 ┌─────────────────────────┐
                 │ Historical Flight Data  │
                 └────────────┬────────────┘
                              │
                              ▼
                             HDFS
                              │
                              ▼
                       Apache Spark ETL
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
                PostgreSQL          Spark MLlib
                                        │
                                        ▼
                                  Random Forest
                                        │
                                        ▼
                                       HDFS
                                        │
                                        │
Live / Simulated Events                 │
        │                               │
        ▼                               │
Python Producer                         │
        │                               │
        ▼                               │
     Kafka ─────────────────────────────┤
        │                               │
        ▼                               ▼
Spark Structured Streaming ──────► ML Inference
        │                               │
        └──────────────┬────────────────┘
                       ▼
                  PostgreSQL
                       │
                       ▼
                    Power BI
```
---

### ML Workflow

```text
Historical Data
      ↓
Data Preparation
      ↓
Feature Engineering
      ↓
Class Balancing
      ↓
Random Forest Training
      ↓
 Trained Model
      ↓
    HDFS
      ↓
Spark Streaming
      ↓
Real-Time Predictions
```

> The project proposal also identifies class imbalance as a challenge and proposes techniques such as SMOTE to improve training-data balance.

---

# 🗂️ Project Structure

A typical project organization is:

```text
FlightIQ/
│
├── data/
│   ├── flights.py
│   └── flights-streaming.py
│
├── kafka/
│   └── producer.py
│
├── spark/
│   ├── etl_spark.py
│   ├── ml_model.py
│   └── streaming_processor.py
│
├── docker-compose.yml
│
└── README.md

```

> The exact repository structure may contain additional configuration or supporting files.

---

# 🚀 Project Setup

## Prerequisites

Make sure you have:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Git](https://git-scm.com/)
- Python 3.9+ (required for the Kafka producer)

Run the commands from the **FlightIQ project root directory**.

---

## 1. Start All Services

Start the complete Docker environment:

```bash
docker-compose up -d
```

Wait approximately **30 seconds** for all services to start.

You can verify the running containers with:

```bash
docker-compose ps
```

---

## 2. Upload Historical Data to HDFS

Copy the flight dataset into the HDFS NameNode container:

```bash
docker cp data/flights.csv flightiq-namenode:/tmp/flights.csv
```

Create the HDFS directory and upload the data:

```bash
docker exec -it flightiq-namenode bash -c "hdfs dfs -mkdir -p /raw/flights && hdfs dfs -put /opt/data/flights.csv /raw/flights/"
```

---

## 3. Run the Spark ETL Pipeline

Copy the ETL script into the Spark Master container:

```bash
docker cp spark/etl_spark.py flightiq-spark-master:/opt/spark/work-dir/etl_spark.py
```

Run the ETL job:

```bash
docker exec -it flightiq-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.5.1 /opt/spark/work-dir/etl_spark.py
```

The ETL stage prepares the historical data for downstream analytics and database operations.

---

## 4. Train the Machine Learning Model

Copy the ML training script:

```bash
docker cp spark/ml_model.py flightiq-spark-master:/opt/spark/work-dir/ml_model.py
```

Run model training:

```bash
docker exec -it flightiq-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.executor.memory=2g --conf spark.driver.memory=2g /opt/spark/work-dir/ml_model.py
```

The trained model is saved to:

```text
/models/flight_delay_model
```

The streaming processor later loads this model for real-time inference.

---

## 5. Start the Kafka Producer

Run the Python producer:

```bash
python kafka/producer.py
```

The producer reads flight records and sends them to Kafka as simulated real-time events.

---

## 6. Start the Spark Streaming Consumer

Copy the streaming processor into the Spark Master container:

```bash
docker cp spark/streaming_processor.py flightiq-spark-master:/opt/spark/work-dir/streaming_processor.py
```

Start the Spark Streaming application:

```bash
docker exec -it flightiq-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.executor.memory=1g --conf spark.driver.memory=1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.5.1 /opt/spark/work-dir/streaming_processor.py
```

---

# 🌐 Service URLs

| Service | URL / Address |
|---|---|
| Spark Master UI | [http://localhost:8080](http://localhost:8080) |
| HDFS NameNode UI | [http://localhost:9870](http://localhost:9870) |
| Kafka UI | [http://localhost:8082](http://localhost:8082) |
| PostgreSQL UI (pgAdmin) | [http://localhost:5050](http://localhost:5050) |

---

## 🔐 PostgreSQL / pgAdmin Credentials

### pgAdmin

```text
Email: admin@flightiq.com
Password: admin
```

### PostgreSQL Server Connection

```text
Username: flightiq
Password: flightiq
Database: flightiq
Host: postgres
Port: 5432
```

> These credentials are intended for the local development environment. Do not use development credentials like these in production.

---

# 🛑 Stop the Project

To stop and remove the running Docker containers:

```bash
docker-compose down
```

---

# 🛠️ Troubleshooting

## HDFS Permission Issues

If you encounter HDFS permission problems in the local development environment:

```bash
docker exec -it flightiq-namenode bash -c "hdfs dfs -chmod -R 777 /"
```

> **Warning:** `777` permissions are suitable only for local development/testing. They are not recommended for production deployments.

## Containers Are Not Ready

Check the service status:

```bash
docker-compose ps
```

If required, inspect the logs:

```bash
docker-compose logs
```

---

# 📦 Project Deliverables

The project deliverables include:

1. **Source Code** — Python and Spark scripts hosted on GitHub.
2. **Docker Environment** — `docker-compose.yml` for running the Big Data stack.
3. **Database** — PostgreSQL tables for querying processed results.
4. **Machine Learning Model** — Trained flight-delay prediction model.
5. **Dashboard** — Visual analytics for historical and real-time results.
6. **Documentation & Demonstration** — Project documentation and demonstration video.

---

# 👥 Team Members

| Name | Role |
|---|---|
| Marwan Khaled Sayed Boraiy | Team Member |
| Abdullah Hussein Mohammed Elsayed | Team Member |
| Roqaia Gamal Hosny Mohamed | Team Member |
| Basmala Atef Mohamed Darwesh | Team Member |

---

# 🔗 Project Resources

| Resource | Link |
|---|---|
| 💻 **GitHub Repository** | [FlightIQ on GitHub](https://github.com/marwan5aled26/FlightIQ) |
| 📄 **Proposal** | [View Proposal](https://drive.google.com/file/d/1cPhYUKsgLzqUnZUflCR3y9vtIh8EARWg/view?usp=drive_link) |
| 🎞️ **Presentation** | [View Presentation](https://docs.google.com/presentation/d/1I_yPc5BcK9OUNpZlc3YIbgP0hM3I_DFl/edit?usp=sharing&ouid=102283129380984698493&rtpof=true&sd=true) |
| 🎥 **Demo** | Coming Soon |

---

<div align="center">

### ✈️ FlightIQ
**Turning Flight Data into Real-Time Intelligence**

Built with Python • Apache Spark • Apache Kafka • HDFS • PostgreSQL • Docker • Power BI

</div>
