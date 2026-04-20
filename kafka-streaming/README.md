# Real-Time Financial Event Streaming — Kafka → Spark → Delta Lake

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.5-black?style=flat-square&logo=apachekafka)
![Spark](https://img.shields.io/badge/Apache_Spark-3.4-orange?style=flat-square&logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-2.4-blue?style=flat-square)
![Docker](https://img.shields.io/badge/Docker-ready-blue?style=flat-square&logo=docker)

**High-throughput, fault-tolerant real-time pipeline ingesting ~5,000 financial transaction events/sec from Kafka into Delta Lake with exactly-once semantics.**

</div>

---

## Architecture

```mermaid
flowchart TD
    A[Payment Systems] --> K
    B[Trading Platforms] --> K
    C[ATM Networks] --> K

    K["APACHE KAFKA
    Topic: financial-transactions
    Partitions: 6
    Throughput: ~5,000 events/sec
    Compression: Snappy
    Acks: all — zero data loss"]

    K -->|readStream| SP

    SP["SPARK STRUCTURED STREAMING
    Parse JSON payload
    Risk classification: HIGH / MEDIUM / LOW
    Late arrival detection via watermarking
    Trigger: every 2 seconds"]

    SP --> S1
    SP --> S2

    S1["STREAM 1 — Raw Events
    Delta Lake append mode
    Full audit trail and replay"]

    S2["STREAM 2 — Windowed Aggregates
    5-minute tumbling windows
    Per currency and risk tier"]

    S1 --> D1[Risk Dashboards]
    S2 --> D2[Fraud Alerts]
    S2 --> D3[Regulatory Feeds]

    style K fill:#1a1a2e,color:#fff,stroke:#e94560
    style SP fill:#16213e,color:#fff,stroke:#0f3460
    style S1 fill:#0f3460,color:#fff,stroke:#533483
    style S2 fill:#0f3460,color:#fff,stroke:#533483
```

---

## Pipeline Flow

```mermaid
sequenceDiagram
    participant P as Kafka Producer
    participant K as Kafka Topic
    participant SP as Spark Streaming
    participant D1 as Delta Lake Raw
    participant D2 as Delta Lake Aggregates
    participant DB as Dashboards

    P->>K: Publish ~5,000 events/sec
    K->>SP: readStream 10,000 offsets per trigger
    SP->>SP: Parse JSON and Risk classify
    SP->>D1: Append enriched events every 2 seconds
    SP->>D2: Write 5-min window aggregates
    D2->>DB: Feed real-time risk dashboards
```

---

## Project Structure

```
kafka-streaming/
│
├── producer/
│   └── transaction_producer.py     # Simulates ~5,000 events/sec
│
├── spark_streaming/
│   └── streaming_pipeline.py       # Kafka → Spark → Delta Lake
│
├── config/
│   └── docker-compose.yml          # Local Kafka + Zookeeper + UI
│
└── README.md
```

---

## Key Features

| Feature | Detail |
|---|---|
| Throughput | ~5,000 events/sec |
| Fault tolerance | Checkpointing — restarts from last offset |
| Exactly-once | Delta Lake idempotent writes |
| Late data handling | 10-minute watermark |
| Risk classification | HIGH / MEDIUM / LOW tiers inline |
| Windowed aggregates | 5-minute tumbling windows per currency |
| Local dev | Full Docker Compose setup included |

---

## Scale and Performance

| Metric | Value |
|---|---|
| Events per second | ~5,000 |
| Micro-batch trigger | Every 2 seconds |
| Watermark for late data | 10 minutes |
| Window size | 5 minutes |
| Kafka partitions | 6 |

---

## Quickstart

```bash
# 1. Start Kafka locally using Docker
cd config
docker-compose up -d

# Kafka UI available at http://localhost:8080

# 2. Install dependencies
pip install pyspark delta-spark kafka-python

# 3. Start the Spark consumer first
python spark_streaming/streaming_pipeline.py

# 4. In a new terminal start the producer
python producer/transaction_producer.py
```

---

## Sample Output

```
Stream 1 — Raw enriched events:
transaction_id        customer_id    amount     risk_tier
a3f2c1d4-...          CUST_52841     142.50     LOW
b8e1a2f3-...          CUST_71023     10500.00   HIGH

Stream 2 — 5-minute windowed aggregates:
window              currency   risk_tier   count    total_amount
10:00 to 10:05      USD        LOW         12,847   847,293.22
10:00 to 10:05      USD        HIGH           203   2,847,500.00
```

---

## Tech Stack

| Component | Tool |
|---|---|
| Message broker | Apache Kafka 3.5 |
| Stream processing | Spark Structured Streaming |
| Storage | Delta Lake |
| Language | Python and PySpark |
| Local setup | Docker and Docker Compose |
| Monitoring | Kafka UI at localhost:8080 |

---

## Author

**Sujith Reddy Manne** — Senior Data Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=flat-square&logo=linkedin)](https://www.linkedin.com/in/sujith-reddy-manne)
[![Email](https://img.shields.io/badge/Email-Contact-red?style=flat-square&logo=gmail)](mailto:sujith.data012@gmail.com)

AWS Certified Solutions Architect · M.S. Computer Science (GPA 3.9/4.0)
