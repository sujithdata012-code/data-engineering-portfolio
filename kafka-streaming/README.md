# Real-Time Financial Event Streaming — Kafka → Spark → Delta Lake

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.5-black?style=flat-square&logo=apachekafka)
![Spark](https://img.shields.io/badge/Apache_Spark-3.4-orange?style=flat-square&logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-2.4-blue?style=flat-square)
![Docker](https://img.shields.io/badge/Docker-ready-blue?style=flat-square&logo=docker)

**High-throughput, fault-tolerant real-time pipeline ingesting ~5,000 financial
transaction events/sec from Kafka into Delta Lake with exactly-once semantics.**

</div>

---

## Architecture

```mermaid
