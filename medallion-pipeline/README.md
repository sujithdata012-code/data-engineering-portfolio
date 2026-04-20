# Financial Data Lakehouse — Medallion Architecture

<div align="center">

![Python](https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.4-orange?style=flat-square&logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-2.4-blue?style=flat-square)
![dbt](https://img.shields.io/badge/dbt-1.6-red?style=flat-square&logo=dbt)
![Databricks](https://img.shields.io/badge/Databricks-cloud-red?style=flat-square&logo=databricks)
![Snowflake](https://img.shields.io/badge/Snowflake-cloud-blue?style=flat-square&logo=snowflake)

**A production-grade data lakehouse pipeline for financial services workloads.**
Processes raw transaction data through Bronze → Silver → Gold layers with automated
data quality validation, incremental loading, and regulatory-ready aggregations.

</div>

---

## Architecture

```mermaid
flowchart TD
    A1[CSV / Transactions] --> B
    A2[JSON / Market Data] --> B
    A3[Reference Data] --> B

    B["🟫 BRONZE LAYER
    Azure Data Factory · ADLS Gen2
    ─────────────────────────────
    Raw data landed as-is
    Metadata: timestamp + source file
    Delta Lake — full audit trail"]

    B -->|PySpark| C

    C["🥈 SILVER LAYER
    Databricks PySpark · Great Expectations
    ─────────────────────────────────────
    Type casting · Deduplication
    Data quality checks + validation
    Bad records → Quarantine table
    Delta MERGE for incremental loads"]

    C -->|dbt + PySpark| D

    D["🥇 GOLD LAYER
    dbt · Snowflake · Databricks SQL
    ────────────────────────────────
    Star schema · SCD Type 1 and 2
    Daily summaries · Customer 360
    Risk flags · Regulatory reporting
    Incremental dbt models"]

    D --> E1[Power BI Dashboards]
    D --> E2[Regulatory Reporting]
    D --> E3[ML Feature Store]

    style B fill:#8B4513,color:#fff,stroke:#5a2d0c
    style C fill:#708090,color:#fff,stroke:#4a5568
    style D fill:#B8860B,color:#fff,stroke:#7a5c0a
```

---

## Pipeline Flow

```mermaid
sequenceDiagram
    participant S as Source Systems
    participant ADF as Azure Data Factory
    participant B as Bronze (ADLS Gen2)
    participant Sp as Spark / Databricks
    participant Si as Silver (Delta Lake)
    participant dbt as dbt + Snowflake
    participant G as Gold (Delta Lake)
    participant BI as Power BI

    S->>ADF: Raw CSV / JSON / API data
    ADF->>B: Land raw files + add metadata
    B->>Sp: Read bronze Delta table
    Sp->>Sp: Cleanse, validate, deduplicate
    Sp->>Si: MERGE into silver Delta table
    Si->>dbt: Trigger dbt incremental run
    dbt->>G: Build aggregated gold models
    G->>BI: Serve curated datasets
```

---

## Project Structure

```
data-engineering-portfolio/
│
└── medallion-pipeline/
    │
    ├── bronze/
    │   └── ingest_raw.py              # Ingest raw data → Delta bronze table
    │
    ├── silver/
    │   └── transform_silver.py        # Cleanse, validate, upsert → Delta silver
    │
    ├── gold/
    │   └── build_gold.py              # Aggregations → Delta gold tables
    │
    ├── dbt_models/
    │   └── models/
    │       ├── staging/
    │       │   └── stg_transactions.sql
    │       ├── marts/
    │       │   └── fct_daily_transaction_summary.sql
    │       └── schema.yml             # Data quality tests
    │
    └── README.md
```

---

## Key Features

| Feature | How it's implemented |
|---|---|
| Incremental loads | Delta Lake MERGE + dbt incremental models |
| Data quality | Great Expectations + dbt schema tests |
| Bad record handling | Quarantine table — records never silently dropped |
| Idempotency | All pipelines are safe to re-run |
| Schema evolution | `mergeSchema: true` on all Delta writes |
| Full audit trail | Bronze immutability + Delta time travel |
| Compliance | RBAC + column masking + data lineage |

---

## Scale & Performance

| Metric | Value |
|---|---|
| Daily data volume | ~1 TB / day |
| Datasets processed | 40+ |
| Query latency improvement | 25% (Hadoop → Databricks migration) |
| dbt test coverage | 18 / 18 passing |
| Bad record rate | < 0.5% (quarantined, not dropped) |

---

## Quickstart

```bash
# Clone the repo
git clone https://github.com/sujithdata012-code/data-engineering-portfolio.git
cd medallion-pipeline

# Install dependencies
pip install pyspark delta-spark great-expectations dbt-snowflake

# Run the pipeline layers in order
python bronze/ingest_raw.py
python silver/transform_silver.py
python gold/build_gold.py

# Run dbt models and tests
cd dbt_models
dbt run
dbt test
```

---

## Tech Stack

| Layer | Tools |
|---|---|
| Ingestion | Azure Data Factory, Python |
| Processing | PySpark, Databricks |
| Storage | Delta Lake, ADLS Gen2 |
| Transformation | dbt, Snowflake |
| Orchestration | Apache Airflow |
| Data Quality | Great Expectations |
| CI/CD | Azure DevOps, Terraform, Docker |
| Visualization | Power BI |

---

## Author

**Sujith Reddy** — Data Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=flat-square&logo=linkedin)](https://www.linkedin.com/in/sujith-reddy-manne)
[![Email](https://img.shields.io/badge/Email-Contact-red?style=flat-square&logo=gmail)](mailto:sujith.data012@gmail.com)

AWS Certified Solutions Architect · M.S. Computer Science (GPA 3.9/4.0)
