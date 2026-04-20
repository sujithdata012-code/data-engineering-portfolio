# Financial Data Lakehouse — Medallion Architecture (Bronze → Silver → Gold)

> **Tech Stack:** PySpark · Databricks · Delta Lake · dbt · Azure Data Factory · ADLS Gen2 · Airflow · Great Expectations

A production-grade, cloud-native data lakehouse pipeline built for financial services workloads. Processes raw transaction data through a Bronze → Silver → Gold medallion architecture with data quality validation, incremental loading, and business-ready aggregations powering risk dashboards and regulatory reporting.

---

## Architecture
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │
│  │  Transactions │  │ Market Data  │  │  Reference   │                  │
│  │  (CSV / API) │  │   (JSON)     │  │   Data       │                  │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                  │
└─────────┼─────────────────┼─────────────────┼────────────────────────── ┘
│                 │                 │
▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER  (Raw / Immutable)                       │
│                    Azure Data Factory  ·  ADLS Gen2                      │
│  • Raw data landed as-is — never modified                                │
│  • Added metadata: _ingestion_timestamp, _source_file                    │
│  • Delta Lake format — full audit trail + time travel                    │
└──────────────────────────┬──────────────────────────────────────────────┘
│  PySpark
▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER  (Cleansed / Conformed)                  │
│                    Databricks (PySpark)  ·  Great Expectations           │
│  • Type casting, string standardization, deduplication                   │
│  • Data quality checks: nulls, range validation, referential integrity   │
│  • Bad records quarantined — never silently dropped                      │
│  • Delta MERGE (upsert) for idempotent incremental loads                 │
└──────────────────────────┬──────────────────────────────────────────────┘
│  dbt + PySpark
▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER  (Business-Ready)                          │
│                    dbt  ·  Snowflake  ·  Databricks SQL                  │
│  • Dimensional models: star schema, SCD Type 1/2                         │
│  • Aggregations: daily summaries, customer 360, risk flags               │
│  • Incremental dbt models — only process new data                        │
│  • Surrogate keys via dbt_utils                                          │
└──────────────────────────┬──────────────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    CONSUMPTION LAYER                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │
│  │   Power BI   │  │  Regulatory  │  │  ML Feature  │                  │
│  │  Dashboards  │  │  Reporting   │  │    Store     │                  │
│  └──────────────┘  └──────────────┘  └──────────────┘                  │
└─────────────────────────────────────────────────────────────────────────┘
Orchestration: Apache Airflow  |  IaC: Terraform  |  CI/CD: Azure DevOps

---

## Project Structure
medallion-pipeline/
├── bronze/
│   └── ingest_raw.py          # Raw ingestion → Delta bronze
├── silver/
│   └── transform_silver.py    # Cleanse, validate, upsert → Delta silver
├── gold/
│   └── build_gold.py          # Aggregations → Delta gold
├── dbt_models/
│   └── models/
│       ├── staging/
│       │   └── stg_transactions.sql
│       ├── marts/
│       │   └── fct_daily_transaction_summary.sql
│       └── schema.yml         # dbt tests + source definitions
├── tests/
│   └── test_transformations.py
└── requirements.txt

---

## Key Features

| Feature | Implementation |
|---|---|
| Incremental loads | Delta Lake MERGE + dbt incremental models |
| Data quality | Great Expectations + dbt schema tests |
| Bad record handling | Quarantine table — never silent drops |
| Idempotency | All pipelines safe to re-run |
| Schema evolution | `mergeSchema: true` on all Delta writes |
| Audit trail | Bronze immutability + Delta time travel |
| SCD Type 2 | dbt snapshots for slowly changing dimensions |
| Regulatory compliance | RBAC + column masking + full lineage |

---

## Quickstart

```bash
# 1. Clone the repo
git clone https://github.com/sujithdata012-code/data-engineering-portfolio.git
cd medallion-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run bronze ingestion
python bronze/ingest_raw.py

# 4. Run silver transformation
python silver/transform_silver.py

# 5. Run gold aggregation
python gold/build_gold.py

# 6. Run dbt models
cd dbt_models
dbt run
dbt test
```

---

## Data Quality Results (Sample Run)
Bronze ingested:  1,250,000 records
Silver valid:     1,243,817 records  (99.5%)
Silver quarantined:   6,183 records  (0.5%)
Gold daily summary:   2,847 rows
Gold customer 360:   89,234 customers
dbt tests passed:        18 / 18

---

## Production Notes

- **Scale tested:** ~1TB/day across 40+ datasets (Citi production environment)
- **Cloud:** Azure ADLS Gen2 + Databricks (swap paths for AWS S3 + EMR)
- **Orchestration:** Designed for Apache Airflow DAG orchestration
- **IaC:** Terraform configs available in `/infra` for full Azure deployment

---

## Author

**Sujith Reddy** — Data Engineer
[LinkedIn](https://www.linkedin.com/in/sujith-reddy-manne) · sujith.data012@gmail.com
AWS Certified Solutions Architect · M.S. Computer Science (GPA 3.9)
