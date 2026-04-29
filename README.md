# Amazon E-Commerce Analytics Data Pipeline

End-to-end data engineering pipeline processing 1M+ daily e-commerce events using AWS big data stack.

## Architecture

```
Kinesis Data Streams → Lambda (real-time enrichment)
                            ↓
                    S3 Raw Zone
                      ↓           ↓
              Glue ETL Jobs    EMR Spark (ML features)
                      ↓               ↓
                  Redshift (warehouse) → BI Dashboards
                      ↓
                  RDS (metadata)
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Kinesis Data Streams, Kinesis Firehose |
| Orchestration | Apache Airflow (MWAA) |
| Processing | AWS Glue, EMR (Spark/Hive), Lambda |
| Storage | S3 (Data Lake), Redshift, RDS |
| Modeling | Star Schema, Redshift Spectrum |
| Language | Python, PySpark, HiveQL, SQL |

## Key Features

- **Real-time**: Sub-second event processing via Kinesis + Lambda
- **Batch ETL**: Glue jobs with partitioning & compaction
- **Big Data**: EMR Spark cluster for ML feature engineering
- **Warehousing**: Redshift star schema with 5 fact & 8 dimension tables
- **Orchestration**: Airflow DAGs with retries, SLA monitoring, lineage
- **Scalability**: Processes 1M+ events/day, auto-scales EMR

## Project Structure

```
├── dags/                  # Airflow DAGs
├── glue_jobs/             # Glue ETL scripts
├── lambda_functions/      # Real-time processors
├── emr_scripts/           # Spark/Hive jobs
├── sql/                   # Redshift DDL & modeling
├── data/                  # Sample datasets
├── infra/                 # Terraform / CloudFormation
└── tests/                 # Unit & integration tests
```
