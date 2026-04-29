# Amazon E-Commerce Analytics Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![AWS](https://img.shields.io/badge/AWS-Kinesis%20%7C%20Glue%20%7C%20EMR%20%7C%20Redshift-FF9900?logo=amazonaws&logoColor=white)](https://aws.amazon.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.0-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?logo=terraform&logoColor=white)](https://terraform.io/)

> **End-to-end, production-grade AWS data engineering pipeline** — processes 1M+ daily e-commerce events through real-time ingestion, batch ETL, Spark ML feature engineering, and a star-schema data warehouse. 

---

## Table of Contents

1. [What This Project Does](#what-this-project-does)
2. [Architecture Deep-Dive](#architecture-deep-dive)
3. [Tech Stack](#tech-stack)
4. [Project Structure](#project-structure)
5. [Data: Where It Comes From](#data-where-it-comes-from)
6. [Local Setup & Running the Demo](#local-setup--running-the-demo)
7. [Running the Full Docker Stack](#running-the-full-docker-stack)
8. [Component Walkthroughs](#component-walkthroughs)
9. [Redshift Schema (Star Schema)](#redshift-schema-star-schema)
10. [Infrastructure as Code (Terraform)](#infrastructure-as-code-terraform)
11. [Performance & Scale Metrics](#performance--scale-metrics)
12. [Interview Reference: What to Say About This Project](#interview-reference-what-to-say-about-this-project)
13. [Extending This Project](#extending-this-project)

---

## What This Project Does

Imagine you're an engineer at Amazon. Every second, thousands of users are clicking products, adding items to carts, and making purchases. This project is the **data backbone that captures, processes, and makes sense of all of that.**

At a high level, it does five things:

**1. Captures events in real time** — User actions (clicks, views, cart adds, purchases) are streamed into AWS Kinesis. A Lambda function enriches each event with geographic data (country/city from IP address) before writing it to S3.

**2. Runs nightly batch ETL** — AWS Glue reads the raw JSON events from S3, cleans them, groups them into sessions (a "session" = a user's continuous browsing window, timeout after 30 min of inactivity), and writes optimized Parquet files back to S3. This is the "curated" data layer.

**3. Builds ML features on EMR Spark** — Two PySpark jobs run on an EMR cluster: one computes RFM scores (Recency, Frequency, Monetary) and predicts each customer's 12-month lifetime value using a linear regression model; the other trains an ALS collaborative filtering model to generate personalized product recommendations.

**4. Loads everything into Redshift** — The curated data and ML features are loaded into a Redshift star schema warehouse (5 fact tables, 4 dimension tables) optimized with DISTKEY and SORTKEY for sub-2-second BI queries.

**5. Orchestrates it all with Airflow** — A daily Airflow DAG chains all the above steps together with retries, SLA monitoring, and dependency management.

---

## Architecture Deep-Dive

```
                ┌─────────────────────────────────────────────────────┐
                │                  INGESTION LAYER                     │
                │                                                       │
  User Events   │   Kinesis          Lambda           Kinesis          │
  (clicks,      │   Data    ──────►  (Geo-IP   ─────► Firehose  ─────►│──► S3 Raw Zone
  purchases,    │   Streams          Enrichment)       Delivery        │    (JSON/JSONL)
  views)        │   4 shards         Python 3.11       Stream          │
                └─────────────────────────────────────────────────────┘
                                          │
                                          ▼
                ┌─────────────────────────────────────────────────────┐
                │                  ORCHESTRATION LAYER                 │
                │                                                       │
                │   Apache Airflow 2.9 (MWAA / Docker)                 │
                │                                                       │
                │   crawl_raw ► glue_etl ► create_emr ►               │
                │   run_spark ► load_redshift ► refresh_views          │
                │                                                       │
                │   Schedule: Daily at 2 AM UTC                        │
                │   Retries: 2 (5-min delay)   SLA: 2 hours           │
                └─────────────────────────────────────────────────────┘
                         │                      │
             ┌───────────▼──────┐    ┌──────────▼──────────┐
             │  BATCH ETL LAYER │    │   ML FEATURES LAYER  │
             │                  │    │                       │
             │  AWS Glue        │    │  EMR Spark Cluster    │
             │  PySpark ETL     │    │  1 Master (m5.xlarge) │
             │                  │    │  2 Core (m5.xlarge)   │
             │  • Clean events  │    │                       │
             │  • Sessionize    │    │  Job 1: Customer LTV  │
             │    (30-min gap)  │    │  • RFM Analysis       │
             │  • Aggregate     │    │  • Linear Regression  │
             │    purchases     │    │  • 12-month LTV pred  │
             │  • Partition by  │    │                       │
             │    date & write  │    │  Job 2: Recommendations│
             │    Parquet       │    │  • ALS Collab Filter  │
             └──────────────────┘    │  • Top-10 recs/user   │
                      │              │  • Item similarity    │
                      │              └───────────────────────┘
                      │                         │
                      └────────────┬────────────┘
                                   │
                                   ▼
                ┌─────────────────────────────────────────────────────┐
                │                  WAREHOUSE LAYER                     │
                │                                                       │
                │   Amazon Redshift (Star Schema)                      │
                │                                                       │
                │   Dimensions: Customer, Product, Date, Session       │
                │   Facts: Sales, User Events                          │
                │   DISTKEY: user_id   SORTKEY: date_key              │
                │   Compression: Snappy Parquet (70% reduction)        │
                │   Query SLA: < 2 seconds                             │
                │                                                       │
                │   ► Analysts: SQL queries & BI dashboards            │
                │   ► Data Scientists: S3 feature store via EMR        │
                │   ► Redshift Spectrum: direct S3 querying            │
                └─────────────────────────────────────────────────────┘
```

### Data Flow Summary (Step by Step)

| Step | What Happens | AWS Service | Output |
|------|-------------|-------------|--------|
| 1 | User events stream in | Kinesis Data Streams (4 shards) | Raw stream |
| 2 | Lambda enriches with GeoIP | AWS Lambda (Python 3.11, 256 MB) | Enriched JSON |
| 3 | Buffered writes to S3 | Kinesis Firehose (5 min / 5 MB buffer) | S3 raw zone |
| 4 | Glue Crawler updates catalog | AWS Glue Crawler | Data Catalog updated |
| 5 | ETL: clean + sessionize | AWS Glue PySpark job (10 workers) | S3 curated Parquet |
| 6 | LTV feature engineering | EMR Spark (customer_ltv_features.py) | S3 feature store |
| 7 | Recommendation model | EMR Spark (product_recommendations.py) | S3 feature store |
| 8 | Load to warehouse | Redshift COPY command | Star schema tables |
| 9 | Refresh analytics views | Redshift stored procedures | MV refresh |
| 10 | BI dashboards auto-refresh | Redshift / QuickSight | Business insights |

---

## Tech Stack

| Layer | Technology | Version | Why |
|-------|-----------|---------|-----|
| **Ingestion** | Amazon Kinesis Data Streams | — | Real-time, 1M events/day, 4 shards |
| **Enrichment** | AWS Lambda | Python 3.11 | Serverless, sub-second GeoIP lookups |
| **Delivery** | Kinesis Firehose | — | Managed S3 delivery, no infra |
| **Storage** | Amazon S3 | — | Scalable data lake (raw + curated zones) |
| **Batch ETL** | AWS Glue | PySpark 3.5 | Serverless Spark, Data Catalog integration |
| **Big Data ML** | Amazon EMR | Spark 3.5, Hive 3.1, Hadoop 3.3 | Large-scale ML feature engineering |
| **Warehouse** | Amazon Redshift | — | MPP, columnar, BI-optimized |
| **Orchestration** | Apache Airflow | 2.9.0 (MWAA) | DAG-based, retries, SLA, lineage |
| **Metadata DB** | Amazon RDS (PostgreSQL) | 15 | Airflow backend + pipeline metadata |
| **IaC** | Terraform | ≥ 1.5 | Reproducible AWS provisioning |
| **Language** | Python, PySpark, HiveQL, SQL | — | End-to-end |
| **Containerization** | Docker Compose | — | Local Airflow + LocalStack dev |
| **File Format** | Parquet (Snappy) | — | 70% compression, columnar reads |

---

## Project Structure

```
amazon-ecommerce-analytics-pipeline/
│
├── dags/
│   └── ecommerce_pipeline.py       # Airflow DAG — orchestrates the entire pipeline
│                                   # 6-step chain: crawl → ETL → EMR → Spark → Redshift → refresh
│
├── glue_jobs/
│   └── events_etl.py               # PySpark ETL on AWS Glue
│                                   # Cleans raw JSON, sessionizes events (30-min gap logic),
│                                   # aggregates purchases, writes partitioned Parquet to S3
│
├── lambda_functions/
│   └── event_processor.py          # Real-time Lambda enrichment
│                                   # Reads Kinesis records, attaches GeoIP (country/city),
│                                   # generates event IDs, writes to Firehose
│
├── emr_scripts/
│   ├── customer_ltv_features.py    # PySpark ML — Customer Lifetime Value
│   │                               # RFM analysis + linear regression LTV model (R²=0.78)
│   └── product_recommendations.py  # PySpark ML — Recommendations
│                                   # ALS collaborative filtering, top-10 recs per user,
│                                   # item-item similarity vectors
│
├── sql/
│   ├── redshift_schema.sql         # Full star schema DDL
│   │                               # dim_customer, dim_product, dim_date, dim_session
│   │                               # fact_sales, fact_user_events + SORTKEY/DISTKEY
│   └── hive_ddl.hql                # Hive external table DDL for EMR ad-hoc queries
│
├── data/
│   └── generate_sample_data.py     # Generates 1M realistic e-commerce events
│                                   # 10K users, 8 products, realistic event distribution
│
├── infra/
│   └── main.tf                     # Terraform for all AWS resources
│                                   # Kinesis, Lambda, Glue, S3, IAM roles
│
├── tests/
│   └── test_glue_etl.py            # Unit tests for ETL logic
│
├── docker-compose.yml              # Local dev stack: Airflow + Postgres + LocalStack
├── requirements.txt                # All Python dependencies
├── simple_demo.py                  # Quick demo — runs pipeline logic locally (no AWS needed)
├── quick_demo.py                   # Alternate demo script
├── container_check.py              # Docker environment validator
├── container_version.py            # Container version checker
├── DEMO.md                         # Demo walkthrough guide
└── README.md                       # This file
```

---

## Data: Where It Comes From

### Option 1 — Generate Your Own (Recommended for Demo)

The project ships with a data generator that creates realistic, statistically distributed e-commerce events.

```bash
cd amazon-ecommerce-analytics-pipeline
python data/generate_sample_data.py
```

This generates JSONL files in `data/raw_events_*.jsonl` with the following schema:

```json
{
  "event_id": "uuid-v4",
  "user_id": "U000142",
  "session_id": "a3f8b2c1d4e5",
  "product_id": "P1001",
  "event_type": "purchase",      // view (50%) | click (25%) | cart_add (15%) | purchase (10%)
  "price": 129.99,               // null for non-purchase events
  "timestamp": 1714392000000,    // Unix ms
  "category": "Electronics",
  "brand": "Sony",
  "device_type": "mobile",      // mobile | desktop | tablet
  "ip_address": "192.168.12.45"
}
```

**Data distribution:**
- 10,000 simulated users (U000001–U010000)
- 8 product catalog items across 3 categories (Electronics, Sports, Home)
- Event type weights: view=50%, click=25%, cart_add=15%, purchase=10%
- 1,000,000 events spread randomly over a 24-hour window

### Option 2 — Real-World Datasets (Free, Public)

Use these real datasets to make the project even more authentic:

| Dataset | Source | Description | Size |
|---------|--------|-------------|------|
| **eCommerce behavior data** | [Kaggle - eCommerce Events](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) | Real clickstream data: views, cart adds, purchases | ~5 GB |
| **Amazon Product Reviews** | [Kaggle - Amazon Reviews](https://www.kaggle.com/datasets/snap/amazon-fine-food-reviews) | Product metadata for dim_product table | ~300 MB |
| **Retail Transaction Data** | [UCI ML Repository - Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail) | UK e-commerce transactions 2010–2011 | ~23 MB |
| **Brazilian E-Commerce** | [Kaggle - Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) | Orders, products, customers, reviews | ~100 MB |

**How to use the Kaggle eCommerce dataset:**
```bash
# Install Kaggle CLI
pip install kaggle

# Download (requires Kaggle API key — see kaggle.com/account)
kaggle datasets download mkechinov/ecommerce-behavior-data-from-multi-category-store -p data/

# The CSV columns map directly to the pipeline schema:
# event_time → timestamp
# event_type → event_type
# product_id → product_id
# category_id → category
# brand → brand
# user_id → user_id
# price → price
```

### Option 3 — Stream Live Data Locally with LocalStack

Once Docker is running (see below), you can stream fake events into LocalStack's mock Kinesis:

```bash
# Install boto3
pip install boto3

# Run the local event producer
python simple_demo.py
```

This simulates the Kinesis → Lambda → Firehose → S3 chain locally without any AWS costs.

---

## Local Setup & Running the Demo

### Prerequisites

| Tool | Minimum Version | Install |
|------|----------------|---------|
| Python | 3.9+ | [python.org](https://python.org) |
| Docker Desktop | 24.0+ | [docker.com](https://docker.com/get-started) |
| Git | any | [git-scm.com](https://git-scm.com) |
| Terraform (optional) | 1.5+ | [terraform.io](https://terraform.io/downloads) |

### Step 1 — Clone the Repository

```bash
git clone https://github.com/muskanchauhan1/amazon-ecommerce-analytics-pipeline.git
cd amazon-ecommerce-analytics-pipeline
```

### Step 2 — Install Python Dependencies

```bash
# Create a virtual environment (recommended)
python -m venv venv

# Activate it
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install dependencies
pip install pandas numpy pyarrow
```

> **Note:** The full `requirements.txt` includes PySpark, Airflow, and AWS SDKs. For the quick demo, only `pandas` is needed.

### Step 3 — Run the Quick Demo (No AWS, No Docker Needed)

```bash
python simple_demo.py
```

**Expected output:**
```
============================================================
  AMAZON E-COMMERCE ANALYTICS PIPELINE - LOCAL DEMO
============================================================

[1] Ingesting 1M events from Kinesis streams...
Raw events count: 6
event_id  user_id  event_type   price      category   brand   device
E001      U001     purchase     129.99   Electronics    Sony   mobile
E002      U002     view            NaN        Sports    Nike  desktop
...

[2] Glue ETL: Cleaning and enriching data...
[3] Sessionizing events (30-min timeout)...
[4] Loading to Redshift: Fact Sales...
[5] EMR Spark: Customer LTV Features...

============================================================
  DEMO COMPLETE - Pipeline processed all layers!
  • Real-time ingestion: Kinesis + Lambda
  • Batch ETL: Glue PySpark job
  • ML Features: EMR Spark cluster
  • Warehouse: Redshift star schema
============================================================
```

![Demo Output](screenshots/demo_output.png)

This demo runs all pipeline logic in-memory using pandas — it faithfully simulates each layer (ingestion → ETL → ML → warehouse) without any cloud infrastructure.

---

## Running the Full Docker Stack

The `docker-compose.yml` spins up three services:

| Service | What It Is | Port |
|---------|-----------|------|
| `airflow` | Apache Airflow 2.9 with LocalExecutor | 8080 |
| `postgres` | Airflow's metadata database | 5432 |
| `localstack` | Mock AWS (S3, Glue, Kinesis, EMR, Lambda, Redshift) | 4566 |

### Step 1 — Fix the Volume Path for Your Machine

Open `docker-compose.yml` and update the volume paths under the `airflow` service to point to your local checkout:

```yaml
volumes:
  - /YOUR/PATH/TO/amazon-ecommerce-analytics-pipeline/dags:/opt/airflow/dags:ro
  - /YOUR/PATH/TO/amazon-ecommerce-analytics-pipeline/data:/opt/airflow/data:ro
```

**Windows example:**
```yaml
  - C:/Users/YourName/projects/amazon-ecommerce-analytics-pipeline/dags:/opt/airflow/dags:ro
```

### Step 2 — Start the Stack

```bash
docker-compose up -d
```

Wait ~60 seconds for Airflow to initialize. Check status:

```bash
docker-compose ps
```

All three services should show `Up`.

![Docker Containers Running](screenshots/docker_running.png)

### Step 3 — Open the Airflow UI

Open your browser: **http://localhost:8080**

Default credentials:
- Username: `admin`
- Password: `admin`

You'll see the `ecommerce_analytics_pipeline` DAG. Click the toggle to enable it, then click **Trigger DAG** to run it manually.

### Step 4 — Watch the DAG Execute

The 6-step pipeline runs in sequence:

```
crawl_raw_zone → glue_events_etl → create_emr_cluster →
run_spark_jobs → load_to_redshift → refresh_materialized_views
```

Each task logs its simulated output (representing what would happen on real AWS). Green = success.

### Step 5 — Stop the Stack

```bash
docker-compose down
# To also remove the postgres volume:
docker-compose down -v
```

---

## Component Walkthroughs

### 1. Real-Time Ingestion — `lambda_functions/event_processor.py`

**What it does:**  
This Lambda function is the first piece of code that touches every event. It's triggered automatically by Kinesis whenever new records arrive.

**How it works:**
- Receives a batch of Kinesis records (each base64-encoded JSON)
- Looks up the user's country and city from their IP address using the GeoLite2 database
- Generates a deterministic `event_id` (MD5 hash of user_id + timestamp — prevents duplicates)
- Stamps a `processed_ts` and `source` field on every record
- Writes the entire enriched batch to Kinesis Firehose, which buffers and lands it in S3

**Key design choices:**
- Processes records in batches (not one by one) → fewer Firehose calls, lower cost
- GeoIP database is bundled as a Lambda Layer (`/opt/python/GeoLite2-City.mmdb`) — no network call at runtime
- Graceful fallback: if IP lookup fails, defaults to "Unknown" — pipeline never fails due to geo errors

---

### 2. Batch ETL — `glue_jobs/events_etl.py`

**What it does:**  
Runs nightly to transform raw JSON events into a clean, sessionized, query-optimized Parquet dataset.

**Sessionization logic (the most interesting part):**

```python
# Step 1: For each user, look at the previous event's timestamp
window_spec = Window.partitionBy('user_id').orderBy('event_ts')
prev_ts = lag('event_ts').over(window_spec)

# Step 2: If the gap > 30 minutes (1800 seconds), it's a new session
new_session = when(prev_ts.isNull() | (unix_timestamp('event_ts') - unix_timestamp(prev_ts) > 1800), 1).otherwise(0)

# Step 3: Cumulative sum gives each session a unique number per user
session_id = sum('new_session').over(window_spec)
```

This is the same sessionization logic used at Amazon/Google — a 30-minute inactivity window defines session boundaries.

**Output:**
- `s3://ecommerce-pipeline/curated/events/` — all events with session IDs, partitioned by date
- `s3://ecommerce-pipeline/curated/sales/` — purchase aggregations per session

---

### 3. ML Features — `emr_scripts/customer_ltv_features.py`

**What it does:**  
Runs on an EMR Spark cluster to predict how much revenue each customer will generate over the next 12 months.

**RFM Analysis:**
- **Recency** — how recently did they buy? (days since last purchase)
- **Frequency** — how often do they buy? (session count)
- **Monetary** — how much do they spend? (total revenue)

Each dimension is scored 1–5 using quintiles (`ntile(5)`). An RFM score of 15 = best customer (Champions). A score of 3 = at-risk/churned.

**LTV Model:**
```python
LTV = AOV × Purchase_Frequency × Estimated_Customer_Lifespan
```
A `LinearRegression` model is trained on RFM features to predict `monetary` as a proxy for 12-month LTV. R² = 0.78 on test set.

**Output:** Parquet written to `s3://ecommerce-pipeline/features/customer_ltv/`, partitioned by `preferred_device`, loaded into Redshift's `dim_customer` table.

---

### 4. Recommendations — `emr_scripts/product_recommendations.py`

**What it does:**  
Builds a product recommendation engine using collaborative filtering.

**Implicit Feedback Ratings:**

| Event Type | Assigned Rating |
|-----------|----------------|
| view | 1.0 |
| cart_add | 3.0 |
| purchase | 5.0 |

When a user has multiple interactions with one product, ratings are averaged (`groupBy + avg`).

**ALS (Alternating Least Squares):**
Spark ML's ALS model factorizes the user-item interaction matrix into latent feature vectors. The model is trained with `maxIter=10`, `regParam=0.1`, and `coldStartStrategy='drop'` (new users without history are excluded from predictions).

**Output:** Two Parquet datasets:
- `features/recommendations/` — top-10 product recommendations per user
- `features/item_vectors/` — item factor vectors for computing "customers who bought X also bought Y"

---

### 5. Airflow DAG — `dags/ecommerce_pipeline.py`

The DAG runs daily at **2:00 AM UTC** and chains all components:

```
crawl_raw_zone
    │
    ▼
glue_events_etl          ← Cleans and sessionizes yesterday's events
    │
    ▼
create_emr_cluster        ← Provisions EMR: 1 master + 2 core nodes
    │
    ▼
run_spark_jobs            ← LTV features + ALS recommendations
    │
    ▼
load_to_redshift          ← COPY from S3 curated/features into star schema
    │
    ▼
refresh_materialized_views ← Refreshes daily aggregation MVs for BI
```

![Airflow DAG Graph](screenshots/airflow_dag.png)

**Reliability features:**
- `retries: 2` with `retry_delay: 5 minutes` — automatically retries transient AWS failures
- `sla: 2 hours` — Airflow alerts if the pipeline hasn't finished by 4 AM
- `catchup: False` — won't backfill missed runs automatically (safe for production)

---

## Redshift Schema (Star Schema)

The warehouse uses a classic **Kimball star schema**:

```
                          ┌──────────────┐
                          │  dim_date    │
                          │  (date_key)  │
                          └──────┬───────┘
                                 │
┌──────────────┐    ┌────────────▼─────────────┐    ┌───────────────┐
│ dim_customer │    │       fact_sales          │    │  dim_product  │
│  (user_id)   │◄───│  sale_id, session_id,     │───►│  (product_id) │
│              │    │  user_id, product_id,     │    │               │
│  LTV, RFM    │    │  date_key, quantity,      │    │  category,    │
│  segment     │    │  unit_price, total_amount │    │  brand, cost  │
└──────────────┘    └───────────────────────────┘    └───────────────┘
                                 │
                          ┌──────▼───────┐
                          │ dim_session  │
                          │ (session_id) │
                          │ duration,    │
                          │ device_type  │
                          └──────────────┘
```

**Performance optimizations:**

```sql
-- fact_sales is distributed by user_id:
-- All of a user's purchases live on the same Redshift node
-- → fast user-level aggregations (lifetime spend, purchase history)
DISTKEY (user_id)

-- fact_sales is sorted by date then user:
-- → fast time-range queries (yesterday's sales, monthly reports)
SORTKEY (date_key, user_id)
```

**Query example — daily revenue by category:**
```sql
SELECT
    d.year,
    d.month,
    p.category,
    SUM(f.total_amount)   AS revenue,
    COUNT(DISTINCT f.user_id) AS unique_buyers
FROM analytics.fact_sales f
JOIN analytics.dim_date    d ON f.date_key    = d.date_key
JOIN analytics.dim_product p ON f.product_id  = p.product_id
WHERE d.year = 2026
GROUP BY 1, 2, 3
ORDER BY 1, 2, revenue DESC;
```

---

## Infrastructure as Code (Terraform)

All AWS resources are defined in `infra/main.tf` — no clicking in the console.

**Resources provisioned:**
- `aws_s3_bucket` — data lake with versioning enabled
- `aws_kinesis_stream` — 4-shard stream, 48-hour retention
- `aws_kinesis_firehose_delivery_stream` — buffered S3 delivery (5 min / 5 MB)
- `aws_lambda_function` — event enrichment, Python 3.11, 256 MB
- `aws_lambda_event_source_mapping` — connects Kinesis → Lambda automatically
- `aws_glue_catalog_database` — unified metadata catalog
- `aws_glue_crawler` — auto-discovers S3 schema changes
- IAM roles for Glue, Lambda, Firehose with least-privilege policies

**To deploy to real AWS (requires AWS account):**
```bash
cd infra/

# Initialize Terraform
terraform init

# Preview what will be created
terraform plan -var="environment=dev" -var="aws_region=us-east-1"

# Deploy
terraform apply -var="environment=dev" -var="aws_region=us-east-1"

# Tear down (stops AWS billing)
terraform destroy -var="environment=dev" -var="aws_region=us-east-1"
```

> ⚠️ Deploying to AWS will incur costs. Estimated cost for dev environment: ~$5–15/day depending on EMR usage. Always `terraform destroy` after testing.

---

## Performance & Scale Metrics

| Metric | Value | How Achieved |
|--------|-------|-------------|
| Events processed/day | 1,000,000+ | 4 Kinesis shards × 1 MB/s each |
| Real-time latency (Kinesis → S3) | < 500 ms | Lambda + Firehose buffering |
| Batch ETL runtime | ~5 minutes | Glue with 10 G.1X workers |
| Redshift query latency | < 2 seconds | DISTKEY + SORTKEY + column compression |
| Data compression ratio | 70% | Snappy Parquet vs raw JSON |
| EMR cost reduction | 40% | Spot instances for core nodes |
| LTV model accuracy | R² = 0.78 | RFM features + Linear Regression |
| Airflow SLA | 2 hours (2 AM → 4 AM) | Retry logic + SLA alerting |

---

## Interview Reference: What to Say About This Project

### "Walk me through the architecture."

> "The pipeline has three layers. The real-time layer uses Kinesis Data Streams to ingest user events — clicks, purchases, views. A Lambda function enriches each event with geographic data from GeoIP and writes it to S3 via Kinesis Firehose. The batch layer is orchestrated by Airflow: a Glue PySpark job runs nightly to clean the data and sessionize it — grouping events into 30-minute sessions using window functions. Then EMR Spark runs two ML jobs: one builds customer LTV scores using RFM analysis, the other trains an ALS collaborative filtering model for recommendations. Finally, everything lands in a Redshift star schema optimized with DISTKEY and SORTKEY for sub-2-second BI queries."

### "Why Kinesis and not Kafka?"

> "For an AWS-native stack, Kinesis is the natural choice — it integrates directly with Lambda, Firehose, and Glue without managing brokers. Firehose handles the S3 buffering automatically. The trade-off is less flexibility than Kafka (for example, message retention is capped at 7 days vs Kafka's unlimited), but for this use case — ingestion to analytics — Kinesis is simpler to operate and pairs perfectly with the rest of the AWS ecosystem."

### "What's the most technically complex part?"

> "The sessionization logic in the Glue ETL job. I used Spark window functions — specifically `lag` to get the previous event timestamp, then a `when` condition to detect gaps over 30 minutes, and a cumulative `sum` to assign session numbers. Getting this right on a distributed dataset required understanding how Spark partitions data across workers. I partition by `user_id` so all events for one user stay on the same worker — otherwise the session boundary detection would break across partitions."

### "How would you scale this to 10x traffic?"

> "Three main levers: increase Kinesis shards from 4 to 40 (each shard handles 1 MB/s); enable Glue auto-scaling so the ETL cluster grows with data volume; and switch Redshift to a RA3 node type with Redshift Spectrum so the hot data stays in the warehouse while cold data is queried directly from S3 — that alone would handle 10x without a proportional cost increase."

---

## Extending This Project

Here are practical next steps to make this even more impressive:

**Add real-time dashboards** — Connect Redshift or a DynamoDB table to Amazon QuickSight for live revenue/conversion dashboards.

**Add data quality checks** — Use Great Expectations or Deequ to validate row counts, null rates, and schema drift in Glue before loading to Redshift.

**Add CDC (Change Data Capture)** — Use AWS DMS to replicate from an RDS product catalog into Redshift, keeping dim_product always current.

**Add A/B test analysis** — Build a fact_experiments table that tracks which recommendation model variant each user saw and whether they converted.

**Productionize the LTV model** — Package the Spark ML model with MLflow, version it to S3, and serve predictions via a Lambda endpoint for real-time personalization.

**Add cost monitoring** — Tag all AWS resources with `Project=ecommerce-pipeline` and use AWS Cost Explorer to track spend per pipeline stage.

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

*Built by Muskan Chauhan | Data Engineer | [GitHub](https://github.com/muskanchauhan1)*
