-- Hive DDL for EMR Hadoop cluster
-- External tables pointing to S3 data lake

CREATE DATABASE IF NOT EXISTS ecommerce
LOCATION 's3a://ecommerce-pipeline/hive-warehouse/';

USE ecommerce;

-- Raw events table (direct from Kinesis/Firehose)
CREATE EXTERNAL TABLE raw_events (
    event_id        STRING,
    user_id         STRING,
    session_id      STRING,
    product_id      STRING,
    event_type      STRING,
    price           DECIMAL(10,2),
    timestamp       BIGINT,
    category        STRING,
    brand           STRING,
    device_type     STRING,
    ip_address      STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 's3a://ecommerce-pipeline/raw/events/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Curated events table (post-Glue ETL)
CREATE EXTERNAL TABLE curated_events (
    event_id        STRING,
    user_id         STRING,
    session_id      STRING,
    product_id      STRING,
    event_type      STRING,
    price           DECIMAL(10,2),
    event_ts        TIMESTAMP,
    category        STRING,
    brand           STRING,
    device_type     STRING
)
PARTITIONED BY (date DATE)
STORED AS PARQUET
LOCATION 's3a://ecommerce-pipeline/curated/events/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Sales aggregation table
CREATE EXTERNAL TABLE curated_sales (
    session_id      STRING,
    user_id         STRING,
    total_revenue   DECIMAL(12,2),
    items_purchased INT,
    categories_purchased ARRAY<STRING>,
    device_type     STRING
)
PARTITIONED BY (date DATE)
STORED AS PARQUET
LOCATION 's3a://ecommerce-pipeline/curated/sales/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Customer LTV features (output from Spark ML)
CREATE EXTERNAL TABLE customer_ltv (
    user_id             STRING,
    recency_days        INT,
    frequency           INT,
    monetary            DECIMAL(10,2),
    rfm_score           INT,
    preferred_device    STRING,
    predicted_ltv_12m   DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 's3a://ecommerce-pipeline/features/customer_ltv/';

-- Product recommendations
CREATE EXTERNAL TABLE product_recommendations (
    user_id                STRING,
    recommended_product_id  STRING,
    predicted_rating        DOUBLE
)
STORED AS PARQUET
LOCATION 's3a://ecommerce-pipeline/features/recommendations/';

-- Materialized view: Daily category metrics (Hive 3.0+)
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_category_metrics
STORED AS PARQUET
LOCATION 's3a://ecommerce-pipeline/mv/daily_category/'
AS
SELECT
    date,
    category,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS revenue,
    AVG(CASE WHEN event_type = 'purchase' THEN price END) AS avg_order_value
FROM curated_events
GROUP BY date, category;
