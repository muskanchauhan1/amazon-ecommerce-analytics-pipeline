-- =============================================
-- Amazon E-Commerce Analytics - Redshift Star Schema
-- =============================================

-- DIMENSION TABLES

CREATE TABLE analytics.dim_customer (
    user_id        VARCHAR(64)   PRIMARY KEY,
    first_seen     DATE          NOT NULL,
    last_seen      DATE          NOT NULL,
    registration_date DATE,
    customer_segment VARCHAR(20),        -- New, Returning, VIP, Churned
    preferred_device  VARCHAR(20),
    country        VARCHAR(2),
    city           VARCHAR(50),
    ltv_12m        DECIMAL(12,2),        -- Predicted lifetime value
    recency_score  SMALLINT,
    frequency_score SMALLINT,
    monetary_score  SMALLINT,
    rfm_segment    VARCHAR(10)           -- Champions, Loyal, At Risk, etc.
);

CREATE TABLE analytics.dim_product (
    product_id     VARCHAR(64)   PRIMARY KEY,
    product_name   VARCHAR(200),
    category       VARCHAR(50),
    subcategory    VARCHAR(50),
    brand          VARCHAR(50),
    price          DECIMAL(10,2),
    cost           DECIMAL(10,2),
    margin_pct     DECIMAL(5,2),
    is_active      BOOLEAN       DEFAULT TRUE,
    created_date   DATE
);

CREATE TABLE analytics.dim_date (
    date_key       DATE          PRIMARY KEY,
    year           SMALLINT,
    quarter        SMALLINT,
    month          SMALLINT,
    week           SMALLINT,
    day            SMALLINT,
    day_of_week    VARCHAR(10),
    is_weekend     BOOLEAN,
    is_holiday     BOOLEAN
);

CREATE TABLE analytics.dim_session (
    session_id     VARCHAR(64)   PRIMARY KEY,
    user_id        VARCHAR(64)   NOT NULL,
    session_start  TIMESTAMP,
    session_end    TIMESTAMP,
    duration_secs  INT,
    page_views     INT,
    device_type    VARCHAR(20),
    browser        VARCHAR(20),
    traffic_source VARCHAR(50)
);

-- FACT TABLES

CREATE TABLE analytics.fact_sales (
    sale_id        VARCHAR(64)   PRIMARY KEY,
    session_id     VARCHAR(64)   NOT NULL,
    user_id        VARCHAR(64)   NOT NULL,
    product_id     VARCHAR(64)   NOT NULL,
    date_key       DATE          NOT NULL,
    hour           SMALLINT,
    quantity       INT           DEFAULT 1,
    unit_price     DECIMAL(10,2) NOT NULL,
    total_amount   DECIMAL(12,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    tax_amount     DECIMAL(10,2),
    payment_method VARCHAR(20),
    FOREIGN KEY (user_id) REFERENCES analytics.dim_customer(user_id),
    FOREIGN KEY (product_id) REFERENCES analytics.dim_product(product_id),
    FOREIGN KEY (date_key) REFERENCES analytics.dim_date(date_key)
)
SORTKEY (date_key, user_id)
DISTKEY (user_id);

CREATE TABLE analytics.fact_user_events (
    event_id       VARCHAR(64)   PRIMARY KEY,
    session_id     VARCHAR(64)   NOT NULL,
    user_id        VARCHAR(64)   NOT NULL,
    product_id     VARCHAR(64),
    date_key       DATE          NOT NULL,
    event_timestamp TIMESTAMP    NOT NULL,
    event_type     VARCHAR(20),  -- click, view, cart_add, purchase
    category       VARCHAR(50),
    brand          VARCHAR(50),
    device_type    VARCHAR(20),
    FOREIGN KEY (user_id) REFERENCES analytics.dim_customer(user_id),
    FOREIGN KEY (product_id) REFERENCES analytics.dim_product(product_id),
    FOREIGN KEY (date_key) REFERENCES analytics.dim_date(date_key)
)
SORTKEY (date_key, event_type)
DISTKEY (user_id);

-- Aggregate Tables (for fast BI queries)

CREATE TABLE analytics.aggr_daily_sales (
    date_key       DATE      NOT NULL,
    category       VARCHAR(50),
    total_revenue  DECIMAL(14,2),
    total_orders   INT,
    unique_customers INT,
    avg_order_value DECIMAL(10,2),
    PRIMARY KEY (date_key, category)
);

CREATE TABLE analytics.aggr_customer_cohort (
    cohort_month   DATE      NOT NULL,
    period         INT       NOT NULL,  -- months since first purchase
    active_customers INT,
    revenue        DECIMAL(14,2),
    retention_rate DECIMAL(5,4),
    PRIMARY KEY (cohort_month, period)
);

-- Stored Procedure for daily aggregation refresh
CREATE OR REPLACE PROCEDURE analytics.refresh_daily_aggregations()
AS $$
BEGIN
    -- Refresh daily sales aggregates
    DELETE FROM analytics.aggr_daily_sales
    WHERE date_key >= CURRENT_DATE - 2;

    INSERT INTO analytics.aggr_daily_sales
    SELECT
        d.date_key,
        p.category,
        SUM(f.total_amount) AS total_revenue,
        COUNT(DISTINCT f.sale_id) AS total_orders,
        COUNT(DISTINCT f.user_id) AS unique_customers,
        AVG(f.total_amount) AS avg_order_value
    FROM analytics.fact_sales f
    JOIN analytics.dim_date d ON f.date_key = d.date_key
    JOIN analytics.dim_product p ON f.product_id = p.product_id
    WHERE d.date_key >= CURRENT_DATE - 2
    GROUP BY d.date_key, p.category;

    -- Refresh cohort analysis
    DELETE FROM analytics.aggr_customer_cohort
    WHERE cohort_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '6 months');

    INSERT INTO analytics.aggr_customer_cohort
    WITH first_purchase AS (
        SELECT user_id, DATE_TRUNC('month', MIN(date_key)) AS cohort_month
        FROM analytics.fact_sales
        GROUP BY user_id
    ),
    cohort_data AS (
        SELECT
            fp.cohort_month,
            DATEDIFF(month, fp.cohort_month, fs.date_key) AS period,
            COUNT(DISTINCT fs.user_id) AS active_customers,
            SUM(fs.total_amount) AS revenue
        FROM first_purchase fp
        JOIN analytics.fact_sales fs ON fp.user_id = fs.user_id
        GROUP BY fp.cohort_month, period
    )
    SELECT
        cohort_month,
        period,
        active_customers,
        revenue,
        active_customers * 1.0 / FIRST_VALUE(active_customers) OVER (
            PARTITION BY cohort_month ORDER BY period
        ) AS retention_rate
    FROM cohort_data;
END;
$$ LANGUAGE plpgsql;

-- Views for BI dashboards

CREATE VIEW analytics.vw_customer_360 AS
SELECT
    c.user_id,
    c.customer_segment,
    c.ltv_12m,
    c.rfm_segment,
    COUNT(DISTINCT fs.sale_id) AS total_orders,
    SUM(fs.total_amount) AS lifetime_spend,
    MAX(fs.date_key) AS last_purchase_date,
    COUNT(DISTINCT fe.event_id) AS total_events
FROM analytics.dim_customer c
LEFT JOIN analytics.fact_sales fs ON c.user_id = fs.user_id
LEFT JOIN analytics.fact_user_events fe ON c.user_id = fe.user_id
GROUP BY c.user_id, c.customer_segment, c.ltv_12m, c.rfm_segment;

CREATE VIEW analytics.vw_product_performance AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    SUM(fs.quantity) AS units_sold,
    SUM(fs.total_amount) AS total_revenue,
    COUNT(DISTINCT fs.user_id) AS unique_buyers,
    AVG(fs.unit_price) AS avg_selling_price
FROM analytics.dim_product p
LEFT JOIN analytics.fact_sales fs ON p.product_id = fs.product_id
GROUP BY p.product_id, p.product_name, p.category, p.brand;
