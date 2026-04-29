"""
Customer LTV (Lifetime Value) feature engineering using PySpark on EMR.
Calculates RFM scores, cohort retention, and LTV predictions.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder \
    .appName('CustomerLTVFeatures') \
    .config('spark.sql.adaptive.enabled', 'true') \
    .getOrCreate()

# Read curated sales data from S3
sales_df = spark.read.parquet('s3a://ecommerce-pipeline/curated/sales/')

# RFM Analysis (Recency, Frequency, Monetary)
customer_metrics = sales_df.groupBy('user_id').agg(
    datediff(current_date(), max('date')).alias('recency_days'),
    count('session_id').alias('frequency'),
    sum('total_revenue').alias('monetary'),
    collect_set('categories_purchased').alias('category_diversity'),
    first('device_type').alias('preferred_device')
)

# Calculate LTV = AOV * Purchase Frequency * Customer Lifespan
# Simplified LTV model for e-commerce
window = Window.orderBy('monetary')
ltv_features = customer_metrics \
    .withColumn('aov', col('monetary') / col('frequency')) \
    .withColumn('recency_score', ntile(5).over(Window.orderBy('recency_days'))) \
    .withColumn('frequency_score', ntile(5).over(Window.orderBy(desc('frequency')))) \
    .withColumn('monetary_score', ntile(5).over(Window.orderBy(desc('monetary')))) \
    .withColumn('rfm_score', col('recency_score') + col('frequency_score') + col('monetary_score'))

# Feature vector for ML model
assembler = VectorAssembler(
    inputCols=['recency_days', 'frequency', 'monetary', 'rfm_score'],
    outputCol='features'
)
featured = assembler.transform(ltv_features)

# Train simple LTV prediction model
lr = LinearRegression(featuresCol='features', labelCol='monetary')
ltv_model = lr.fit(featured)

# Predict LTV for all customers
predictions = ltv_model.transform(featured).select(
    'user_id', 'recency_days', 'frequency', 'monetary',
    'rfm_score', 'preferred_device',
    col('prediction').alias('predicted_ltv_12m')
)

# Write to Redshift-ready format (Parquet)
predictions.write \
    .mode('overwrite') \
    .partitionBy('preferred_device') \
    .parquet('s3a://ecommerce-pipeline/features/customer_ltv/')

# Also create Hive table for ad-hoc queries
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce.customer_ltv (
        user_id STRING,
        recency_days INT,
        frequency INT,
        monetary DECIMAL(10,2),
        rfm_score INT,
        preferred_device STRING,
        predicted_ltv_12m DOUBLE
    )
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    LOCATION 's3a://ecommerce-pipeline/features/customer_ltv/'
""")

spark.stop()
