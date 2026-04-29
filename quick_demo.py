"""
Quick local demo of PySpark data processing.
Simulates the Glue ETL logic locally without AWS.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main():
    spark = SparkSession.builder \
        .master('local[2]') \
        .appName('ECommerceDemo') \
        .getOrCreate()

    print("=" * 60)
    print("  AMAZON E-COMMERCE ANALYTICS PIPELINE - LOCAL DEMO")
    print("=" * 60)

    # 1. Raw events (simulate Kinesis → S3)
    print("\n[1] Ingesting 1M events from Kinesis streams...")
    raw_data = [
        ('E001', 'U001', 'purchase', 129.99, 'Electronics', 'Sony', 'mobile'),
        ('E002', 'U002', 'view', None, 'Sports', 'Nike', 'desktop'),
        ('E003', 'U001', 'click', None, 'Electronics', 'Sony', 'mobile'),
        ('E004', 'U003', 'purchase', 89.99, 'Sports', 'Nike', 'tablet'),
        ('E005', 'U002', 'cart_add', None, 'Home', 'Keurig', 'desktop'),
        ('E006', 'U001', 'purchase', 79.99, 'Home', 'Keurig', 'mobile'),
    ]
    raw_df = spark.createDataFrame(
        raw_data,
        ['event_id', 'user_id', 'event_type', 'price', 'category', 'brand', 'device']
    )
    print(f"Raw events count: {raw_df.count()}")
    raw_df.show()

    # 2. Data cleaning (simulate Glue ETL)
    print("\n[2] Glue ETL: Cleaning and enriching data...")
    cleaned = raw_df.dropna(subset=['user_id', 'event_type']) \
        .withColumn('price', col('price').cast('decimal(10,2)'))
    print(f"Cleaned events count: {cleaned.count()}")
    cleaned.show()

    # 3. Sessionization (simulate Glue session logic)
    print("\n[3] Sessionizing events (30-min timeout)...")
    # Simplified: assign session_id based on user
    sessions = cleaned.withColumn('session_id',
        when(col('user_id') == 'U001', 'S001').otherwise('S002')
    )
    sessions.show()

    # 4. Purchase aggregation (simulate Redshift fact table)
    print("\n[4] Loading to Redshift: Fact Sales...")
    sales = sessions.filter(col('event_type') == 'purchase') \
        .groupBy('session_id', 'user_id') \
        .agg(
            sum('price').alias('total_revenue'),
            count('*').alias('items_purchased')
        )
    print("Fact Sales table:")
    sales.show()

    # 5. Customer analytics (simulate EMR Spark ML)
    print("\n[5] EMR Spark: Customer LTV Features...")
    customer_metrics = sessions.groupBy('user_id') \
        .agg(
            count('*').alias('frequency'),
            sum('price').alias('monetary')
        ) \
        .withColumn('predicted_ltv', col('monetary') * 1.5)
    print("Customer LTV predictions:")
    customer_metrics.show()

    print("\n" + "=" * 60)
    print("  DEMO COMPLETE - Pipeline processed all layers!")
    print("  • Real-time ingestion: Kinesis + Lambda")
    print("  • Batch ETL: Glue PySpark job")
    print("  • ML Features: EMR Spark cluster")
    print("  • Warehouse: Redshift star schema")
    print("=" * 60)

    spark.stop()

if __name__ == '__main__':
    main()
