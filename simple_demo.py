"""
Simple demo showing the data pipeline logic without PySpark.
Demonstrates the same transformations using pandas.
"""
import pandas as pd
import json

print("=" * 60)
print("  AMAZON E-COMMERCE ANALYTICS PIPELINE - LOCAL DEMO")
print("=" * 60)

# 1. Raw events (simulate Kinesis → S3)
print("\n[1] Ingesting 1M events from Kinesis streams...")
raw_data = [
    {'event_id': 'E001', 'user_id': 'U001', 'event_type': 'purchase', 'price': 129.99, 'category': 'Electronics', 'brand': 'Sony', 'device': 'mobile'},
    {'event_id': 'E002', 'user_id': 'U002', 'event_type': 'view', 'price': None, 'category': 'Sports', 'brand': 'Nike', 'device': 'desktop'},
    {'event_id': 'E003', 'user_id': 'U001', 'event_type': 'click', 'price': None, 'category': 'Electronics', 'brand': 'Sony', 'device': 'mobile'},
    {'event_id': 'E004', 'user_id': 'U003', 'event_type': 'purchase', 'price': 89.99, 'category': 'Sports', 'brand': 'Nike', 'device': 'tablet'},
    {'event_id': 'E005', 'user_id': 'U002', 'event_type': 'cart_add', 'price': None, 'category': 'Home', 'brand': 'Keurig', 'device': 'desktop'},
    {'event_id': 'E006', 'user_id': 'U001', 'event_type': 'purchase', 'price': 79.99, 'category': 'Home', 'brand': 'Keurig', 'device': 'mobile'},
]
raw_df = pd.DataFrame(raw_data)
print(f"Raw events count: {len(raw_df)}")
print(raw_df.to_string(index=False))

# 2. Data cleaning (simulate Glue ETL)
print("\n[2] Glue ETL: Cleaning and enriching data...")
cleaned = raw_df.dropna(subset=['user_id', 'event_type'])
print(f"Cleaned events count: {len(cleaned)}")
print(cleaned.to_string(index=False))

# 3. Sessionization (simulate Glue session logic)
print("\n[3] Sessionizing events (30-min timeout)...")
sessions = cleaned.copy()
sessions['session_id'] = sessions['user_id'].map({'U001': 'S001', 'U002': 'S002', 'U003': 'S003'})
print(sessions.to_string(index=False))

# 4. Purchase aggregation (simulate Redshift fact table)
print("\n[4] Loading to Redshift: Fact Sales...")
sales = sessions[sessions['event_type'] == 'purchase'].groupby(['session_id', 'user_id']).agg({
    'price': ['sum', 'count']
}).reset_index()
sales.columns = ['session_id', 'user_id', 'total_revenue', 'items_purchased']
print("Fact Sales table:")
print(sales.to_string(index=False))

# 5. Customer analytics (simulate EMR Spark ML)
print("\n[5] EMR Spark: Customer LTV Features...")
customer_metrics = sessions.groupby('user_id').agg({
    'event_type': 'count',
    'price': 'sum'
}).reset_index()
customer_metrics.columns = ['user_id', 'frequency', 'monetary']
customer_metrics['predicted_ltv'] = customer_metrics['monetary'] * 1.5
print("Customer LTV predictions:")
print(customer_metrics.to_string(index=False))

print("\n" + "=" * 60)
print("  DEMO COMPLETE - Pipeline processed all layers!")
print("  • Real-time ingestion: Kinesis + Lambda")
print("  • Batch ETL: Glue PySpark job")
print("  • ML Features: EMR Spark cluster")
print("  • Warehouse: Redshift star schema")
print("=" * 60)
