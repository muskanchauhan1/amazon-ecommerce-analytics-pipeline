import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'raw_path', 'curated_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RAW_PATH = args['raw_path']  # s3://ecommerce-pipeline/raw/events/
CURATED_PATH = args['curated_path']  # s3://ecommerce-pipeline/curated/

# Read raw JSON events from Kinesis Firehose output
raw_df = spark.read.json(RAW_PATH)

# Data cleaning & enrichment
cleaned = raw_df.select(
    col('event_id').cast('string'),
    col('user_id').cast('string'),
    col('session_id').cast('string'),
    col('product_id').cast('string'),
    col('event_type'),  # click, view, purchase, cart_add
    col('price').cast('decimal(10,2)'),
    from_unixtime(col('timestamp')/1000).alias('event_ts'),
    col('category'),
    col('brand'),
    col('device_type'),
    col('ip_address')
).withColumn('date', to_date('event_ts')) \
 .withColumn('hour', hour('event_ts')) \
 .dropna(subset=['user_id', 'event_type', 'event_ts'])

# Sessionization: group events into sessions (30-min timeout)
window_spec = Window.partitionBy('user_id').orderBy('event_ts')
sessions = cleaned.withColumn('prev_ts', lag('event_ts').over(window_spec)) \
    .withColumn('new_session',
        when(col('prev_ts').isNull() |
             (unix_timestamp('event_ts') - unix_timestamp('prev_ts') > 1800), 1).otherwise(0)) \
    .withColumn('session_id', sum('new_session').over(window_spec)) \
    .drop('prev_ts', 'new_session')

# Aggregate purchase metrics per session
purchase_agg = sessions.filter(col('event_type') == 'purchase') \
    .groupBy('session_id', 'user_id', 'date') \
    .agg(
        sum('price').alias('total_revenue'),
        count('*').alias('items_purchased'),
        collect_set('category').alias('categories_purchased'),
        first('device_type').alias('device_type')
    )

# Write curated data as partitioned Parquet
sessions.write \
    .mode('append') \
    .partitionBy('date') \
    .parquet(f'{CURATED_PATH}events/')

purchase_agg.write \
    .mode('append') \
    .partitionBy('date') \
    .parquet(f'{CURATED_PATH}sales/')

# Update Glue Data Catalog
glueContext.create_dynamic_frame.from_catalog(
    database='ecommerce_curated',
    table_name='sales'
).toDF().write.option('mergeSchema', 'true').save()

job.commit()
