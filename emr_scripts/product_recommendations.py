"""
Product recommendation features using Spark ML on EMR.
Builds item-item collaborative filtering features.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName('ProductRecommendations') \
    .config('spark.sql.shuffle.partitions', '200') \
    .getOrCreate()

# Read events data
events = spark.read.parquet('s3a://ecommerce-pipeline/curated/events/')

# Create user-item interaction matrix (implicit feedback)
interactions = events.filter(col('event_type').isin(['view', 'cart_add', 'purchase'])) \
    .withColumn('rating',
        when(col('event_type') == 'purchase', 5.0)
        .when(col('event_type') == 'cart_add', 3.0)
        .otherwise(1.0)
    ).groupBy('user_id', 'product_id').agg(avg('rating').alias('rating'))

# ALS Collaborative Filtering
als = ALS(
    maxIter=10,
    regParam=0.1,
    userCol='user_id',
    itemCol='product_id',
    ratingCol='rating',
    coldStartStrategy='drop',
    nonnegative=True
)

model = als.fit(interactions)

# Generate top-10 recommendations per user
user_recs = model.recommendForAllUsers(10)

# Explode recommendations into rows
recs_exploded = user_recs.select(
    'user_id',
    explode('recommendations').alias('rec')
).select(
    'user_id',
    col('rec.product_id').alias('recommended_product_id'),
    col('rec.rating').alias('predicted_rating')
)

# Product similarity using item factors
item_factors = model.itemFactors
# Self-join to compute cosine similarity between products
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=['features'], outputCol='vec')
item_vectors = assembler.transform(item_factors)

# Write recommendations to S3 for Redshift loading
recs_exploded.write \
    .mode('overwrite') \
    .parquet('s3a://ecommerce-pipeline/features/recommendations/')

# Write item similarity matrix for "users who bought X also bought Y"
item_vectors.write \
    .mode('overwrite') \
    .parquet('s3a://ecommerce-pipeline/features/item_vectors/')

spark.stop()
