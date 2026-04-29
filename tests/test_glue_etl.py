"""Unit tests for Glue ETL job logic."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

@pytest.fixture
def spark():
    return SparkSession.builder \
        .master('local[1]') \
        .appName('TestGlueETL') \
        .getOrCreate()

def test_sessionization(spark):
    """Test that events are correctly grouped into sessions."""
    data = [
        ('U001', 1000), ('U001', 1100), ('U001', 4000),  # 3rd event > 30s gap
        ('U002', 2000), ('U002', 2100)
    ]
    df = spark.createDataFrame(data, ['user_id', 'ts']) \
        .withColumn('event_ts', from_unixtime(col('ts')))

    window_spec = Window.partitionBy('user_id').orderBy('event_ts')
    result = df.withColumn('prev_ts', lag('event_ts').over(window_spec)) \
        .withColumn('new_session',
            when(col('prev_ts').isNull() |
                 (unix_timestamp('event_ts') - unix_timestamp('prev_ts') > 30), 1).otherwise(0)) \
        .withColumn('session_id', sum('new_session').over(window_spec))

    sessions = result.collect()
    user1_sessions = [r['session_id'] for r in sessions if r['user_id'] == 'U001']
    assert max(user1_sessions) > min(user1_sessions)  # Should have multiple sessions

def test_cleaned_data_drops_nulls(spark):
    """Test that null user_ids are filtered out."""
    data = [('U001', 'purchase', 50.0), (None, 'view', None), ('U002', 'click', None)]
    df = spark.createDataFrame(data, ['user_id', 'event_type', 'price']) \
        .withColumn('price', col('price').cast('decimal(10,2)')) \
        .dropna(subset=['user_id', 'event_type'])

    assert df.count() == 2
    assert df.filter(col('user_id').isNull()).count() == 0
