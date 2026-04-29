from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrStepSensor
)
from airflow.providers.amazon.aws.sensors.kinesis import AwsGlueCrawlerSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.python import PythonOperator
import boto3

DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2),
}

# EMR cluster config for Spark jobs
EMR_CONFIG = {
    'Name': 'spark-feature-engineering',
    'ReleaseLabel': 'emr-6.15.0',
    'Applications': [{'Name': 'Spark'}, {'Name': 'Hive'}, {'Name': 'Hadoop'}],
    'Instances': {
        'InstanceGroups': [
            {'Name': 'Master', 'InstanceRole': 'MASTER', 'InstanceType': 'm5.xlarge', 'InstanceCount': 1},
            {'Name': 'Core', 'InstanceRole': 'CORE', 'InstanceType': 'm5.xlarge', 'InstanceCount': 2},
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}

# Spark steps for EMR
SPARK_STEPS = [
    {
        'Name': 'Customer-LTV-Features',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--deploy-mode', 'cluster',
                     's3://ecommerce-pipeline/emr_scripts/customer_ltv_features.py'],
        },
    },
    {
        'Name': 'Product-Recommendation-Prep',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--deploy-mode', 'cluster',
                     's3://ecommerce-pipeline/emr_scripts/product_recommendations.py'],
        },
    },
]

with DAG(
    dag_id='ecommerce_analytics_pipeline',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['ecommerce', 'redshift', 'glue', 'emr'],
) as dag:

    # Step 1: Run Glue Crawler to update Data Catalog
    crawl_raw = PythonOperator(
        task_id='crawl_raw_zone',
        python_callable=lambda: boto3.client('glue').start_crawler(Name='raw_zone_crawler'),
    )

    # Step 2: Glue ETL - clean & partition raw events
    glue_etl = GlueJobOperator(
        task_id='glue_events_etl',
        job_name='ecommerce_events_transformer',
        script_location='s3://ecommerce-pipeline/glue_jobs/events_etl.py',
        iam_role_name='GlueServiceRole',
        create_job_kwargs={'GlueVersion': '4.0', 'NumberOfWorkers': 10, 'WorkerType': 'G.1X'},
    )

    # Step 3: EMR cluster for Spark feature engineering
    create_emr = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=EMR_CONFIG,
    )

    add_spark_steps = EmrAddStepsOperator(
        task_id='add_spark_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        steps=SPARK_STEPS,
    )

    # Wait for Spark jobs to complete
    wait_spark = EmrStepSensor(
        task_id='wait_spark_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps')[0] }}",
    )

    # Step 4: Load curated data to Redshift
    load_fact_sales = S3ToRedshiftOperator(
        task_id='load_fact_sales',
        schema='analytics',
        table='fact_sales',
        s3_bucket='ecommerce-pipeline',
        s3_key='curated/sales/',
        copy_options=['FORMAT AS PARQUET'],
        aws_conn_id='aws_default',
    )

    load_fact_events = S3ToRedshiftOperator(
        task_id='load_fact_events',
        schema='analytics',
        table='fact_user_events',
        s3_bucket='ecommerce-pipeline',
        s3_key='curated/events/',
        copy_options=['FORMAT AS PARQUET'],
        aws_conn_id='aws_default',
    )

    # Step 5: Run Redshift stored procedures for aggregations
    refresh_mvs = PythonOperator(
        task_id='refresh_materialized_views',
        python_callable=lambda: boto3.client('redshift-data').execute_statement(
            Database='ecommerce',
            Sql='CALL analytics.refresh_daily_aggregations();'
        ),
    )

    # Pipeline dependencies
    crawl_raw >> glue_etl >> create_emr >> add_spark_steps >> wait_spark
    wait_spark >> [load_fact_sales, load_fact_events] >> refresh_mvs
