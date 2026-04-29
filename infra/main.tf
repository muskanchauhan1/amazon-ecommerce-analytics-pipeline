# Terraform configuration for Amazon E-Commerce Analytics Pipeline

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Data Lake
resource "aws_s3_bucket" "data_lake" {
  bucket = "ecommerce-pipeline-${var.environment}"
  tags   = { Environment = var.environment, Project = "ecommerce-analytics" }
}

resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration { status = "Enabled" }
}

# Kinesis Data Stream for real-time events
resource "aws_kinesis_stream" "events" {
  name             = "ecommerce-events-${var.environment}"
  shard_count      = 4
  retention_period = 48  # hours
  shard_level_metrics = ["IncomingBytes", "OutgoingBytes"]
}

# Kinesis Firehose to S3
resource "aws_kinesis_firehose_delivery_stream" "events_to_s3" {
  name        = "ecommerce-events-firehose-${var.environment}"
  destination = "s3"

  s3_configuration {
    role_arn   = aws_iam_role.firehose_role.arn
    bucket_arn = aws_s3_bucket.data_lake.arn
    prefix     = "raw/events/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    buffer_interval = 300
    buffer_size     = 5
  }
}

# Lambda for real-time enrichment
resource "aws_lambda_function" "event_processor" {
  function_name = "ecommerce-event-processor-${var.environment}"
  runtime       = "python3.11"
  handler       = "event_processor.lambda_handler"
  role          = aws_iam_role.lambda_role.arn
  s3_bucket     = aws_s3_bucket.data_lake.bucket
  s3_key        = "lambda/ecommerce_lambda.zip"
  timeout       = 30
  memory_size   = 256
}

resource "aws_lambda_event_source_mapping" "kinesis_trigger" {
  event_source_arn = aws_kinesis_stream.events.arn
  function_name    = aws_lambda_function.event_processor.arn
  starting_position = "LATEST"
}

# Glue Catalog Database
resource "aws_glue_catalog_database" "ecommerce" {
  name = "ecommerce_${var.environment}"
}

# Glue Crawler for raw zone
resource "aws_glue_crawler" "raw_zone" {
  name          = "raw_zone_crawler_${var.environment}"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.ecommerce.name
  s3_target { path = "s3://${aws_s3_bucket.data_lake.bucket}/raw/" }
}

# Glue Job for ETL
resource "aws_glue_job" "events_etl" {
  name     = "ecommerce_events_transformer_${var.environment}"
  role_arn = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 10
  command {
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/glue_jobs/events_etl.py"
    python_version  = "3"
  }
}

# Redshift Cluster
resource "aws_redshift_cluster" "analytics" {
  cluster_identifier  = "ecommerce-analytics-${var.environment}"
  node_type          = "ra3.xlplus"
  number_of_nodes    = 2
  master_username    = var.redshift_username
  master_password    = var.redshift_password
  db_name            = "ecommerce"
  cluster_subnet_group_name = aws_redshift_subnet_group.main.name
  iam_roles          = [aws_iam_role.redshift_role.arn]
}

# RDS PostgreSQL for metadata
resource "aws_db_instance" "metadata" {
  identifier        = "ecommerce-metadata-${var.environment}"
  engine            = "postgres"
  engine_version    = "15"
  instance_class    = "db.t3.micro"
  allocated_storage = 20
  db_name           = "metadata"
  username          = var.rds_username
  password          = var.rds_password
}

# EMR Cluster (for Spark jobs)
resource "aws_emr_cluster" "spark_cluster" {
  name          = "ecommerce-spark-${var.environment}"
  release_label = "emr-6.15.0"
  applications  = ["Spark", "Hive", "Hadoop"]

  ec2_attributes {
    subnet_id = var.subnet_id
  }

  master_instance_group { instance_type = "m5.xlarge" }
  core_instance_group {
    instance_type  = "m5.xlarge"
    instance_count = 2
  }

  service_role = aws_iam_role.emr_role.arn
  autoscaling_role = aws_iam_role.emr_autoscaling_role.arn
}

# IAM Roles
resource "aws_iam_role" "glue_role" {
  name = "GlueServiceRole_${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Action = "sts:AssumeRole", Principal = { Service = "glue.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
