CREATE OR REPLACE STORAGE INTEGRATION s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::xxxxxxxxx:role/snowflake-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://<bucket>/');

DESC STORAGE INTEGRATION s3_int;
-- Update Policy

USE DATABASE ANALYTICS;
USE SCHEMA BRONZE;

CREATE OR REPLACE STAGE bronze_stage
  url='s3://<bucket>/'
  storage_integration = s3_int;

CREATE OR REPLACE FILE FORMAT parquet_ff
  type = parquet;