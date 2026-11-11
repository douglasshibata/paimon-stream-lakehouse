-- ===================================================================================================
-- FLINK SQL SCRIPT: APACHE PAIMON CATALOG REGISTRATION AND REAL-TIME CDC PIPELINE INGESTION
-- ===================================================================================================

-- 1. Create and configure the Apache Paimon Catalog backed by MinIO/S3 FileSystem Catalog
CREATE CATALOG paimon_catalog WITH (
  'type' = 'paimon',
  'warehouse' = 's3://paimon-lakehouse/warehouse',
  -- S3 / MinIO Configuration Details
  'fs.s3a.endpoint' = 'http://minio:9000',
  'fs.s3a.access-key' = 'admin',
  'fs.s3a.secret-key' = 'supersecretpassword',
  'fs.s3a.path.style.access' = 'true',
  'fs.s3a.impl' = 'org.apache.hadoop.fs.s3a.S3AFileSystem'
);

-- Shift context to use the newly created catalog
USE CATALOG paimon_catalog;

-- Create target database inside the Paimon S3 Warehouse
CREATE DATABASE IF NOT EXISTS dw_lakehouse;
USE dw_lakehouse;


-- 2. Create the target Apache Paimon Primary Key table with optimized parameters
-- We configure:
-- - 'primary key' on 'user_id' (enforcing row uniqueness).
-- - 'merge-engine' = 'deduplicate' to keep only the latest record when key duplicates arrive.
-- - 'changelog-producer' = 'lookup' which dynamically queries index to generate delta streams (+I, -U, +U, -D).
-- - 'bucket' = '4' to distribute operations nicely across 4 write buckets.
-- - 'bucket-key' = 'user_id' to prevent bucket skew and ensure uniform hashing.
CREATE TABLE IF NOT EXISTS users_paimon (
  user_id BIGINT,
  name STRING,
  email STRING,
  city STRING,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'lookup',
  'bucket' = '4',
  'bucket-key' = 'user_id',
  'write-buffer-size' = '64mb',
  'file.format' = 'parquet',
  -- Keeps 10 snapshots maximum and expires older metadata automatically
  'num-sorted-run.max' = '5',
  'snapshot.num-retained.max' = '10',
  'snapshot.num-retained.min' = '5'
);


-- 3. Create the input/source Kafka CDC Table
-- This simulates upstream raw databases streaming mutated events through Kafka.
CREATE TEMPORARY TABLE kafka_cdc_source (
  user_id BIGINT,
  name STRING,
  email STRING,
  city STRING,
  updated_at BIGINT, -- Raw epoch millisecond timestamps
  -- Flink CDC automatically maps metadata details
  row_time AS TO_TIMESTAMP(FROM_UNIXTIME(updated_at / 1000))
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc-users-events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-paimon-cdc-consumer',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.fail-on-missing-field' = 'false'
);


-- 4. Start the Continuous Streaming Ingestion Pipeline
-- Since Paimon is an LSM-tree format, Flink SQL continuously upserts existing rows based on user_id key.
INSERT INTO users_paimon
SELECT
  user_id,
  name,
  email,
  city,
  row_time AS updated_at
FROM kafka_cdc_source;
