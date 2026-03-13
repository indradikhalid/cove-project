-- DDL for BigQuery raw tables
-- Replace `sandbox-adi` with your actual GCP project ID

CREATE SCHEMA IF NOT EXISTS `sandbox-adi.cove_staging`
OPTIONS (location = 'US');

CREATE OR REPLACE TABLE `sandbox-adi.cove_staging.properties` (
  _id STRING,
  name STRING,
  city STRING,
  lease_start_date STRING,
  lease_end_date STRING,
  updatedAt STRING,
  deletedAt STRING
);

CREATE OR REPLACE TABLE `sandbox-adi.cove_staging.rooms` (
  _id STRING,
  propertyId STRING,
  room_number STRING,
  type STRING,
  updatedAt STRING,
  deletedAt STRING
);

CREATE OR REPLACE TABLE `sandbox-adi.cove_staging.tenancies` (
  _id STRING,
  roomId STRING,
  tenant_id STRING,
  checkInDate STRING,
  checkOutDate STRING,
  status STRING,
  updatedAt STRING
);
