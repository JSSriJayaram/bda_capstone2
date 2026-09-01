-- ============================================================
--   taxi_analytics.sql
--   Setup Database & Table for NYC Taxi Analytics (Hive)
--   Dataset: yellow_tripdata_sample_for_hadoop.csv
-- ============================================================

-- Drop and recreate database
DROP DATABASE IF EXISTS taxi_analytics CASCADE;
CREATE DATABASE taxi_analytics;
USE taxi_analytics;

-- Create the main trips table (matches CSV column order)
CREATE TABLE IF NOT EXISTS taxi_trips (
    VendorID              INT,
    tpep_pickup_datetime  STRING,
    tpep_dropoff_datetime STRING,
    passenger_count       DOUBLE,
    trip_distance         DOUBLE,
    RatecodeID            DOUBLE,
    store_and_fwd_flag    STRING,
    PULocationID          INT,
    DOLocationID          INT,
    payment_type          INT,
    fare_amount           DOUBLE,
    extra                 DOUBLE,
    mta_tax               DOUBLE,
    tip_amount            DOUBLE,
    tolls_amount          DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount          DOUBLE,
    congestion_surcharge  DOUBLE,
    Airport_fee           DOUBLE,
    cbd_congestion_fee    DOUBLE,
    trip_duration_min     DOUBLE,
    pickup_date           STRING,
    pickup_hour           INT,
    pickup_day            INT,
    pickup_month          INT,
    pickup_year           INT,
    pickup_dayofweek      INT,
    pickup_day_name       STRING,
    is_weekend            INT,
    avg_speed_mph         DOUBLE,
    fare_per_mile         DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Load data from local CSV
LOAD DATA LOCAL INPATH '/home/real/bda/yellow_tripdata_sample_for_hadoop.csv'
OVERWRITE INTO TABLE taxi_trips;
