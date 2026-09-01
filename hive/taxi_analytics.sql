-- ============================================================
-- NYC Yellow Taxi Analytics Pipeline
-- Apache Hive 3.1.3 + MapReduce Engine
-- Dataset: ~7.6M records, Jan-Mar 2026, 31 columns
-- ============================================================

-- ============================================================
-- STEP 1: Create database
-- ============================================================

CREATE DATABASE IF NOT EXISTS taxi_analytics
COMMENT 'NYC Yellow Taxi Analytics — Jan-Mar 2026';

USE taxi_analytics;

-- ============================================================
-- STEP 2: Create External Staging Table (reads raw CSV from HDFS)
-- All columns as STRING to safely ingest raw data
-- HDFS path: /user/bda/taxi/clean/
-- ============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS taxi_trips_raw (
    VendorID              STRING,
    tpep_pickup_datetime  STRING,
    tpep_dropoff_datetime STRING,
    passenger_count       STRING,
    trip_distance         STRING,
    RatecodeID            STRING,
    store_and_fwd_flag    STRING,
    PULocationID          STRING,
    DOLocationID          STRING,
    payment_type          STRING,
    fare_amount           STRING,
    extra                 STRING,
    mta_tax               STRING,
    tip_amount            STRING,
    tolls_amount          STRING,
    improvement_surcharge STRING,
    total_amount          STRING,
    congestion_surcharge  STRING,
    Airport_fee           STRING,
    cbd_congestion_fee    STRING,
    trip_duration_min     STRING,
    pickup_date           STRING,
    pickup_hour           STRING,
    pickup_day            STRING,
    pickup_month          STRING,
    pickup_year           STRING,
    pickup_dayofweek      STRING,
    pickup_day_name       STRING,
    is_weekend            STRING,
    avg_speed_mph         STRING,
    fare_per_mile         STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\"",
    "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/user/bda/taxi/clean/'
TBLPROPERTIES ("skip.header.line.count" = "1");

-- ============================================================
-- STEP 3: Create Managed ORC Table (typed, columnar, compressed)
-- Data is cast from raw strings into proper Hive types
-- ============================================================

CREATE TABLE IF NOT EXISTS taxi_trips
STORED AS ORC
TBLPROPERTIES ("orc.compress" = "SNAPPY")
AS
SELECT
    CAST(VendorID             AS INT)    AS VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    CAST(passenger_count       AS DOUBLE) AS passenger_count,
    CAST(trip_distance         AS DOUBLE) AS trip_distance,
    CAST(RatecodeID            AS DOUBLE) AS RatecodeID,
    store_and_fwd_flag,
    CAST(PULocationID          AS INT)    AS PULocationID,
    CAST(DOLocationID          AS INT)    AS DOLocationID,
    CAST(payment_type          AS INT)    AS payment_type,
    CAST(fare_amount           AS DOUBLE) AS fare_amount,
    CAST(extra                 AS DOUBLE) AS extra,
    CAST(mta_tax               AS DOUBLE) AS mta_tax,
    CAST(tip_amount            AS DOUBLE) AS tip_amount,
    CAST(tolls_amount          AS DOUBLE) AS tolls_amount,
    CAST(improvement_surcharge AS DOUBLE) AS improvement_surcharge,
    CAST(total_amount          AS DOUBLE) AS total_amount,
    CAST(congestion_surcharge  AS DOUBLE) AS congestion_surcharge,
    CAST(Airport_fee           AS DOUBLE) AS Airport_fee,
    CAST(cbd_congestion_fee    AS DOUBLE) AS cbd_congestion_fee,
    CAST(trip_duration_min     AS DOUBLE) AS trip_duration_min,
    pickup_date,
    CAST(pickup_hour           AS INT)    AS pickup_hour,
    CAST(pickup_day            AS INT)    AS pickup_day,
    CAST(pickup_month          AS INT)    AS pickup_month,
    CAST(pickup_year           AS INT)    AS pickup_year,
    CAST(pickup_dayofweek      AS INT)    AS pickup_dayofweek,
    pickup_day_name,
    CAST(is_weekend            AS INT)    AS is_weekend,
    CAST(avg_speed_mph         AS DOUBLE) AS avg_speed_mph,
    CAST(fare_per_mile         AS DOUBLE) AS fare_per_mile
FROM taxi_trips_raw
WHERE VendorID IS NOT NULL
  AND VendorID != 'VendorID';

-- ============================================================
-- ANALYTICAL QUERIES (Q1 - Q12)
-- ============================================================

-- Q1: Total number of taxi trips
SELECT 'Q1: Total Taxi Trips' AS query_title;
SELECT COUNT(*) AS total_trips FROM taxi_trips;

-- Q2: Total revenue generated across all trips
SELECT 'Q2: Total Revenue Generated' AS query_title;
SELECT ROUND(SUM(total_amount), 2) AS total_revenue FROM taxi_trips;

-- Q3: Average fare, trip distance, and trip duration
SELECT 'Q3: Platform-wide Averages' AS query_title;
SELECT
    ROUND(AVG(fare_amount),      2) AS avg_fare,
    ROUND(AVG(trip_distance),    2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_min),2) AS avg_duration_min,
    ROUND(AVG(avg_speed_mph),    2) AS avg_speed_mph
FROM taxi_trips;

-- Q4: Trip volume by pickup location (top 20 busiest zones)
SELECT 'Q4: Trip Volume by Pickup Zone (Top 20)' AS query_title;
SELECT
    PULocationID,
    COUNT(*) AS trip_count
FROM taxi_trips
GROUP BY PULocationID
ORDER BY trip_count DESC
LIMIT 20;

-- Q5: Revenue by pickup location (top 20 highest-earning zones)
SELECT 'Q5: Revenue by Pickup Zone (Top 20)' AS query_title;
SELECT
    PULocationID,
    COUNT(*)                          AS trip_count,
    ROUND(SUM(total_amount),    2)    AS total_revenue,
    ROUND(AVG(total_amount),    2)    AS avg_revenue_per_trip
FROM taxi_trips
GROUP BY PULocationID
ORDER BY total_revenue DESC
LIMIT 20;

-- Q6: Top 10 highest-value individual trips
SELECT 'Q6: Top 10 Highest-Value Trips' AS query_title;
SELECT
    PULocationID,
    DOLocationID,
    tpep_pickup_datetime,
    ROUND(trip_distance,    2) AS trip_distance_miles,
    ROUND(trip_duration_min,2) AS trip_duration_min,
    ROUND(fare_amount,      2) AS fare_amount,
    ROUND(tip_amount,       2) AS tip_amount,
    ROUND(total_amount,     2) AS total_amount
FROM taxi_trips
ORDER BY total_amount DESC
LIMIT 10;

-- Q7: Maximum and minimum trip distance statistics
SELECT 'Q7: Trip Distance Statistics' AS query_title;
SELECT
    MAX(trip_distance)            AS max_distance_miles,
    MIN(trip_distance)            AS min_distance_miles,
    ROUND(AVG(trip_distance), 2)  AS avg_distance_miles,
    COUNT(*)                      AS total_trips
FROM taxi_trips
WHERE trip_distance > 0;

-- Q8: Trip demand and average duration by pickup hour
SELECT 'Q8: Trip Demand by Pickup Hour' AS query_title;
SELECT
    pickup_hour,
    COUNT(*)                           AS trip_count,
    ROUND(AVG(trip_duration_min), 2)   AS avg_duration_min,
    ROUND(AVG(total_amount),      2)   AS avg_total_amount
FROM taxi_trips
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- Q9: Weekday vs Weekend trip analysis
SELECT 'Q9: Weekday vs Weekend Analysis' AS query_title;
SELECT
    CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(total_amount),   2)   AS avg_total_amount,
    ROUND(AVG(trip_distance),  2)   AS avg_distance_miles,
    ROUND(AVG(tip_amount),     2)   AS avg_tip
FROM taxi_trips
GROUP BY is_weekend
ORDER BY is_weekend;

-- Q10: Payment type analysis (revenue and tip behaviour)
SELECT 'Q10: Payment Type Analysis' AS query_title;
SELECT
    CASE
        WHEN payment_type = 1 THEN 'Credit Card'
        WHEN payment_type = 2 THEN 'Cash'
        WHEN payment_type = 3 THEN 'No Charge'
        WHEN payment_type = 4 THEN 'Dispute'
        ELSE 'Unknown'
    END                               AS payment_method,
    COUNT(*)                          AS trip_count,
    ROUND(SUM(total_amount),    2)    AS total_revenue,
    ROUND(AVG(tip_amount),      2)    AS avg_tip,
    ROUND(AVG(total_amount),    2)    AS avg_fare
FROM taxi_trips
GROUP BY payment_type
ORDER BY trip_count DESC;

-- Q11: High-volume pickup zones (more than 10,000 trips)
SELECT 'Q11: High-Volume Pickup Zones (>10000 trips)' AS query_title;
SELECT
    PULocationID,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(total_amount),   2)   AS avg_total_amount,
    ROUND(AVG(tip_amount),     2)   AS avg_tip,
    ROUND(SUM(total_amount),   2)   AS total_revenue
FROM taxi_trips
GROUP BY PULocationID
HAVING COUNT(*) > 10000
ORDER BY trip_count DESC;

-- Q12: Monthly taxi performance (Jan=1, Feb=2, Mar=3)
SELECT 'Q12: Monthly Performance Breakdown' AS query_title;
SELECT
    pickup_year,
    pickup_month,
    COUNT(*)                         AS trip_count,
    ROUND(SUM(total_amount),    2)   AS total_revenue,
    ROUND(AVG(trip_distance),   2)   AS avg_distance_miles,
    ROUND(AVG(trip_duration_min),2)  AS avg_duration_min,
    ROUND(AVG(fare_amount),     2)   AS avg_fare
FROM taxi_trips
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month;
