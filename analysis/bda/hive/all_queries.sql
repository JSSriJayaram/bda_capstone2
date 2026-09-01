-- Enable fast local execution mode for Native Hive
SET hive.exec.mode.local.auto=true;
SET hive.exec.mode.local.auto.inputbytes.max=134217728;
SET hive.exec.mode.local.auto.input.files.max=100;

USE taxi_analytics;

SELECT '=== Q1 — Total Number of Taxi Trips ===' AS query_title;
SELECT COUNT(*) AS total_trips FROM taxi_trips WHERE VendorID IS NOT NULL;

SELECT '=== Q2 — Total Revenue Generated ===' AS query_title;
SELECT ROUND(SUM(total_amount), 2) AS total_revenue FROM taxi_trips WHERE VendorID IS NOT NULL;

SELECT '=== Q3 — Platform-wide Averages ===' AS query_title;
SELECT 
    ROUND(AVG(fare_amount), 2) AS avg_fare, 
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles, 
    ROUND(AVG(trip_duration_min), 2) AS avg_duration_min, 
    ROUND(AVG(avg_speed_mph), 2) AS avg_speed_mph 
FROM taxi_trips 
WHERE VendorID IS NOT NULL;

SELECT '=== Q4 — Top 20 Busiest Zones ===' AS query_title;
SELECT PULocationID, COUNT(*) AS trip_count 
FROM taxi_trips 
WHERE VendorID IS NOT NULL 
GROUP BY PULocationID 
ORDER BY trip_count DESC 
LIMIT 20;

SELECT '=== Q5 — Top 20 Revenue Zones ===' AS query_title;
SELECT 
    PULocationID, 
    COUNT(*) AS trip_count, 
    ROUND(SUM(total_amount), 2) AS total_revenue, 
    ROUND(AVG(total_amount), 2) AS avg_revenue_per_trip 
FROM taxi_trips 
WHERE VendorID IS NOT NULL 
GROUP BY PULocationID 
ORDER BY total_revenue DESC 
LIMIT 20;

SELECT '=== Q6 — Top 10 Highest-Value Trips ===' AS query_title;
SELECT 
    PULocationID, 
    DOLocationID, 
    tpep_pickup_datetime, 
    ROUND(trip_distance, 2) AS distance_mi, 
    ROUND(trip_duration_min, 2) AS duration_min, 
    ROUND(fare_amount, 2) AS fare, 
    ROUND(tip_amount, 2) AS tip, 
    ROUND(total_amount, 2) AS total 
FROM taxi_trips 
WHERE VendorID IS NOT NULL 
ORDER BY total_amount DESC 
LIMIT 10;

SELECT '=== Q7 — Distance Statistics ===' AS query_title;
SELECT 
    MAX(trip_distance) AS max_dist, 
    MIN(trip_distance) AS min_dist, 
    ROUND(AVG(trip_distance), 2) AS avg_dist, 
    COUNT(*) AS total_trips 
FROM taxi_trips 
WHERE trip_distance > 0 AND VendorID IS NOT NULL;

SELECT '=== Q8 — Trip Demand by Pickup Hour ===' AS query_title;
SELECT 
    pickup_hour, 
    COUNT(*) AS trip_count, 
    ROUND(AVG(trip_duration_min), 2) AS avg_duration_min, 
    ROUND(AVG(total_amount), 2) AS avg_total_amount 
FROM taxi_trips 
WHERE VendorID IS NOT NULL 
GROUP BY pickup_hour 
ORDER BY pickup_hour;

SELECT '=== Q9 — Weekday vs Weekend Analysis ===' AS query_title;
SELECT 
    CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type, 
    COUNT(*) AS trip_count, 
    ROUND(AVG(total_amount), 2) AS avg_total_amount, 
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles, 
    ROUND(AVG(tip_amount), 2) AS avg_tip 
FROM taxi_trips 
WHERE VendorID IS NOT NULL 
GROUP BY is_weekend 
ORDER BY is_weekend;

SELECT '=== Q10 — Payment Type Analysis ===' AS query_title;
SELECT 
    CASE 
        WHEN payment_type = 1 THEN 'Credit Card' 
        WHEN payment_type = 2 THEN 'Cash' 
        WHEN payment_type = 3 THEN 'No Charge' 
        WHEN payment_type = 4 THEN 'Dispute' 
        ELSE 'Unknown' 
    END AS payment_method, 
    COUNT(*) AS trip_count, 
    ROUND(SUM(total_amount), 2) AS total_revenue, 
    ROUND(AVG(tip_amount), 2) AS avg_tip, 
    ROUND(AVG(total_amount), 2) AS avg_fare 
FROM taxi_trips 
WHERE VendorID IS NOT NULL 
GROUP BY payment_type 
ORDER BY trip_count DESC;

SELECT '=== Q11 — High-Volume Zones ===' AS query_title;
SELECT 
    PULocationID, 
    COUNT(*) AS trip_count, 
    ROUND(AVG(total_amount), 2) AS avg_total_amount, 
    ROUND(AVG(tip_amount), 2) AS avg_tip, 
    ROUND(SUM(total_amount), 2) AS total_revenue 
FROM taxi_trips 
WHERE VendorID IS NOT NULL 
GROUP BY PULocationID 
HAVING COUNT(*) >= 100
ORDER BY trip_count DESC;

SELECT '=== Q12 — Monthly Performance Breakdown ===' AS query_title;
SELECT 
    pickup_year, 
    pickup_month, 
    COUNT(*) AS trip_count, 
    ROUND(SUM(total_amount), 2) AS total_revenue, 
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles, 
    ROUND(AVG(trip_duration_min), 2) AS avg_duration_min, 
    ROUND(AVG(fare_amount), 2) AS avg_fare 
FROM taxi_trips 
WHERE VendorID IS NOT NULL 
GROUP BY pickup_year, pickup_month 
ORDER BY pickup_year, pickup_month;
