package com.nyctaxi.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * STUDENT 1 — CreateTable.java
 * =====================================================================
 * Project      : NYC Yellow Taxi HBase Analytics (BDA Review 2)
 * Dataset      : NYC TLC Yellow Taxi Trip Data, Jan 2026 (7.6M rows)
 * Domain       : Transportation Analytics
 * =====================================================================
 *
 * This program uses the HBase Java Admin API to:
 *   1. Connect to HBase via ZooKeeper
 *   2. Drop the existing 'nyc_taxi' table (if present)
 *   3. Create the table with 4 carefully designed Column Families:
 *      - trip_info  : trip metadata (distance, passengers, location IDs)
 *      - financials : fare, tip, payment type, congestion surcharge
 *      - timing     : pickup/dropoff timestamps, duration, hour, day name
 *      - metrics    : derived analytics (avg_speed_mph, fare_per_mile, is_weekend)
 *   4. Print a structured summary of the schema
 *
 * Row Key Design: {VendorID}_{pickup_datetime_compact}_{PULocationID}
 *   Example: 2_20260101005404_239
 *   Rationale:
 *     - Enables prefix scans by VendorID (vendor analytics)
 *     - Enables range scans by date (time-range analytics)
 *     - Prevents region hot-spotting (distributed across vendors + times)
 *
 * Column Family Rationale:
 *   - trip_info  : queried for geographic and capacity analytics
 *   - financials : queried for payment and revenue analytics
 *   - timing     : queried for temporal and peak-hour analytics
 *   - metrics    : queried for performance dashboards
 *   Separating into 4 CFs enables column-family locality — each query
 *   reads only the relevant HFiles from disk, reducing I/O significantly.
 *
 * Run: java -cp nyc-taxi-hbase-analytics-1.0.jar com.nyctaxi.hbase.CreateTable
 */
public class CreateTable {

    static final String TABLE_NAME = "nyc_taxi";
    static final byte[] CF_TRIP_INFO  = Bytes.toBytes("trip_info");
    static final byte[] CF_FINANCIALS = Bytes.toBytes("financials");
    static final byte[] CF_TIMING     = Bytes.toBytes("timing");
    static final byte[] CF_METRICS    = Bytes.toBytes("metrics");

    public static void main(String[] args) throws Exception {
        System.out.println(line('=', 65));
        System.out.println("   STUDENT 1: HBase Table Creation — NYC Taxi Analytics");
        System.out.println("   Dataset : NYC TLC Yellow Taxi Trip Data (Jan 2026)");
        System.out.println(line('=', 65));

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin()) {

            TableName tableName = TableName.valueOf(TABLE_NAME);

            // Drop existing table to allow clean re-creation
            if (admin.tableExists(tableName)) {
                System.out.println("[INFO] Table '" + TABLE_NAME + "' already exists. Dropping...");
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("[INFO] Old table dropped successfully.");
            }

            // Build TableDescriptor with 4 Column Families
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);

            // CF1: trip_info — core trip metadata, geographic data
            tdb.setColumnFamily(
                ColumnFamilyDescriptorBuilder.newBuilder(CF_TRIP_INFO)
                    .setMaxVersions(1)
                    .build()
            );

            // CF2: financials — fare, tip, payment type, tolls
            tdb.setColumnFamily(
                ColumnFamilyDescriptorBuilder.newBuilder(CF_FINANCIALS)
                    .setMaxVersions(1)
                    .build()
            );

            // CF3: timing — pickup/dropoff datetimes, duration, temporal fields
            tdb.setColumnFamily(
                ColumnFamilyDescriptorBuilder.newBuilder(CF_TIMING)
                    .setMaxVersions(1)
                    .build()
            );

            // CF4: metrics — computed analytics (speed, fare/mile, weekend flag)
            tdb.setColumnFamily(
                ColumnFamilyDescriptorBuilder.newBuilder(CF_METRICS)
                    .setMaxVersions(1)
                    .build()
            );

            admin.createTable(tdb.build());

            System.out.println("[SUCCESS] Table '" + TABLE_NAME + "' created with 4 column families:");
            System.out.println(line('-', 65));
            System.out.println("  CF1 trip_info  : passenger_count, trip_distance, PULocationID,");
            System.out.println("                   DOLocationID, payment_type");
            System.out.println("  CF2 financials : fare_amount, tip_amount, total_amount,");
            System.out.println("                   congestion_surcharge");
            System.out.println("  CF3 timing     : pickup_datetime, dropoff_datetime,");
            System.out.println("                   trip_duration_min, pickup_hour, pickup_day_name");
            System.out.println("  CF4 metrics    : avg_speed_mph, fare_per_mile, is_weekend");
            System.out.println(line('-', 65));
            System.out.println("  Row Key Format : {VendorID}_{pickup_datetime}_{PULocationID}");
            System.out.println("  Example Key    : 2_20260101005404_239");
            System.out.println(line('=', 65));
        }
    }

    static String line(char ch, int n) {
        char[] c = new char[n];
        java.util.Arrays.fill(c, ch);
        return new String(c);
    }
}
