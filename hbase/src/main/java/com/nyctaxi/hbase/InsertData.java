package com.nyctaxi.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.ArrayList;
import java.util.List;

/**
 * STUDENT 2 — InsertData.java
 * =====================================================================
 * Project      : NYC Yellow Taxi HBase Analytics (BDA Review 2)
 * Dataset      : NYC TLC Yellow Taxi Trip Data, Jan 2026 (7.6M rows)
 * Domain       : Transportation Analytics
 * =====================================================================
 *
 * This program uses the HBase Java Table API to:
 *   1. Connect to HBase and open the 'nyc_taxi' table
 *   2. Build 12 Put objects from real NYC TLC dataset records
 *   3. Batch-insert all 12 Puts in a single table.put(List) call
 *   4. Print the inserted row keys with trip highlights
 *
 * Data Source:
 *   yellow_tripdata_cleaned.csv — NYC TLC Open Data (Jan 2026)
 *   https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
 *
 * Row Key format: {VendorID}_{pickup_datetime_compact}_{PULocationID}
 *   Example: 2_20260101005404_239
 *
 * HBase Write Path (internal):
 *   Put -> Write-Ahead Log (WAL on HDFS) -> MemStore -> HFile
 *   WAL ensures durability; MemStore is in-memory write buffer (128MB default)
 *
 * All values are stored as raw byte arrays using Bytes.toBytes().
 * HBase is schema-agnostic — no data types enforced at storage level.
 *
 * Run: java -cp nyc-taxi-hbase-analytics-1.0.jar com.nyctaxi.hbase.InsertData
 */
public class InsertData {

    static final String TABLE_NAME = "nyc_taxi";
    static final byte[] CF_TRIP    = Bytes.toBytes("trip_info");
    static final byte[] CF_FIN     = Bytes.toBytes("financials");
    static final byte[] CF_TIME    = Bytes.toBytes("timing");
    static final byte[] CF_METRICS = Bytes.toBytes("metrics");

    /** Compact datetime string: "2026-01-01 00:54:04" -> "20260101005404" */
    static String compactDT(String dt) {
        return dt.replace("-", "").replace(" ", "").replace(":", "");
    }

    /**
     * Builds a single HBase Put from one real NYC taxi trip record.
     * Maps each CSV field to its designated column family and qualifier.
     */
    static Put buildPut(String vendorID, String pickupDT, String dropoffDT,
                        String passengers, String distance, String puLoc, String doLoc,
                        String payType, String fare, String tip, String total,
                        String durationMin, String pickupHour, String dayName,
                        String speed, String farePerMile, String isWeekend, String congestion) {

        // Row Key: composite key for distributed storage and vendor prefix scans
        String rowKey = vendorID + "_" + compactDT(pickupDT) + "_" + puLoc;
        Put p = new Put(Bytes.toBytes(rowKey));

        // CF: trip_info — geographic and capacity fields
        p.addColumn(CF_TRIP, Bytes.toBytes("passenger_count"),  Bytes.toBytes(passengers));
        p.addColumn(CF_TRIP, Bytes.toBytes("trip_distance"),    Bytes.toBytes(distance));
        p.addColumn(CF_TRIP, Bytes.toBytes("PULocationID"),     Bytes.toBytes(puLoc));
        p.addColumn(CF_TRIP, Bytes.toBytes("DOLocationID"),     Bytes.toBytes(doLoc));
        p.addColumn(CF_TRIP, Bytes.toBytes("payment_type"),     Bytes.toBytes(payType));

        // CF: financials — all monetary amounts
        p.addColumn(CF_FIN, Bytes.toBytes("fare_amount"),            Bytes.toBytes(fare));
        p.addColumn(CF_FIN, Bytes.toBytes("tip_amount"),             Bytes.toBytes(tip));
        p.addColumn(CF_FIN, Bytes.toBytes("total_amount"),           Bytes.toBytes(total));
        p.addColumn(CF_FIN, Bytes.toBytes("congestion_surcharge"),   Bytes.toBytes(congestion));

        // CF: timing — all temporal fields
        p.addColumn(CF_TIME, Bytes.toBytes("pickup_datetime"),    Bytes.toBytes(pickupDT));
        p.addColumn(CF_TIME, Bytes.toBytes("dropoff_datetime"),   Bytes.toBytes(dropoffDT));
        p.addColumn(CF_TIME, Bytes.toBytes("trip_duration_min"),  Bytes.toBytes(durationMin));
        p.addColumn(CF_TIME, Bytes.toBytes("pickup_hour"),        Bytes.toBytes(pickupHour));
        p.addColumn(CF_TIME, Bytes.toBytes("pickup_day_name"),    Bytes.toBytes(dayName));

        // CF: metrics — computed analytics fields (derived from raw CSV data)
        p.addColumn(CF_METRICS, Bytes.toBytes("avg_speed_mph"), Bytes.toBytes(speed));
        p.addColumn(CF_METRICS, Bytes.toBytes("fare_per_mile"), Bytes.toBytes(farePerMile));
        p.addColumn(CF_METRICS, Bytes.toBytes("is_weekend"),    Bytes.toBytes(isWeekend));

        return p;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(line('=', 65));
        System.out.println("   STUDENT 2: Data Ingestion — 12 Real NYC Taxi Records");
        System.out.println("   Source  : NYC TLC Yellow Taxi Trip Data (Jan 2026)");
        System.out.println(line('=', 65));

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {

            List<Put> puts = new ArrayList<>();

            // -------------------------------------------------------
            // 12 real records from yellow_tripdata_cleaned.csv
            // Format: vendorID, pickupDT, dropoffDT, passengers,
            //         distance, puLoc, doLoc, payType,
            //         fare, tip, total, durationMin, pickupHour, dayName,
            //         speed, farePerMile, isWeekend, congestion
            // -------------------------------------------------------

            // Vendor 1 trips (VeriFone Inc.)
            puts.add(buildPut("1","2026-01-01 00:40:35","2026-01-01 00:45:12","1","0.5","161","229","4","0.0","0.0","0.0","4.62","0","Thursday","6.49","0.0","0","0.0"));
            puts.add(buildPut("1","2026-01-01 00:23:16","2026-01-01 00:37:18","2","2.6","48","151","1","14.9","0.0","20.65","14.03","0","Thursday","11.12","7.94","0","2.5"));
            puts.add(buildPut("1","2026-01-01 00:41:53","2026-01-01 01:23:56","1","10.8","142","189","1","47.8","13.4","66.95","42.05","0","Thursday","15.41","6.2","0","2.5"));
            puts.add(buildPut("1","2026-01-01 00:38:54","2026-01-01 01:28:40","1","9.4","170","64","1","43.5","0.0","44.0","49.77","0","Thursday","11.33","4.68","0","0.0"));

            // Vendor 2 trips (Creative Mobile Technologies)
            puts.add(buildPut("2","2026-01-01 00:23:38","2026-01-01 00:29:35","1","1.56","239","151","1","9.3","4.29","18.59","5.95","0","Thursday","15.73","11.92","0","2.5"));
            puts.add(buildPut("2","2026-01-01 00:07:08","2026-01-01 00:18:18","3","1.73","239","236","1","12.1","1.71","18.81","11.17","0","Thursday","9.29","10.87","0","2.5"));
            puts.add(buildPut("2","2026-01-01 00:10:16","2026-01-01 00:14:26","1","0.84","249","68","2","6.5","0.0","12.25","4.17","0","Thursday","12.09","14.58","0","2.5"));
            puts.add(buildPut("2","2026-01-01 00:10:55","2026-01-01 00:19:41","1","1.94","239","236","1","12.1","3.42","20.52","8.77","0","Thursday","13.27","10.58","0","2.5"));
            puts.add(buildPut("2","2026-01-01 00:58:17","2026-01-01 01:09:57","2","1.94","79","233","1","12.8","3.71","22.26","11.67","0","Thursday","9.97","11.47","0","2.5"));
            puts.add(buildPut("2","2026-01-01 00:19:31","2026-01-01 00:46:09","3","7.01","137","225","1","32.4","9.02","54.11","26.63","0","Thursday","15.79","7.72","0","2.5"));
            puts.add(buildPut("2","2026-01-01 00:36:29","2026-01-01 00:41:04","1","0.6","143","239","1","6.5","2.0","13.5","4.58","0","Thursday","7.86","22.5","0","2.5"));
            puts.add(buildPut("2","2026-01-01 00:30:46","2026-01-01 00:36:58","1","0.64","161","237","1","7.9","2.73","16.38","6.2","0","Thursday","6.19","25.59","0","2.5"));

            // Batch insert all 12 records in a single RPC call
            table.put(puts);

            System.out.println("[SUCCESS] 12 real NYC taxi trip records inserted into '" + TABLE_NAME + "'");
            System.out.println(line('-', 65));
            System.out.println("  Inserted Row Keys (sample):");
            System.out.println("    1_20260101004035_161  (Vendor 1, $0.0 total — zero-fare record)");
            System.out.println("    1_20260101002316_48   (Vendor 1, 2.6 mi, $20.65 total)");
            System.out.println("    1_20260101004153_142  (Vendor 1, 10.8 mi, $66.95 — long trip)");
            System.out.println("    1_20260101003854_170  (Vendor 1, 9.4 mi, $44.0 total)");
            System.out.println("    2_20260101002338_239  (Vendor 2, 1.56 mi, $18.59)");
            System.out.println("    2_20260101000708_239  (Vendor 2, 3 passengers, $18.81)");
            System.out.println("    2_20260101001016_249  (Vendor 2, Cash payment, $12.25)");
            System.out.println("    2_20260101001055_239  (Vendor 2, $20.52)");
            System.out.println("    2_20260101005817_79   (Vendor 2, 2 passengers, $22.26)");
            System.out.println("    2_20260101001931_137  (Vendor 2, 3 pax, 7.01 mi, $54.11 — group)");
            System.out.println("    2_20260101003629_143  (Vendor 2, 0.6 mi, $13.5)");
            System.out.println("    2_20260101003046_161  (Vendor 2, $16.38)");
            System.out.println(line('-', 65));
            System.out.println("  [NOTE] Row Key = {VendorID}_{pickup_datetime}_{PULocationID}");
            System.out.println("  [NOTE] All values stored as Bytes.toBytes() — raw byte arrays");
            System.out.println(line('=', 65));
        }
    }

    static String line(char ch, int n) {
        char[] c = new char[n];
        java.util.Arrays.fill(c, ch);
        return new String(c);
    }
}
