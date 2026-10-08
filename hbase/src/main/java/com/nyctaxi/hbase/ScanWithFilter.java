package com.nyctaxi.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.Arrays;

/**
 * STUDENT 4 — ScanWithFilter.java
 * =====================================================================
 * Project      : NYC Yellow Taxi HBase Analytics (BDA Review 2)
 * Dataset      : NYC TLC Yellow Taxi Trip Data, Jan 2026 (7.6M rows)
 * Domain       : Transportation Analytics
 * =====================================================================
 *
 * This program demonstrates 5 distinct HBase Server-Side Scan Filters:
 *
 *   FILTER 1 — SingleColumnValueFilter (SIMPLE)
 *              Find trips paid by Credit Card (payment_type = "1")
 *              Business Use: Revenue team isolates digital payment trips
 *
 *   FILTER 2 — SingleColumnValueFilter for no-tip detection (SIMPLE)
 *              Find trips with tip_amount = "0.0" (driver got no tip)
 *              Business Use: Driver earnings audit — fairness analysis
 *
 *   FILTER 3 — PrefixFilter (MODERATE)
 *              Retrieve all Vendor 2 trips (row key prefix = "2_")
 *              Business Use: Vendor performance comparison
 *
 *   FILTER 4 — ColumnPrefixFilter (MODERATE)
 *              Return only columns starting with "fare" across all families
 *              (matches fare_amount in financials, fare_per_mile in metrics)
 *              Business Use: Fare analytics report — only revenue columns
 *
 *   FILTER 5 — PageFilter (ADVANCED)
 *              Server-side pagination — return first 3 rows only
 *              Business Use: Display first page of trips in dashboard UI
 *
 * KEY INSIGHT: All filters execute on the RegionServer (server-side).
 *   Only matching rows/columns are transmitted across the network to the
 *   client — drastically reducing network I/O for large dataset queries.
 *   This is the major advantage of HBase filter push-down.
 *
 * Run (default): java -jar nyc-taxi-hbase-analytics-1.0.jar
 * Run (explicit): java -cp nyc-taxi-hbase-analytics-1.0.jar com.nyctaxi.hbase.ScanWithFilter
 */
public class ScanWithFilter {

    static final String TABLE_NAME = "nyc_taxi";
    static final byte[] CF_FIN  = Bytes.toBytes("financials");
    static final byte[] CF_TRIP = Bytes.toBytes("trip_info");
    static final byte[] CF_TIME = Bytes.toBytes("timing");
    static final byte[] CF_MET  = Bytes.toBytes("metrics");

    public static void main(String[] args) throws Exception {
        char[] eq = new char[65]; Arrays.fill(eq, '=');
        char[] dash = new char[65]; Arrays.fill(dash, '-');
        String EQ = new String(eq);
        String DASH = new String(dash);

        System.out.println(EQ);
        System.out.println("   STUDENT 4: HBase Scan with 5 Filters — NYC Taxi Analytics");
        System.out.println("   Dataset : NYC TLC Yellow Taxi Trip Data (Jan 2026)");
        System.out.println("   All filters execute SERVER-SIDE on the RegionServer.");
        System.out.println(EQ);

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {

            // ================================================================
            // FILTER 1 [SIMPLE]: SingleColumnValueFilter — Credit Card Trips
            // Finds all rows where trip_info:payment_type equals "1"
            // In NYC TLC data: 1=Credit Card, 2=Cash, 3=No Charge, 4=Dispute
            // setFilterIfMissing(true) skips rows that don't have the column
            // ================================================================
            System.out.println("\n" + EQ);
            System.out.println("FILTER 1 [SIMPLE]: SingleColumnValueFilter");
            System.out.println("Goal   : Find trips paid by Credit Card (payment_type = '1')");
            System.out.println("Use    : Revenue team isolates digital payment trips");
            System.out.println(DASH);

            Scan scan1 = new Scan();
            SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
                CF_TRIP,
                Bytes.toBytes("payment_type"),
                CompareOp.EQUAL,
                Bytes.toBytes("1")
            );
            filter1.setFilterIfMissing(true);
            scan1.setFilter(filter1);

            int count1 = 0;
            try (ResultScanner rs1 = table.getScanner(scan1)) {
                for (Result r : rs1) {
                    String rowKey = Bytes.toString(r.getRow());
                    String fare   = Bytes.toString(r.getValue(CF_FIN,  Bytes.toBytes("fare_amount")));
                    String tip    = Bytes.toString(r.getValue(CF_FIN,  Bytes.toBytes("tip_amount")));
                    String total  = Bytes.toString(r.getValue(CF_FIN,  Bytes.toBytes("total_amount")));
                    System.out.printf("  Row: %-35s  Fare: $%-6s  Tip: $%-5s  Total: $%s%n",
                        rowKey, fare, tip, total);
                    count1++;
                }
            }
            System.out.println(DASH);
            System.out.println("[RESULT] Credit card trips found: " + count1);

            // ================================================================
            // FILTER 2 [SIMPLE]: SingleColumnValueFilter — No-Tip Trips
            // Finds all rows where financials:tip_amount equals "0.0"
            // Identifies trips where drivers received no tip from passengers
            // ================================================================
            System.out.println("\n" + EQ);
            System.out.println("FILTER 2 [SIMPLE]: SingleColumnValueFilter — Zero Tip");
            System.out.println("Goal   : Find trips where tip_amount = '0.0' (no tip given)");
            System.out.println("Use    : Driver earnings fairness audit");
            System.out.println(DASH);

            Scan scan2 = new Scan();
            SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
                CF_FIN,
                Bytes.toBytes("tip_amount"),
                CompareOp.EQUAL,
                Bytes.toBytes("0.0")
            );
            filter2.setFilterIfMissing(true);
            scan2.setFilter(filter2);

            int count2 = 0;
            try (ResultScanner rs2 = table.getScanner(scan2)) {
                for (Result r : rs2) {
                    String rowKey  = Bytes.toString(r.getRow());
                    String total   = Bytes.toString(r.getValue(CF_FIN,  Bytes.toBytes("total_amount")));
                    String payType = Bytes.toString(r.getValue(CF_TRIP, Bytes.toBytes("payment_type")));
                    String dayName = Bytes.toString(r.getValue(CF_TIME, Bytes.toBytes("pickup_day_name")));
                    System.out.printf("  Row: %-35s  Total: $%-7s  PayType: %s  Day: %s%n",
                        rowKey, total, payType, dayName);
                    count2++;
                }
            }
            System.out.println(DASH);
            System.out.println("[RESULT] No-tip trips found: " + count2);

            // ================================================================
            // FILTER 3 [MODERATE]: PrefixFilter — Vendor 2 Trips
            // Scans only rows whose key starts with "2_"
            // HBase's sorted key order makes this VERY efficient —
            // the scanner stops as soon as it sees a row key that doesn't match.
            // ================================================================
            System.out.println("\n" + EQ);
            System.out.println("FILTER 3 [MODERATE]: PrefixFilter");
            System.out.println("Goal   : Retrieve only Vendor 2 trips (row key starts with '2_')");
            System.out.println("Use    : Vendor performance comparison & segmentation");
            System.out.println(DASH);

            Scan scan3 = new Scan();
            scan3.setFilter(new PrefixFilter(Bytes.toBytes("2_")));

            int count3 = 0;
            try (ResultScanner rs3 = table.getScanner(scan3)) {
                for (Result r : rs3) {
                    String rowKey   = Bytes.toString(r.getRow());
                    String distance = Bytes.toString(r.getValue(CF_TRIP, Bytes.toBytes("trip_distance")));
                    String speed    = Bytes.toString(r.getValue(CF_MET,  Bytes.toBytes("avg_speed_mph")));
                    String total    = Bytes.toString(r.getValue(CF_FIN,  Bytes.toBytes("total_amount")));
                    System.out.printf("  Row: %-35s  Dist: %-5s mi  Speed: %-6s mph  Total: $%s%n",
                        rowKey, distance, speed, total);
                    count3++;
                }
            }
            System.out.println(DASH);
            System.out.println("[RESULT] Vendor 2 trips found: " + count3);

            // ================================================================
            // FILTER 4 [MODERATE]: ColumnPrefixFilter — Fare Columns Only
            // Returns ONLY column qualifiers starting with "fare" across all CFs
            // Matches: financials:fare_amount, metrics:fare_per_mile
            // Server-side column projection — reduces data sent over network
            // ================================================================
            System.out.println("\n" + EQ);
            System.out.println("FILTER 4 [MODERATE]: ColumnPrefixFilter");
            System.out.println("Goal   : Retrieve only columns starting with 'fare'");
            System.out.println("         (fare_amount in financials, fare_per_mile in metrics)");
            System.out.println("Use    : Fare analytics report — project only revenue columns");
            System.out.println(DASH);

            Scan scan4 = new Scan();
            scan4.setFilter(new ColumnPrefixFilter(Bytes.toBytes("fare")));

            int count4 = 0;
            try (ResultScanner rs4 = table.getScanner(scan4)) {
                for (Result r : rs4) {
                    String rowKey    = Bytes.toString(r.getRow());
                    String fareAmt   = Bytes.toString(r.getValue(CF_FIN, Bytes.toBytes("fare_amount")));
                    String fareMile  = Bytes.toString(r.getValue(CF_MET, Bytes.toBytes("fare_per_mile")));
                    System.out.printf("  Row: %-35s  fare_amount: $%-6s  fare_per_mile: $%s%n",
                        rowKey,
                        fareAmt  != null ? fareAmt  : "N/A",
                        fareMile != null ? fareMile : "N/A");
                    count4++;
                }
            }
            System.out.println(DASH);
            System.out.println("[RESULT] Rows with 'fare' prefix columns: " + count4);

            // ================================================================
            // FILTER 5 [ADVANCED]: PageFilter — Server-Side Pagination
            // Returns ONLY the first 3 rows; pagination limit enforced on server.
            // Real-world use: UI dashboard shows "Page 1 of results"
            // The PageFilter ensures only 3 rows are transmitted to client.
            // ================================================================
            System.out.println("\n" + EQ);
            System.out.println("FILTER 5 [ADVANCED]: PageFilter");
            System.out.println("Goal   : Return first 3 rows only (server-side page limit)");
            System.out.println("Use    : Trip dashboard — display first page of results");
            System.out.println("Note   : PageFilter evaluated on RegionServer — client never");
            System.out.println("         sees rows beyond the limit (no excess data transferred)");
            System.out.println(DASH);

            Scan scan5 = new Scan();
            scan5.setFilter(new PageFilter(3));

            int count5 = 0;
            try (ResultScanner rs5 = table.getScanner(scan5)) {
                for (Result r : rs5) {
                    String rowKey  = Bytes.toString(r.getRow());
                    String pickup  = Bytes.toString(r.getValue(CF_TIME, Bytes.toBytes("pickup_datetime")));
                    String dropoff = Bytes.toString(r.getValue(CF_TIME, Bytes.toBytes("dropoff_datetime")));
                    String total   = Bytes.toString(r.getValue(CF_FIN,  Bytes.toBytes("total_amount")));
                    System.out.printf("  Row: %-35s  Pickup: %-22s  Dropoff: %-22s  Total: $%s%n",
                        rowKey, pickup, dropoff, total);
                    count5++;
                }
            }
            System.out.println(DASH);
            System.out.println("[RESULT] Rows returned (Page 1): " + count5 + "  [PageFilter limit = 3]");

            // ================================================================
            // FILTER RESULTS SUMMARY
            // ================================================================
            System.out.println("\n" + EQ);
            System.out.println("                     FILTER RESULTS SUMMARY");
            System.out.println(EQ);
            System.out.printf("  Filter 1 [SIMPLE]   SingleColumnValueFilter (Credit Card)  : %d rows%n", count1);
            System.out.printf("  Filter 2 [SIMPLE]   SingleColumnValueFilter (Zero Tip)     : %d rows%n", count2);
            System.out.printf("  Filter 3 [MODERATE] PrefixFilter (Vendor 2 trips)          : %d rows%n", count3);
            System.out.printf("  Filter 4 [MODERATE] ColumnPrefixFilter (fare columns)      : %d rows%n", count4);
            System.out.printf("  Filter 5 [ADVANCED] PageFilter (first 3 rows, page 1)      : %d rows%n", count5);
            System.out.println(EQ);
        }
    }
}
