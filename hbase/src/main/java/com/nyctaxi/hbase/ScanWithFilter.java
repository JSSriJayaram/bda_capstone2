package com.nyctaxi.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * STUDENT 4 — ScanWithFilter.java
 * =====================================================================
 * Project      : NYC Yellow Taxi HBase Analytics (BDA Review 2)
 * Dataset      : NYC TLC Yellow Taxi Trip Data, Jan 2026 (7.6M rows)
 * =====================================================================
 *
 * ANALYTICS QUERY: Multi-Filter Revenue & Trip Quality Dashboard
 * --------------------------------------------------------------
 * Executes 4 progressive analytics queries using HBase server-side filters:
 *
 *   QUERY 1 — Credit Card Revenue Report
 *             Filter: SingleColumnValueFilter (payment_type = "1")
 *             Analytics: Sum total revenue from credit card payments
 *
 *   QUERY 2 — Fast Trip Detector (Speed > 12 mph)
 *             Filter: SingleColumnValueFilter (avg_speed_mph > "12")
 *             Analytics: Find trips that finished quickly — high efficiency
 *
 *   QUERY 3 — Vendor 2 Short Trip Audit (PrefixFilter + column projection)
 *             Filter: PrefixFilter("2_") on row key
 *             Analytics: Count Vendor 2 trips, compute avg distance
 *
 *   QUERY 4 — Revenue Dashboard (PageFilter — Top 5 trips by scan order)
 *             Filter: PageFilter(5) — server-side pagination
 *             Analytics: Print first page of trip data for dashboard UI
 *
 * All filters are evaluated SERVER-SIDE on the HBase RegionServer.
 * Only matching rows are sent to client — drastically reduces network I/O.
 *
 * Run: java -jar nyc-taxi-hbase-analytics-1.0.jar
 */
public class ScanWithFilter {

    static final String TABLE_NAME = "nyc_taxi";
    static final byte[] CF_FIN     = Bytes.toBytes("financials");
    static final byte[] CF_TRIP    = Bytes.toBytes("trip_info");
    static final byte[] CF_TIME    = Bytes.toBytes("timing");
    static final byte[] CF_MET     = Bytes.toBytes("metrics");

    public static void main(String[] args) throws Exception {
        System.out.println(line('=', 70));
        System.out.println("   STUDENT 4: Multi-Filter Revenue & Trip Quality Dashboard");
        System.out.println("   Dataset : NYC TLC Yellow Taxi Trip Data (Jan 2026)");
        System.out.println("   All filters execute SERVER-SIDE on RegionServer.");
        System.out.println(line('=', 70));

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {

            // ================================================================
            // QUERY 1: Credit Card Revenue Report
            // Filter: SingleColumnValueFilter — trip_info:payment_type = "1"
            // Analytics: Count trips + sum total revenue from card payments
            // ================================================================
            System.out.println("\n" + line('=', 70));
            System.out.println("  QUERY 1: Credit Card Revenue Report");
            System.out.println("  Filter : SingleColumnValueFilter(payment_type = '1')");
            System.out.println("  Goal   : Total revenue earned from credit card payments");
            System.out.println(line('-', 70));

            Scan scan1 = new Scan();
            SingleColumnValueFilter cardFilter = new SingleColumnValueFilter(
                CF_TRIP, Bytes.toBytes("payment_type"),
                CompareOp.EQUAL, Bytes.toBytes("1")
            );
            cardFilter.setFilterIfMissing(true);
            scan1.setFilter(cardFilter);
            scan1.addColumn(CF_FIN,  Bytes.toBytes("fare_amount"));
            scan1.addColumn(CF_FIN,  Bytes.toBytes("tip_amount"));
            scan1.addColumn(CF_FIN,  Bytes.toBytes("total_amount"));
            scan1.addColumn(CF_TRIP, Bytes.toBytes("payment_type"));

            int    cardCount   = 0;
            double cardRevenue = 0.0;
            double cardTips    = 0.0;

            System.out.printf("  %-35s  %-8s  %-7s  %-8s%n", "Row Key", "Fare", "Tip", "Total");
            System.out.println("  " + line('-', 65));

            try (ResultScanner rs1 = table.getScanner(scan1)) {
                for (Result r : rs1) {
                    String rowKey = Bytes.toString(r.getRow());
                    String fare   = Bytes.toString(r.getValue(CF_FIN, Bytes.toBytes("fare_amount")));
                    String tip    = Bytes.toString(r.getValue(CF_FIN, Bytes.toBytes("tip_amount")));
                    String total  = Bytes.toString(r.getValue(CF_FIN, Bytes.toBytes("total_amount")));

                    double tipVal   = Double.parseDouble(tip   != null ? tip   : "0");
                    double totalVal = Double.parseDouble(total != null ? total : "0");

                    cardRevenue += totalVal;
                    cardTips    += tipVal;
                    cardCount++;

                    System.out.printf("  %-35s  $%-7s  $%-6s  $%-8s%n", rowKey, fare, tip, total);
                }
            }

            System.out.println("  " + line('-', 65));
            System.out.printf("  [RESULT] Card trips: %d | Total Revenue: $%.2f | Total Tips: $%.2f%n",
                cardCount, cardRevenue, cardTips);
            System.out.printf("  [INSIGHT] Avg tip per card trip: $%.2f%n",
                cardCount > 0 ? cardTips / cardCount : 0);

            // ================================================================
            // QUERY 2: Fast Trip Detector (avg_speed_mph > "12.0")
            // Filter: SingleColumnValueFilter — metrics:avg_speed_mph > "12"
            // Analytics: Count high-efficiency trips and their avg distance
            // NOTE: comparison is lexicographic on string values
            // ================================================================
            System.out.println("\n" + line('=', 70));
            System.out.println("  QUERY 2: Fast Trip Detector (avg_speed_mph > 12 mph)");
            System.out.println("  Filter : SingleColumnValueFilter(avg_speed_mph GREATER '12.0')");
            System.out.println("  Goal   : Find high-efficiency trips (clear road conditions)");
            System.out.println(line('-', 70));

            Scan scan2 = new Scan();
            SingleColumnValueFilter speedFilter = new SingleColumnValueFilter(
                CF_MET, Bytes.toBytes("avg_speed_mph"),
                CompareOp.GREATER, Bytes.toBytes("12.0")
            );
            speedFilter.setFilterIfMissing(true);
            scan2.setFilter(speedFilter);

            int    fastCount   = 0;
            double fastDistSum = 0.0;

            System.out.printf("  %-35s  %-10s  %-10s  %-8s%n", "Row Key", "Speed", "Distance", "Total");
            System.out.println("  " + line('-', 65));

            try (ResultScanner rs2 = table.getScanner(scan2)) {
                for (Result r : rs2) {
                    String rowKey = Bytes.toString(r.getRow());
                    String speed  = Bytes.toString(r.getValue(CF_MET,  Bytes.toBytes("avg_speed_mph")));
                    String dist   = Bytes.toString(r.getValue(CF_TRIP, Bytes.toBytes("trip_distance")));
                    String total  = Bytes.toString(r.getValue(CF_FIN,  Bytes.toBytes("total_amount")));

                    double distVal = Double.parseDouble(dist != null ? dist : "0");
                    fastDistSum += distVal;
                    fastCount++;

                    System.out.printf("  %-35s  %-10s  %-10s  $%-8s%n",
                        rowKey, speed + " mph", dist + " mi", total);
                }
            }

            System.out.println("  " + line('-', 65));
            System.out.printf("  [RESULT] Fast trips (>12 mph): %d | Avg distance: %.2f mi%n",
                fastCount, fastCount > 0 ? fastDistSum / fastCount : 0);

            // ================================================================
            // QUERY 3: Vendor 2 Short Trip Audit — PrefixFilter
            // Filter: PrefixFilter("2_") — scans only Vendor 2 row keys
            // Analytics: Count trips, compute avg trip distance for Vendor 2
            // ================================================================
            System.out.println("\n" + line('=', 70));
            System.out.println("  QUERY 3: Vendor 2 Trip Audit");
            System.out.println("  Filter : PrefixFilter('2_') on row key");
            System.out.println("  Goal   : Count Vendor 2 trips & compute average trip distance");
            System.out.println(line('-', 70));

            Scan scan3 = new Scan();
            scan3.setFilter(new PrefixFilter(Bytes.toBytes("2_")));
            scan3.addColumn(CF_TRIP, Bytes.toBytes("trip_distance"));
            scan3.addColumn(CF_FIN,  Bytes.toBytes("total_amount"));
            scan3.addColumn(CF_TIME, Bytes.toBytes("trip_duration_min"));

            int    v2Count   = 0;
            double v2DistSum = 0.0;
            double v2RevSum  = 0.0;

            System.out.printf("  %-35s  %-10s  %-10s  %-8s%n",
                "Row Key", "Distance", "Duration", "Total");
            System.out.println("  " + line('-', 65));

            try (ResultScanner rs3 = table.getScanner(scan3)) {
                for (Result r : rs3) {
                    String rowKey = Bytes.toString(r.getRow());
                    String dist   = Bytes.toString(r.getValue(CF_TRIP, Bytes.toBytes("trip_distance")));
                    String dur    = Bytes.toString(r.getValue(CF_TIME, Bytes.toBytes("trip_duration_min")));
                    String total  = Bytes.toString(r.getValue(CF_FIN,  Bytes.toBytes("total_amount")));

                    v2DistSum += Double.parseDouble(dist  != null ? dist  : "0");
                    v2RevSum  += Double.parseDouble(total != null ? total : "0");
                    v2Count++;

                    System.out.printf("  %-35s  %-10s  %-10s  $%-8s%n",
                        rowKey, dist + " mi", dur + " min", total);
                }
            }

            System.out.println("  " + line('-', 65));
            System.out.printf("  [RESULT] Vendor 2 trips: %d | Avg distance: %.2f mi | Total revenue: $%.2f%n",
                v2Count, v2Count > 0 ? v2DistSum / v2Count : 0, v2RevSum);

            // ================================================================
            // QUERY 4: Trip Revenue Dashboard — PageFilter (Top 5 trips)
            // Filter: PageFilter(5) — server-side pagination, returns first 5 rows
            // Analytics: Print a formatted dashboard view of the first 5 trips
            // ================================================================
            System.out.println("\n" + line('=', 70));
            System.out.println("  QUERY 4: Trip Revenue Dashboard (Page 1 — Top 5 Trips)");
            System.out.println("  Filter : PageFilter(5) — server-side pagination");
            System.out.println("  Goal   : Dashboard UI — first page of trip data");
            System.out.println(line('-', 70));

            Scan scan4 = new Scan();
            scan4.setFilter(new PageFilter(5));

            int    dashCount = 0;
            double dashTotal = 0.0;

            System.out.printf("  %-5s  %-35s  %-24s  %-8s  %-8s%n",
                "No.", "Row Key", "Pickup Time", "Distance", "Total");
            System.out.println("  " + line('-', 65));

            try (ResultScanner rs4 = table.getScanner(scan4)) {
                for (Result r : rs4) {
                    String rowKey = Bytes.toString(r.getRow());
                    String pickup = Bytes.toString(r.getValue(CF_TIME, Bytes.toBytes("pickup_datetime")));
                    String dist   = Bytes.toString(r.getValue(CF_TRIP, Bytes.toBytes("trip_distance")));
                    String total  = Bytes.toString(r.getValue(CF_FIN,  Bytes.toBytes("total_amount")));

                    dashTotal += Double.parseDouble(total != null ? total : "0");
                    dashCount++;

                    System.out.printf("  %-5d  %-35s  %-24s  %-8s  $%-8s%n",
                        dashCount, rowKey, pickup, dist + " mi", total);
                }
            }

            System.out.println("  " + line('-', 65));
            System.out.printf("  [RESULT] Page 1 rows: %d | Combined revenue: $%.2f%n",
                dashCount, dashTotal);
            System.out.println("  [NOTE] PageFilter evaluated on RegionServer — client only");
            System.out.println("         receives " + dashCount + " rows, never reads rest of table.");

            // ================================================================
            // DASHBOARD SUMMARY
            // ================================================================
            System.out.println("\n" + line('=', 70));
            System.out.println("                    ANALYTICS DASHBOARD SUMMARY");
            System.out.println(line('=', 70));
            System.out.printf("  Query 1 — Card Revenue Report  : %d trips, $%.2f revenue%n",
                cardCount, cardRevenue);
            System.out.printf("  Query 2 — Fast Trips (>12 mph) : %d trips, avg %.2f mi%n",
                fastCount, fastCount > 0 ? fastDistSum / fastCount : 0);
            System.out.printf("  Query 3 — Vendor 2 Audit       : %d trips, avg %.2f mi%n",
                v2Count, v2Count > 0 ? v2DistSum / v2Count : 0);
            System.out.printf("  Query 4 — Dashboard Page 1     : %d trips, $%.2f combined%n",
                dashCount, dashTotal);
            System.out.println(line('=', 70));
        }
    }

    // Declare fastDistSum at class level to share between lambda and summary
    // (Java workaround — use instance vars or atomic refs for real apps)
    static double fastDistSum = 0.0;

    static String line(char ch, int n) {
        char[] c = new char[n];
        java.util.Arrays.fill(c, ch);
        return new String(c);
    }
}
