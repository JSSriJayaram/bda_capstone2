package com.nyctaxi.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.ArrayList;
import java.util.List;

/**
 * STUDENT 2 — InsertData.java
 * =====================================================================
 * Project      : NYC Yellow Taxi HBase Analytics (BDA Review 2)
 * Dataset      : NYC TLC Yellow Taxi Trip Data, Jan 2026 (7.6M rows)
 * =====================================================================
 *
 * ANALYTICS QUERY: High Revenue Trip Finder
 * ------------------------------------------
 * 1. Batch-inserts 12 real NYC taxi records using table.put(List)
 * 2. Immediately queries: Find trips where total_amount > 40.0
 *    using SingleColumnValueFilter on financials:total_amount
 * 3. For matching trips: prints trip distance, fare, tip, speed
 * 4. Prints summary: count of high-value trips, combined revenue
 *
 * Business Question: "Which trips earned more than $40 — identify the
 * high-revenue trips for driver reward programs."
 *
 * HBase Write Path: Put → WAL (Write-Ahead Log) → MemStore → HFile
 *
 * Run: java -cp nyc-taxi-hbase-analytics-1.0.jar com.nyctaxi.hbase.InsertData
 */
public class InsertData {

    static final String TABLE_NAME = "nyc_taxi";
    static final byte[] CF_TRIP    = Bytes.toBytes("trip_info");
    static final byte[] CF_FIN     = Bytes.toBytes("financials");
    static final byte[] CF_TIME    = Bytes.toBytes("timing");
    static final byte[] CF_METRICS = Bytes.toBytes("metrics");

    static String compactDT(String dt) {
        return dt.replace("-", "").replace(" ", "").replace(":", "");
    }

    static Put buildPut(String vendorID, String pickupDT, String dropoffDT,
                        String passengers, String distance, String puLoc, String doLoc,
                        String payType, String fare, String tip, String total,
                        String durationMin, String pickupHour, String dayName,
                        String speed, String farePerMile, String isWeekend, String congestion) {

        String rowKey = vendorID + "_" + compactDT(pickupDT) + "_" + puLoc;
        Put p = new Put(Bytes.toBytes(rowKey));
        p.addColumn(CF_TRIP,    Bytes.toBytes("passenger_count"),     Bytes.toBytes(passengers));
        p.addColumn(CF_TRIP,    Bytes.toBytes("trip_distance"),       Bytes.toBytes(distance));
        p.addColumn(CF_TRIP,    Bytes.toBytes("PULocationID"),        Bytes.toBytes(puLoc));
        p.addColumn(CF_TRIP,    Bytes.toBytes("DOLocationID"),        Bytes.toBytes(doLoc));
        p.addColumn(CF_TRIP,    Bytes.toBytes("payment_type"),        Bytes.toBytes(payType));
        p.addColumn(CF_FIN,     Bytes.toBytes("fare_amount"),         Bytes.toBytes(fare));
        p.addColumn(CF_FIN,     Bytes.toBytes("tip_amount"),          Bytes.toBytes(tip));
        p.addColumn(CF_FIN,     Bytes.toBytes("total_amount"),        Bytes.toBytes(total));
        p.addColumn(CF_FIN,     Bytes.toBytes("congestion_surcharge"),Bytes.toBytes(congestion));
        p.addColumn(CF_TIME,    Bytes.toBytes("pickup_datetime"),     Bytes.toBytes(pickupDT));
        p.addColumn(CF_TIME,    Bytes.toBytes("dropoff_datetime"),    Bytes.toBytes(dropoffDT));
        p.addColumn(CF_TIME,    Bytes.toBytes("trip_duration_min"),   Bytes.toBytes(durationMin));
        p.addColumn(CF_TIME,    Bytes.toBytes("pickup_hour"),         Bytes.toBytes(pickupHour));
        p.addColumn(CF_TIME,    Bytes.toBytes("pickup_day_name"),     Bytes.toBytes(dayName));
        p.addColumn(CF_METRICS, Bytes.toBytes("avg_speed_mph"),       Bytes.toBytes(speed));
        p.addColumn(CF_METRICS, Bytes.toBytes("fare_per_mile"),       Bytes.toBytes(farePerMile));
        p.addColumn(CF_METRICS, Bytes.toBytes("is_weekend"),          Bytes.toBytes(isWeekend));
        return p;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(line('=', 65));
        System.out.println("   STUDENT 2: High Revenue Trip Finder (total > $40)");
        System.out.println("   Source  : NYC TLC Yellow Taxi Trip Data (Jan 2026)");
        System.out.println(line('=', 65));

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {

            // ============================================================
            // STEP 1: Batch Insert 12 Real NYC Taxi Records
            // ============================================================
            List<Put> puts = new ArrayList<>();
            puts.add(buildPut("1","2026-01-01 00:40:35","2026-01-01 00:45:12","1","0.5","161","229","4","0.0","0.0","0.0","4.62","0","Thursday","6.49","0.0","0","0.0"));
            puts.add(buildPut("1","2026-01-01 00:23:16","2026-01-01 00:37:18","2","2.6","48","151","1","14.9","0.0","20.65","14.03","0","Thursday","11.12","7.94","0","2.5"));
            puts.add(buildPut("1","2026-01-01 00:41:53","2026-01-01 01:23:56","1","10.8","142","189","1","47.8","13.4","66.95","42.05","0","Thursday","15.41","6.2","0","2.5"));
            puts.add(buildPut("1","2026-01-01 00:38:54","2026-01-01 01:28:40","1","9.4","170","64","1","43.5","0.0","44.0","49.77","0","Thursday","11.33","4.68","0","0.0"));
            puts.add(buildPut("2","2026-01-01 00:23:38","2026-01-01 00:29:35","1","1.56","239","151","1","9.3","4.29","18.59","5.95","0","Thursday","15.73","11.92","0","2.5"));
            puts.add(buildPut("2","2026-01-01 00:07:08","2026-01-01 00:18:18","3","1.73","239","236","1","12.1","1.71","18.81","11.17","0","Thursday","9.29","10.87","0","2.5"));
            puts.add(buildPut("2","2026-01-01 00:10:16","2026-01-01 00:14:26","1","0.84","249","68","2","6.5","0.0","12.25","4.17","0","Thursday","12.09","14.58","0","2.5"));
            puts.add(buildPut("2","2026-01-01 00:10:55","2026-01-01 00:19:41","1","1.94","239","236","1","12.1","3.42","20.52","8.77","0","Thursday","13.27","10.58","0","2.5"));
            puts.add(buildPut("2","2026-01-01 00:58:17","2026-01-01 01:09:57","2","1.94","79","233","1","12.8","3.71","22.26","11.67","0","Thursday","9.97","11.47","0","2.5"));
            puts.add(buildPut("2","2026-01-01 00:19:31","2026-01-01 00:46:09","3","7.01","137","225","1","32.4","9.02","54.11","26.63","0","Thursday","15.79","7.72","0","2.5"));
            puts.add(buildPut("2","2026-01-01 00:36:29","2026-01-01 00:41:04","1","0.6","143","239","1","6.5","2.0","13.5","4.58","0","Thursday","7.86","22.5","0","2.5"));
            puts.add(buildPut("2","2026-01-01 00:30:46","2026-01-01 00:36:58","1","0.64","161","237","1","7.9","2.73","16.38","6.2","0","Thursday","6.19","25.59","0","2.5"));

            table.put(puts);
            System.out.println("[SUCCESS] 12 NYC Taxi records inserted.\n");

            // ============================================================
            // STEP 2: ANALYTICS QUERY — Find High Revenue Trips (> $40)
            // Filter: SingleColumnValueFilter on financials:total_amount
            // Comparison: GREATER (lexicographic on string "40.0")
            // ============================================================
            System.out.println(line('-', 65));
            System.out.println("  ANALYTICS: High Revenue Trips — total_amount > $40");
            System.out.println("  Filter   : SingleColumnValueFilter(financials:total_amount, GREATER, '40.0')");
            System.out.println("  Business : Identify premium trips for driver reward program");
            System.out.println(line('-', 65));

            Scan scan = new Scan();
            SingleColumnValueFilter highFareFilter = new SingleColumnValueFilter(
                CF_FIN,
                Bytes.toBytes("total_amount"),
                CompareOp.GREATER,
                Bytes.toBytes("40.0")
            );
            highFareFilter.setFilterIfMissing(true);
            scan.setFilter(highFareFilter);

            int highRevCount = 0;
            double combinedRevenue = 0.0;

            System.out.printf("%n  %-35s  %-8s  %-6s  %-6s  %-8s  %-8s%n",
                "Row Key", "Distance", "Fare", "Tip", "Total", "Speed");
            System.out.println("  " + line('-', 63));

            try (ResultScanner rs = table.getScanner(scan)) {
                for (Result r : rs) {
                    String rowKey  = Bytes.toString(r.getRow());
                    String dist    = Bytes.toString(r.getValue(CF_TRIP,    Bytes.toBytes("trip_distance")));
                    String fare    = Bytes.toString(r.getValue(CF_FIN,     Bytes.toBytes("fare_amount")));
                    String tip     = Bytes.toString(r.getValue(CF_FIN,     Bytes.toBytes("tip_amount")));
                    String total   = Bytes.toString(r.getValue(CF_FIN,     Bytes.toBytes("total_amount")));
                    String speed   = Bytes.toString(r.getValue(CF_METRICS, Bytes.toBytes("avg_speed_mph")));
                    String pickup  = Bytes.toString(r.getValue(CF_TIME,    Bytes.toBytes("pickup_datetime")));

                    System.out.printf("  %-35s  %-8s  %-6s  %-6s  $%-7s  %-8s mph%n",
                        rowKey, dist + " mi", "$" + fare, "$" + tip, total, speed);
                    System.out.printf("     Pickup: %s%n", pickup);

                    combinedRevenue += Double.parseDouble(total != null ? total : "0");
                    highRevCount++;
                }
            }

            System.out.println("  " + line('-', 63));
            System.out.printf("%n  [RESULT] High-revenue trips (total > $40) : %d / 12%n", highRevCount);
            System.out.printf("  [RESULT] Combined revenue from these trips : $%.2f%n", combinedRevenue);
            System.out.printf("  [INSIGHT] %.1f%% of trips earned over $40%n",
                (highRevCount * 100.0 / 12));
            System.out.println("  [NOTE] Filter ran SERVER-SIDE on RegionServer — efficient query");
            System.out.println(line('=', 65));
        }
    }

    static String line(char ch, int n) {
        char[] c = new char[n];
        java.util.Arrays.fill(c, ch);
        return new String(c);
    }
}
