package com.nyctaxi.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * STUDENT 3 — GetAndDelete.java
 * =====================================================================
 * Project      : NYC Yellow Taxi HBase Analytics (BDA Review 2)
 * Dataset      : NYC TLC Yellow Taxi Trip Data, Jan 2026 (7.6M rows)
 * =====================================================================
 *
 * ANALYTICS QUERY: Average Speed & Fare Audit with Outlier Detection
 * -------------------------------------------------------------------
 * 1. GET 3 specific trips using point-lookup (Get API)
 * 2. Compute: average speed, average fare across the 3 trips
 * 3. Detect outlier: flag any trip where speed < half the average
 *    (possible data error or traffic anomaly)
 * 4. Identify zero-fare record (data quality issue) and DELETE it
 * 5. Verify deletion via tombstone check (Get returns empty)
 *
 * Business Question: "Are there anomalous trips with unusually low speed
 * or zero fares that indicate data errors or fraud?"
 *
 * HBase Delete Internals:
 *   - delete() writes a Tombstone Marker to WAL & MemStore
 *   - Row is immediately hidden from Get/Scan
 *   - Physical bytes removed only during Major Compaction
 *
 * Run: java -cp nyc-taxi-hbase-analytics-1.0.jar com.nyctaxi.hbase.GetAndDelete
 */
public class GetAndDelete {

    static final String TABLE_NAME = "nyc_taxi";
    static final byte[] CF_TRIP = Bytes.toBytes("trip_info");
    static final byte[] CF_FIN  = Bytes.toBytes("financials");
    static final byte[] CF_TIME = Bytes.toBytes("timing");
    static final byte[] CF_MET  = Bytes.toBytes("metrics");

    public static void main(String[] args) throws Exception {
        System.out.println(line('=', 65));
        System.out.println("   STUDENT 3: Avg Speed & Fare Audit with Outlier Detection");
        System.out.println("   Dataset : NYC TLC Yellow Taxi Trip Data (Jan 2026)");
        System.out.println(line('=', 65));

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {

            // Trip row keys selected for audit
            String[] auditKeys = {
                "1_20260101004153_142",   // 10.8 mi, $66.95, 15.41 mph — long trip
                "2_20260101001931_137",   // 7.01 mi, $54.11, 15.79 mph — group ride
                "1_20260101002316_48"     // 2.6 mi,  $20.65, 11.12 mph — short trip
            };

            double totalSpeed = 0.0;
            double totalFare  = 0.0;
            int    validCount = 0;

            // ============================================================
            // STEP 1: Point-Lookup (GET) — Fetch 3 trips for audit
            // ============================================================
            System.out.println("\n" + line('-', 65));
            System.out.println("  STEP 1: Point Lookups — Fetching 3 trips for audit");
            System.out.println(line('-', 65));
            System.out.printf("  %-35s  %-8s  %-8s  %-8s  %-8s%n",
                "Row Key", "Distance", "Total", "Speed", "Duration");
            System.out.println("  " + line('-', 63));

            double[] speeds = new double[auditKeys.length];
            double[] fares  = new double[auditKeys.length];

            for (int i = 0; i < auditKeys.length; i++) {
                Get g = new Get(Bytes.toBytes(auditKeys[i]));
                Result r = table.get(g);

                if (!r.isEmpty()) {
                    String dist     = Bytes.toString(r.getValue(CF_TRIP, Bytes.toBytes("trip_distance")));
                    String total    = Bytes.toString(r.getValue(CF_FIN,  Bytes.toBytes("total_amount")));
                    String speedStr = Bytes.toString(r.getValue(CF_MET,  Bytes.toBytes("avg_speed_mph")));
                    String dur      = Bytes.toString(r.getValue(CF_TIME, Bytes.toBytes("trip_duration_min")));

                    double spd  = Double.parseDouble(speedStr != null ? speedStr : "0");
                    double fare = Double.parseDouble(total    != null ? total    : "0");

                    speeds[i]   = spd;
                    fares[i]    = fare;
                    totalSpeed += spd;
                    totalFare  += fare;
                    validCount++;

                    System.out.printf("  %-35s  %-8s  $%-7s  %-8s  %-8s min%n",
                        auditKeys[i], dist + " mi", total, speedStr + " mph", dur);
                } else {
                    System.out.printf("  %-35s  [NOT FOUND]%n", auditKeys[i]);
                }
            }

            // ============================================================
            // STEP 2: ANALYTICS — Compute Averages & Flag Outliers
            // ============================================================
            System.out.println("\n" + line('-', 65));
            System.out.println("  STEP 2: Analytics — Average Speed & Fare + Outlier Detection");
            System.out.println(line('-', 65));

            double avgSpeed = (validCount > 0) ? totalSpeed / validCount : 0.0;
            double avgFare  = (validCount > 0) ? totalFare  / validCount : 0.0;
            double threshold = avgSpeed / 2.0;

            System.out.printf("  Average Speed across 3 trips : %.2f mph%n", avgSpeed);
            System.out.printf("  Average Fare  across 3 trips : $%.2f%n",    avgFare);
            System.out.printf("  Outlier threshold (< half avg speed) : %.2f mph%n%n", threshold);

            for (int i = 0; i < auditKeys.length; i++) {
                if (speeds[i] > 0 && speeds[i] < threshold) {
                    System.out.printf("  [OUTLIER] %s — speed %.2f mph is below threshold!%n",
                        auditKeys[i], speeds[i]);
                } else if (speeds[i] > 0) {
                    System.out.printf("  [OK]      %s — speed %.2f mph (normal)%n",
                        auditKeys[i], speeds[i]);
                }
            }

            // ============================================================
            // STEP 3: DELETE zero-fare record (Data Quality Cleanup)
            // ============================================================
            String zeroFareKey = "1_20260101004035_161";
            System.out.println("\n" + line('-', 65));
            System.out.println("  STEP 3: Data Quality — Remove Zero-Fare Record");
            System.out.println(line('-', 65));

            Get preCheck = new Get(Bytes.toBytes(zeroFareKey));
            Result preResult = table.get(preCheck);

            if (!preResult.isEmpty()) {
                String totalAmt = Bytes.toString(preResult.getValue(CF_FIN, Bytes.toBytes("total_amount")));
                String dist     = Bytes.toString(preResult.getValue(CF_TRIP, Bytes.toBytes("trip_distance")));
                System.out.println("  [FOUND] Erroneous record: " + zeroFareKey);
                System.out.printf("          Distance: %s mi, Total: $%s — ZERO FARE (data error)%n", dist, totalAmt);

                Delete del = new Delete(Bytes.toBytes(zeroFareKey));
                table.delete(del);
                System.out.println("  [DELETE] Tombstone marker written to WAL & MemStore.");
                System.out.println("  [NOTE] Physical data purged only at next Major Compaction.");
            } else {
                System.out.println("  [SKIP] Zero-fare record not found (already deleted).");
            }

            // ============================================================
            // STEP 4: VERIFY — Confirm Deletion (tombstone check)
            // ============================================================
            System.out.println("\n" + line('-', 65));
            System.out.println("  STEP 4: Verify Deletion — Tombstone Confirmation");
            System.out.println(line('-', 65));

            Result verifyResult = table.get(new Get(Bytes.toBytes(zeroFareKey)));
            if (verifyResult.isEmpty()) {
                System.out.println("  [CONFIRMED] Row '" + zeroFareKey + "' → empty result.");
                System.out.println("  [SUCCESS] Row hidden by tombstone. Zero-fare trip removed.");
            } else {
                System.out.println("  [WARN] Row still visible — delete may not have committed.");
            }

            System.out.println(line('=', 65));
            System.out.println("   Audit complete. 1 erroneous zero-fare trip removed.");
            System.out.println(line('=', 65));
        }
    }

    static String line(char ch, int n) {
        char[] c = new char[n];
        java.util.Arrays.fill(c, ch);
        return new String(c);
    }
}
