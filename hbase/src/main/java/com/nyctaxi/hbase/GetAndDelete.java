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
 * Domain       : Transportation Analytics
 * =====================================================================
 *
 * This program demonstrates the complete Record Lifecycle using HBase Java API:
 *
 *   [GET 1]    Point lookup — Fetch a long-distance trip (10.8 miles, $66.95)
 *              Scenario: Dispatcher reviewing a flagged high-fare trip
 *
 *   [GET 2]    Point lookup — Fetch a group ride (3 passengers, $54.11)
 *              Scenario: Analytics team reviewing multi-passenger trips
 *
 *   [DELETE]   Row deletion — Remove a zero-fare erroneous record ($0.0 total)
 *              Scenario: Data quality team performing cleanup
 *
 *   [VERIFY]   Confirm deletion — Re-run Get to prove row is gone
 *              Demonstrates HBase tombstone mechanism (logical delete)
 *
 * HBase Delete Internals:
 *   - HBase does NOT immediately remove data from disk (append-only HDFS)
 *   - A "Tombstone Marker" is written to the WAL and MemStore
 *   - The row is immediately hidden from Get/Scan after deletion
 *   - Physical data is permanently purged during the next Major Compaction
 *   - delete() removes a specific column qualifier
 *   - deleteall() removes all columns for an entire row key
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
        System.out.println("   STUDENT 3: Get & Delete — Record Lifecycle Demo");
        System.out.println("   Dataset : NYC TLC Yellow Taxi Trip Data (Jan 2026)");
        System.out.println(line('=', 65));

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {

            // ============================================================
            // GET 1: Point lookup — Long-distance trip (10.8 miles, $66.95)
            // ============================================================
            String rowKey1 = "1_20260101004153_142";
            System.out.println("\n[GET 1] Point lookup: " + rowKey1);
            System.out.println("[Scenario] Dispatcher reviewing a flagged long-distance trip");
            System.out.println(line('-', 65));

            Get get1 = new Get(Bytes.toBytes(rowKey1));
            Result r1 = table.get(get1);

            if (!r1.isEmpty()) {
                System.out.println("  Pickup    : " + Bytes.toString(r1.getValue(CF_TIME, Bytes.toBytes("pickup_datetime"))));
                System.out.println("  Dropoff   : " + Bytes.toString(r1.getValue(CF_TIME, Bytes.toBytes("dropoff_datetime"))));
                System.out.println("  Duration  : " + Bytes.toString(r1.getValue(CF_TIME, Bytes.toBytes("trip_duration_min"))) + " mins");
                System.out.println("  Passengers: " + Bytes.toString(r1.getValue(CF_TRIP, Bytes.toBytes("passenger_count"))));
                System.out.println("  Distance  : " + Bytes.toString(r1.getValue(CF_TRIP, Bytes.toBytes("trip_distance"))) + " miles");
                System.out.println("  Fare      : $" + Bytes.toString(r1.getValue(CF_FIN, Bytes.toBytes("fare_amount"))));
                System.out.println("  Tip       : $" + Bytes.toString(r1.getValue(CF_FIN, Bytes.toBytes("tip_amount"))));
                System.out.println("  Total     : $" + Bytes.toString(r1.getValue(CF_FIN, Bytes.toBytes("total_amount"))));
                System.out.println("  Speed     : " + Bytes.toString(r1.getValue(CF_MET, Bytes.toBytes("avg_speed_mph"))) + " mph");
                System.out.println("  Fare/Mile : $" + Bytes.toString(r1.getValue(CF_MET, Bytes.toBytes("fare_per_mile"))));
                System.out.println("[SUCCESS] Record retrieved successfully!");
            } else {
                System.out.println("[NOT FOUND] Row does not exist: " + rowKey1);
            }

            // ============================================================
            // GET 2: Point lookup — Group ride (3 passengers, $54.11)
            // ============================================================
            String rowKey2 = "2_20260101001931_137";
            System.out.println("\n[GET 2] Point lookup: " + rowKey2);
            System.out.println("[Scenario] Analytics team reviewing a group ride (3 passengers)");
            System.out.println(line('-', 65));

            Get get2 = new Get(Bytes.toBytes(rowKey2));
            Result r2 = table.get(get2);

            if (!r2.isEmpty()) {
                System.out.println("  Pickup    : " + Bytes.toString(r2.getValue(CF_TIME, Bytes.toBytes("pickup_datetime"))));
                System.out.println("  Passengers: " + Bytes.toString(r2.getValue(CF_TRIP, Bytes.toBytes("passenger_count"))));
                System.out.println("  Distance  : " + Bytes.toString(r2.getValue(CF_TRIP, Bytes.toBytes("trip_distance"))) + " miles");
                System.out.println("  Fare      : $" + Bytes.toString(r2.getValue(CF_FIN, Bytes.toBytes("fare_amount"))));
                System.out.println("  Tip       : $" + Bytes.toString(r2.getValue(CF_FIN, Bytes.toBytes("tip_amount"))));
                System.out.println("  Total     : $" + Bytes.toString(r2.getValue(CF_FIN, Bytes.toBytes("total_amount"))));
                System.out.println("  Speed     : " + Bytes.toString(r2.getValue(CF_MET, Bytes.toBytes("avg_speed_mph"))) + " mph");
                System.out.println("[SUCCESS] Record retrieved successfully!");
            } else {
                System.out.println("[NOT FOUND] Row does not exist: " + rowKey2);
            }

            // ============================================================
            // DELETE: Remove zero-fare erroneous record ($0.0 total)
            // Scenario: Data quality cleanup of a corrupt/test record
            // ============================================================
            String deleteKey = "1_20260101004035_161";
            System.out.println("\n[DELETE] Removing erroneous zero-fare record: " + deleteKey);
            System.out.println("[Scenario] Data quality team removing a $0.0 total trip (data error)");
            System.out.println(line('-', 65));

            // Pre-check: confirm record exists before attempting delete
            Get preCheck = new Get(Bytes.toBytes(deleteKey));
            Result preResult = table.get(preCheck);

            if (!preResult.isEmpty()) {
                String total = Bytes.toString(preResult.getValue(CF_FIN, Bytes.toBytes("total_amount")));
                String dist  = Bytes.toString(preResult.getValue(CF_TRIP, Bytes.toBytes("trip_distance")));
                System.out.println("  [BEFORE] Record exists -> Distance: " + dist + " mi, Total: $" + total);

                // Execute full row delete (tombstone written to WAL + MemStore)
                Delete del = new Delete(Bytes.toBytes(deleteKey));
                table.delete(del);
                System.out.println("  [EXECUTED] deleteall('" + deleteKey + "')");
                System.out.println("  [NOTE] Tombstone marker written. Physical data removed at next Major Compaction.");
            } else {
                System.out.println("  [SKIP] Record not found (may have been deleted already).");
            }

            // ============================================================
            // VERIFY: Confirm deletion — Get should now return empty result
            // ============================================================
            System.out.println("\n[VERIFY] Confirming deletion of: " + deleteKey);
            System.out.println(line('-', 65));
            Result verifyResult = table.get(new Get(Bytes.toBytes(deleteKey)));

            if (verifyResult.isEmpty()) {
                System.out.println("  [CONFIRMED] Row '" + deleteKey + "' no longer exists in HBase.");
                System.out.println("  [SUCCESS] Delete operation verified — row hidden by tombstone.");
            } else {
                System.out.println("  [WARN] Row still exists — delete may not have completed.");
            }

            System.out.println(line('=', 65));
            System.out.println("   Get & Delete lifecycle demo complete.");
            System.out.println(line('=', 65));
        }
    }

    static String line(char ch, int n) {
        char[] c = new char[n];
        java.util.Arrays.fill(c, ch);
        return new String(c);
    }
}
