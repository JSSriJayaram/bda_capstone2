package com.nyctaxi.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.HashMap;
import java.util.Map;

/**
 * STUDENT 1 — TripVolumeReport.java (CreateTable.java)
 * =====================================================================
 * Project      : NYC Yellow Taxi HBase Analytics (BDA Review 2)
 * Dataset      : NYC TLC Yellow Taxi Trip Data, Jan 2026 (7.6M rows)
 * =====================================================================
 *
 * ANALYTICS QUERY: Trip Volume & Revenue Report by Vendor
 * --------------------------------------------------------
 * 1. Creates the nyc_taxi table with 4 column families (Admin API)
 * 2. Scans the table and groups rows by VendorID (extracted from row key)
 * 3. For each vendor: counts trips, sums total revenue, sums distances
 * 4. Prints a formatted analytics summary table
 *
 * Business Question: "Which vendor processes more trips and earns more revenue?"
 *
 * Row Key Design: {VendorID}_{pickup_datetime}_{PULocationID}
 *   - VendorID prefix enables O(1) vendor grouping during scan (no extra index)
 *   - Row key prefix "1_" = Vendor 1 (VeriFone), "2_" = Vendor 2 (Creative Mobile)
 *
 * Run: java -cp nyc-taxi-hbase-analytics-1.0.jar com.nyctaxi.hbase.CreateTable
 */
public class CreateTable {

    static final String TABLE_NAME  = "nyc_taxi";
    static final byte[] CF_TRIP     = Bytes.toBytes("trip_info");
    static final byte[] CF_FIN      = Bytes.toBytes("financials");
    static final byte[] CF_TIMING   = Bytes.toBytes("timing");
    static final byte[] CF_METRICS  = Bytes.toBytes("metrics");

    public static void main(String[] args) throws Exception {
        System.out.println(line('=', 65));
        System.out.println("   STUDENT 1: Trip Volume & Revenue Report by Vendor");
        System.out.println("   Dataset : NYC TLC Yellow Taxi Trip Data (Jan 2026)");
        System.out.println(line('=', 65));

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection conn = ConnectionFactory.createConnection(conf);
             Admin admin = conn.getAdmin();
             Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {

            // ============================================================
            // STEP 1: Create table if it doesn't already exist
            // ============================================================
            TableName tableName = TableName.valueOf(TABLE_NAME);

            if (!admin.tableExists(tableName)) {
                System.out.println("[INFO] Creating table '" + TABLE_NAME + "' with 4 column families...");
                TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
                tdb.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF_TRIP).setMaxVersions(1).build());
                tdb.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF_FIN).setMaxVersions(1).build());
                tdb.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF_TIMING).setMaxVersions(1).build());
                tdb.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF_METRICS).setMaxVersions(1).build());
                admin.createTable(tdb.build());
                System.out.println("[SUCCESS] Table created.\n");
            } else {
                System.out.println("[INFO] Table already exists. Skipping creation.\n");
            }

            // ============================================================
            // STEP 2: ANALYTICS QUERY — Scan & compute per-vendor stats
            // ============================================================
            System.out.println(line('-', 65));
            System.out.println("  ANALYTICS: Trip Volume & Revenue Report by VendorID");
            System.out.println(line('-', 65));

            // Counters per vendor
            Map<String, Integer> tripCount   = new HashMap<>();
            Map<String, Double>  totalRevenue = new HashMap<>();
            Map<String, Double>  totalDist    = new HashMap<>();
            tripCount.put("1", 0);   tripCount.put("2", 0);
            totalRevenue.put("1", 0.0); totalRevenue.put("2", 0.0);
            totalDist.put("1", 0.0);    totalDist.put("2", 0.0);

            Scan scan = new Scan();
            scan.addColumn(CF_FIN,  Bytes.toBytes("total_amount"));
            scan.addColumn(CF_TRIP, Bytes.toBytes("trip_distance"));

            try (ResultScanner rs = table.getScanner(scan)) {
                for (Result r : rs) {
                    String rowKey  = Bytes.toString(r.getRow());
                    String vendor  = rowKey.substring(0, 1);   // "1" or "2"

                    String totalStr = Bytes.toString(r.getValue(CF_FIN, Bytes.toBytes("total_amount")));
                    String distStr  = Bytes.toString(r.getValue(CF_TRIP, Bytes.toBytes("trip_distance")));

                    double total = (totalStr != null) ? Double.parseDouble(totalStr) : 0.0;
                    double dist  = (distStr  != null) ? Double.parseDouble(distStr)  : 0.0;

                    if (tripCount.containsKey(vendor)) {
                        tripCount.put(vendor, tripCount.get(vendor) + 1);
                        totalRevenue.put(vendor, totalRevenue.get(vendor) + total);
                        totalDist.put(vendor, totalDist.get(vendor) + dist);
                    }
                }
            }

            // ============================================================
            // STEP 3: Print formatted analytics report
            // ============================================================
            System.out.println();
            System.out.printf("  %-12s  %-10s  %-14s  %-12s  %-13s%n",
                "VendorID", "Trips", "Total Revenue", "Total Dist", "Avg Fare/Trip");
            System.out.println("  " + line('-', 63));

            for (String v : new String[]{"1", "2"}) {
                int    trips   = tripCount.get(v);
                double rev     = totalRevenue.get(v);
                double dist    = totalDist.get(v);
                double avgFare = (trips > 0) ? rev / trips : 0.0;
                String vName   = v.equals("1") ? "1 (VeriFone)" : "2 (Creative)";
                System.out.printf("  %-12s  %-10d  $%-13.2f  %-12.2f  $%-13.2f%n",
                    vName, trips, rev, dist, avgFare);
            }

            System.out.println("  " + line('-', 63));
            System.out.println();

            int    totalTrips = tripCount.get("1") + tripCount.get("2");
            double grandRev   = totalRevenue.get("1") + totalRevenue.get("2");
            System.out.printf("  %-12s  %-10d  $%-13.2f%n", "TOTAL", totalTrips, grandRev);
            System.out.println();

            // Analytics insight
            String topVendor = totalRevenue.get("1") > totalRevenue.get("2") ? "Vendor 1 (VeriFone)" : "Vendor 2 (Creative Mobile)";
            System.out.println("  [INSIGHT] Highest revenue vendor: " + topVendor);
            System.out.println("  [INSIGHT] Column families read  : trip_info + financials only");
            System.out.println("  [INSIGHT] Other 2 CFs (timing, metrics) skipped — zero disk I/O");
            System.out.println(line('=', 65));
        }
    }

    static String line(char ch, int n) {
        char[] c = new char[n];
        java.util.Arrays.fill(c, ch);
        return new String(c);
    }
}
