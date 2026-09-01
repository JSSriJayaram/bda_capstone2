import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * TaxiMapper
 *
 * Parses each line of the Yellow Taxi cleaned CSV and emits:
 *   Key:   PULocationID  (Text)
 *   Value: "fare_amount,total_amount,trip_distance,trip_duration_min,avg_speed_mph,tip_amount"  (Text)
 *
 * CSV column indices (0-indexed, 31 total columns):
 *   0  VendorID
 *   1  tpep_pickup_datetime
 *   2  tpep_dropoff_datetime
 *   3  passenger_count
 *   4  trip_distance
 *   5  RatecodeID
 *   6  store_and_fwd_flag
 *   7  PULocationID          <-- KEY
 *   8  DOLocationID
 *   9  payment_type
 *   10 fare_amount
 *   11 extra
 *   12 mta_tax
 *   13 tip_amount
 *   14 tolls_amount
 *   15 improvement_surcharge
 *   16 total_amount
 *   17 congestion_surcharge
 *   18 Airport_fee
 *   19 cbd_congestion_fee
 *   20 trip_duration_min
 *   21 pickup_date
 *   22 pickup_hour
 *   23 pickup_day
 *   24 pickup_month
 *   25 pickup_year
 *   26 pickup_dayofweek
 *   27 pickup_day_name
 *   28 is_weekend
 *   29 avg_speed_mph
 *   30 fare_per_mile
 */
public class TaxiMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int IDX_PULOCATION  = 7;
    private static final int IDX_FARE        = 10;
    private static final int IDX_TOTAL       = 16;
    private static final int IDX_DISTANCE    = 4;
    private static final int IDX_DURATION    = 20;
    private static final int IDX_SPEED       = 29;
    private static final int IDX_TIP         = 13;
    private static final int EXPECTED_COLS   = 31;

    private final Text outKey   = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // Split by comma — dataset has no quoted commas (verified from inspection)
        String[] fields = line.split(",", -1);

        // Skip header row
        if (fields[0].trim().equals("VendorID")) return;

        // Skip malformed rows with wrong column count
        if (fields.length < EXPECTED_COLS) return;

        try {
            String puLocation = fields[IDX_PULOCATION].trim();
            if (puLocation.isEmpty()) return;

            double fare     = parseDouble(fields[IDX_FARE]);
            double total    = parseDouble(fields[IDX_TOTAL]);
            double distance = parseDouble(fields[IDX_DISTANCE]);
            double duration = parseDouble(fields[IDX_DURATION]);
            double speed    = parseDouble(fields[IDX_SPEED]);
            double tip      = parseDouble(fields[IDX_TIP]);

            outKey.set(puLocation);
            // Comma-separated metrics: fare,total,distance,duration,speed,tip
            outValue.set(fare + "," + total + "," + distance + "," + duration + "," + speed + "," + tip);
            context.write(outKey, outValue);

        } catch (Exception e) {
            // Skip any malformed record silently
            context.getCounter("TaxiMapper", "SkippedRecords").increment(1);
        }
    }

    /**
     * Safely parse a double from a CSV field.
     * Returns 0.0 for empty, null, NaN, or unparseable values.
     */
    private double parseDouble(String s) {
        if (s == null) return 0.0;
        s = s.trim();
        if (s.isEmpty() || s.equalsIgnoreCase("null")
                || s.equalsIgnoreCase("na") || s.equalsIgnoreCase("nan")) {
            return 0.0;
        }
        double d = Double.parseDouble(s);
        return (Double.isNaN(d) || Double.isInfinite(d)) ? 0.0 : d;
    }
}
