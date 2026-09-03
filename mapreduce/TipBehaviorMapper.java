import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * TipBehaviorMapper
 *
 * MapReduce Job 3: Payment Type & Tipping Behavior Binning
 *
 * Reads each CSV line, bins fare_amount into a discrete bucket, and emits a
 * composite key combining payment_type and the fare bucket. This enables the
 * reducer to analyze tipping behavior across different price brackets and
 * payment methods simultaneously.
 *
 * CSV column indices (0-indexed, 31 total columns):
 *   3  passenger_count
 *   9  payment_type        <-- part of KEY
 *   10 fare_amount         <-- binned into KEY
 *   13 tip_amount          <-- part of VALUE
 *
 * Fare Buckets:
 *   0_5        : fare >=  0.0 and <  5.0
 *   5_10       : fare >=  5.0 and < 10.0
 *   10_25      : fare >= 10.0 and < 25.0
 *   25_50      : fare >= 25.0 and < 50.0
 *   50_100     : fare >= 50.0 and < 100.0
 *   100_PLUS   : fare >= 100.0
 *
 * Mapper Output:
 *   Key:   "<payment_type>_FARE_<bucket>"   e.g. "1_FARE_10_25"
 *   Value: "tip_amount,fare_amount,passenger_count"
 */
public class TipBehaviorMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int IDX_PAYMENT    = 9;
    private static final int IDX_FARE       = 10;
    private static final int IDX_TIP        = 13;
    private static final int IDX_PASSENGERS = 3;
    private static final int EXPECTED_COLS  = 31;

    private final Text outKey   = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] fields = line.split(",", -1);

        // Skip header row
        if (fields[0].trim().equals("VendorID")) return;

        // Skip malformed rows with wrong column count
        if (fields.length < EXPECTED_COLS) return;

        try {
            String paymentStr = fields[IDX_PAYMENT].trim();
            if (paymentStr.isEmpty() || paymentStr.equalsIgnoreCase("null")) return;

            int paymentType = Integer.parseInt(paymentStr);

            // Only analyze Credit Card (1) and Cash (2) — the two meaningful categories.
            // Types 3 (No Charge), 4 (Dispute), 5 (Unknown), 6 (Voided) are excluded
            // because they represent edge cases with non-standard tipping dynamics.
            if (paymentType < 1 || paymentType > 2) return;

            double fare       = parseDouble(fields[IDX_FARE]);
            double tip        = parseDouble(fields[IDX_TIP]);
            double passengers = parseDouble(fields[IDX_PASSENGERS]);

            // Skip invalid fares (negative or zero fare rows are metering anomalies)
            if (fare <= 0.0) return;

            String bucket = fareBucket(fare);

            // Composite key: "<payment_type>_FARE_<bucket>"
            outKey.set(paymentType + "_FARE_" + bucket);
            // Value: tip_amount,fare_amount,passenger_count
            outValue.set(tip + "," + fare + "," + passengers);
            context.write(outKey, outValue);

        } catch (Exception e) {
            // Skip any malformed record silently
            context.getCounter("TipBehaviorMapper", "SkippedRecords").increment(1);
        }
    }

    /**
     * Maps a fare amount to a discrete string bucket label.
     * Buckets are designed to capture meaningful economic segments:
     *   - Short city rides   : $0-$5, $5-$10
     *   - Standard rides     : $10-$25
     *   - Long/premium rides : $25-$50
     *   - Airport/tolls      : $50-$100
     *   - Negotiated/outlier : $100+
     */
    private static String fareBucket(double fare) {
        if (fare < 5.0)   return "0_5";
        if (fare < 10.0)  return "5_10";
        if (fare < 25.0)  return "10_25";
        if (fare < 50.0)  return "25_50";
        if (fare < 100.0) return "50_100";
        return "100_PLUS";
    }

    /**
     * Safely parse a double from a CSV field.
     * Returns 0.0 for null, empty, "null", "na", or "nan" strings.
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
