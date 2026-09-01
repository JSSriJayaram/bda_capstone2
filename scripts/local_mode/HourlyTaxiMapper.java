import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * HourlyTaxiMapper
 *
 * Extracts pickup hour as the key and emits trip metrics for hourly aggregation.
 *
 * Key:   pickup_hour (0 to 23)
 * Value: "total_amount,passenger_count,trip_duration_min,tip_amount,fare_amount"
 */
public class HourlyTaxiMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int IDX_TOTAL       = 16;
    private static final int IDX_PASSENGERS  = 3;
    private static final int IDX_DURATION    = 20;
    private static final int IDX_TIP         = 13;
    private static final int IDX_FARE        = 10;
    private static final int IDX_HOUR        = 22;
    private static final int EXPECTED_COLS   = 31;

    private final Text outKey   = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] fields = line.split(",", -1);

        // Skip header
        if (fields[0].trim().equals("VendorID")) return;

        // Ensure expected columns
        if (fields.length < EXPECTED_COLS) return;

        try {
            String hourStr = fields[IDX_HOUR].trim();
            if (hourStr.isEmpty()) return;

            // Format hour nicely as 2 digits e.g. "00", "01", ..., "23"
            int hourInt = Integer.parseInt(hourStr);
            if (hourInt < 0 || hourInt > 23) return;
            String hourPadded = String.format("%02d", hourInt);

            double total      = parseDouble(fields[IDX_TOTAL]);
            double passengers = parseDouble(fields[IDX_PASSENGERS]);
            double duration   = parseDouble(fields[IDX_DURATION]);
            double tip        = parseDouble(fields[IDX_TIP]);
            double fare       = parseDouble(fields[IDX_FARE]);

            outKey.set(hourPadded);
            outValue.set(total + "," + passengers + "," + duration + "," + tip + "," + fare);
            context.write(outKey, outValue);

        } catch (Exception e) {
            context.getCounter("HourlyTaxiMapper", "SkippedRecords").increment(1);
        }
    }

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
