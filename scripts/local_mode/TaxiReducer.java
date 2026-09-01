import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * TaxiReducer
 *
 * Receives all metric values for a given PULocationID and aggregates them.
 *
 * Input:
 *   Key:   PULocationID (Text)
 *   Value: "fare,total,distance,duration,speed,tip" (Text, comma-separated doubles)
 *
 * Output (tab-separated):
 *   PULocationID  total_trips  total_revenue  avg_fare  avg_distance  avg_duration_min  avg_speed_mph  avg_tip
 */
public class TaxiReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long   totalTrips    = 0;
        double totalRevenue  = 0.0;
        double sumFare       = 0.0;
        double sumDistance   = 0.0;
        double sumDuration   = 0.0;
        double sumSpeed      = 0.0;
        double sumTip        = 0.0;

        for (Text val : values) {
            String[] parts = val.toString().split(",", -1);
            if (parts.length < 6) {
                context.getCounter("TaxiReducer", "MalformedValues").increment(1);
                continue;
            }
            try {
                double fare     = Double.parseDouble(parts[0].trim());
                double total    = Double.parseDouble(parts[1].trim());
                double distance = Double.parseDouble(parts[2].trim());
                double duration = Double.parseDouble(parts[3].trim());
                double speed    = Double.parseDouble(parts[4].trim());
                double tip      = Double.parseDouble(parts[5].trim());

                totalTrips++;
                totalRevenue += total;
                sumFare      += fare;
                sumDistance  += distance;
                sumDuration  += duration;
                sumSpeed     += speed;
                sumTip       += tip;

            } catch (NumberFormatException e) {
                context.getCounter("TaxiReducer", "ParseErrors").increment(1);
            }
        }

        if (totalTrips == 0) return;

        double avgFare     = sumFare     / totalTrips;
        double avgDistance = sumDistance / totalTrips;
        double avgDuration = sumDuration / totalTrips;
        double avgSpeed    = sumSpeed    / totalTrips;
        double avgTip      = sumTip      / totalTrips;

        // Output: PULocationID <TAB> total_trips total_revenue avg_fare avg_distance avg_duration avg_speed avg_tip
        String result = String.format(
            "%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f",
            totalTrips, totalRevenue, avgFare, avgDistance, avgDuration, avgSpeed, avgTip
        );

        outValue.set(result);
        context.write(key, outValue);
    }
}
