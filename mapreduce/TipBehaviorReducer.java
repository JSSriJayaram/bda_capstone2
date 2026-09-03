import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * TipBehaviorReducer
 *
 * MapReduce Job 3: Payment Type & Tipping Behavior Binning
 *
 * Receives all tip/fare/passenger values grouped by composite key
 * "<payment_type>_FARE_<bucket>" and computes tipping behavior statistics.
 *
 * Input:
 *   Key:   "<payment_type>_FARE_<bucket>"  (Text)
 *   Value: "tip_amount,fare_amount,passenger_count" (Text, comma-separated)
 *
 * Output (tab-separated):
 *   payment_fare_bucket | total_trips | avg_tip_amount | avg_tip_pct | zero_tip_rate | avg_passengers
 *
 * Derived Metrics:
 *   avg_tip_amount  = total_tip / total_trips
 *   avg_tip_pct     = (total_tip / total_fare) * 100   — tip as % of base fare
 *   zero_tip_rate   = (zero_tip_count / total_trips) * 100  — % of trips with no tip
 *   avg_passengers  = sum_passengers / total_trips
 *
 * Key Insight:
 *   Cash trips (payment_type=2) will exhibit ~100% zero_tip_rate because the
 *   taximeter only records electronic (card) tips. This surfaces a systemic data
 *   gap in the dataset — cash tipping is underreported — which is itself a
 *   valuable analytical finding for the project.
 */
public class TipBehaviorReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long   totalTrips    = 0;
        long   zeroTipCount  = 0;
        double sumTip        = 0.0;
        double sumFare       = 0.0;
        double sumPassengers = 0.0;

        for (Text val : values) {
            String[] parts = val.toString().split(",", -1);
            if (parts.length < 3) {
                context.getCounter("TipBehaviorReducer", "MalformedValues").increment(1);
                continue;
            }
            try {
                double tip        = Double.parseDouble(parts[0].trim());
                double fare       = Double.parseDouble(parts[1].trim());
                double passengers = Double.parseDouble(parts[2].trim());

                totalTrips++;
                sumTip        += tip;
                sumFare       += fare;
                sumPassengers += passengers;

                // Count trips where tip was exactly zero (no tip recorded)
                if (tip == 0.0) {
                    zeroTipCount++;
                }

            } catch (NumberFormatException e) {
                context.getCounter("TipBehaviorReducer", "ParseErrors").increment(1);
            }
        }

        if (totalTrips == 0) return;

        double avgTipAmount  = sumTip        / totalTrips;
        double avgTipPct     = (sumFare > 0) ? (sumTip / sumFare) * 100.0 : 0.0;
        double zeroTipRate   = ((double) zeroTipCount / totalTrips) * 100.0;
        double avgPassengers = sumPassengers / totalTrips;

        // Output: composite_key <TAB> total_trips avg_tip_amount avg_tip_pct zero_tip_rate avg_passengers
        String result = String.format(
            "%d\t%.2f\t%.2f%%\t%.2f%%\t%.2f",
            totalTrips, avgTipAmount, avgTipPct, zeroTipRate, avgPassengers
        );

        outValue.set(result);
        context.write(key, outValue);
    }
}
