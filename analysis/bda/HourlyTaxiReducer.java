import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * HourlyTaxiReducer
 *
 * Aggregates metrics per pickup hour (00 to 23).
 *
 * Input:
 *   Key:   pickup_hour (00 - 23)
 *   Value: "total_amount,passenger_count,trip_duration_min,tip_amount,fare_amount"
 *
 * Output (tab-separated):
 *   pickup_hour  total_trips  total_revenue  avg_revenue_per_trip  avg_passengers  avg_duration_min  avg_tip  tip_percentage
 */
public class HourlyTaxiReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long   totalTrips      = 0;
        double totalRevenue    = 0.0;
        double sumPassengers   = 0.0;
        double sumDuration     = 0.0;
        double sumTip          = 0.0;
        double sumFare         = 0.0;

        for (Text val : values) {
            String[] parts = val.toString().split(",", -1);
            if (parts.length < 5) {
                context.getCounter("HourlyTaxiReducer", "MalformedValues").increment(1);
                continue;
            }
            try {
                double total      = Double.parseDouble(parts[0].trim());
                double passengers = Double.parseDouble(parts[1].trim());
                double duration   = Double.parseDouble(parts[2].trim());
                double tip        = Double.parseDouble(parts[3].trim());
                double fare       = Double.parseDouble(parts[4].trim());

                totalTrips++;
                totalRevenue   += total;
                sumPassengers  += passengers;
                sumDuration    += duration;
                sumTip         += tip;
                sumFare        += fare;

            } catch (NumberFormatException e) {
                context.getCounter("HourlyTaxiReducer", "ParseErrors").increment(1);
            }
        }

        if (totalTrips == 0) return;

        double avgRevenue   = totalRevenue / totalTrips;
        double avgPass      = sumPassengers / totalTrips;
        double avgDuration  = sumDuration / totalTrips;
        double avgTip       = sumTip / totalTrips;
        double tipPct       = (sumFare > 0) ? (sumTip / sumFare) * 100.0 : 0.0;

        // Output: hour <TAB> total_trips total_revenue avg_revenue_per_trip avg_passengers avg_duration avg_tip tip_percentage
        String result = String.format(
            "%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f%%",
            totalTrips, totalRevenue, avgRevenue, avgPass, avgDuration, avgTip, tipPct
        );

        outValue.set(result);
        context.write(key, outValue);
    }
}
