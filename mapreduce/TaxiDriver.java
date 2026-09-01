import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * TaxiDriver
 *
 * Usage:
 *   hadoop jar taxi-analysis.jar <input_hdfs_path> <output_hdfs_path>
 *
 * Example (sample):
 *   hadoop jar taxi-analysis.jar /user/bda/taxi/sample/yellow_tripdata_sample.csv /user/bda/taxi/mapreduce/zone_performance_sample
 *
 * Example (full dataset, default if no args):
 *   hadoop jar taxi-analysis.jar
 *
 *   For every pickup zone (PULocationID), compute:
 *     - Total number of trips
 *     - Total revenue
 *     - Average fare
 *     - Average trip distance
 *     - Average trip duration (minutes)
 *     - Average speed (mph)
 *     - Average tip amount
 *
 * Input:  HDFS path /user/bda/taxi/clean/yellow_tripdata_cleaned.csv
 * Output: HDFS path /user/bda/taxi/mapreduce/zone_performance/
 *
 * Output format (tab-separated):
 *   PULocationID  total_trips  total_revenue  avg_fare  avg_distance  avg_duration_min  avg_speed_mph  avg_tip
 */
public class TaxiDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "NYC Taxi Zone Performance Analysis");
        job.setJarByClass(TaxiDriver.class);

        // Mapper and Reducer classes
        job.setMapperClass(TaxiMapper.class);
        job.setReducerClass(TaxiReducer.class);

        // Map output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input/Output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Use single reducer for sorted zone output
        job.setNumReduceTasks(1);

        // HDFS paths
        // Accept command-line args: <input_path> <output_path>
        // Defaults to full dataset paths if not provided
        String inputPath  = (args.length > 0) ? args[0] : "/user/bda/taxi/clean/yellow_tripdata_cleaned.csv";
        String outputPath = (args.length > 1) ? args[1] : "/user/bda/taxi/mapreduce/zone_performance";

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
