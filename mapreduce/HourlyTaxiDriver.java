import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * HourlyTaxiDriver
 *
 * Driver class for Hourly Peak Demand & Revenue Analysis MapReduce job.
 *
 * Usage:
 *   hadoop jar taxi-hourly-analysis.jar [input_path] [output_path]
 */
public class HourlyTaxiDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "NYC Taxi Hourly Demand & Revenue Analysis");
        job.setJarByClass(HourlyTaxiDriver.class);

        // Mapper & Reducer classes
        job.setMapperClass(HourlyTaxiMapper.class);
        job.setReducerClass(HourlyTaxiReducer.class);

        // Map output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set to 1 reducer to guarantee hours 00 to 23 are sorted in a single output file
        job.setNumReduceTasks(1);

        String inputPath  = (args.length > 0) ? args[0] : "/user/bda/taxi/clean/yellow_tripdata_cleaned.csv";
        String outputPath = (args.length > 1) ? args[1] : "/user/bda/taxi/mapreduce/hourly_performance";

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
