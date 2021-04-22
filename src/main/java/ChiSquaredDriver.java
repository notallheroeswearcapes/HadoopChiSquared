import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Entry point for the chi-squared program. Contains the main and run methods and passes the relevant arguments. Sets up
 * the Hadoop job with the Map and Reduce classes.
 *
 * @author Matthias Eder, 01624856
 * @since 16.04.2021
 */
public class ChiSquaredDriver extends Configured implements Tool {

    /**
     * Main function. Calls the run method and passes args.
     *
     * @param args input and output file paths as a string array
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ChiSquaredDriver(), args);
        System.exit(exitCode);
    }

    /**
     * Schedules the Hadoop job and handles configuration.
     *
     * @param args arguments passed from main
     * @return 0 if job was successful, else 1 (prints accordingly)
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.printf("%s needs exactly four arguments.\n" +
                    "First argument: path to input file\n" +
                    "Second argument: path to output of first job\n" +
                    "Third argument: path to output of second job\n" +
                    "Fourth argument: path to stopwords file", getClass().getSimpleName());
            return -1;
        }

        /* JOB 1 */
        Configuration confJob1 = new Configuration();
        Job job1 = Job.getInstance(confJob1, "chi-squared-first");
        job1.setJarByClass(ChiSquaredDriver.class);

        // add file paths for input and output to the first job based on the args passed
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(ReviewValue.class);
        job1.setOutputFormatClass(ChiSquaredOutputFormat.class);
        job1.setNumReduceTasks(1);

        // set job configurations
        job1.getConfiguration().set("mapreduce.output.basename", "chi-squared-first");
        job1.getConfiguration().set("mapreduce.output.textoutputformat.recordseparator", "\n");
        job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", "\t");

        // add Map and Reduce classes to the job
        job1.setMapperClass(ReviewMapper.class);
        job1.setReducerClass(ReviewReducer.class);

        // set distributed file cache for additional files passed to hdfs (e.g. stopwords.txt)
        job1.addCacheFile(new Path(args[3]).toUri());

        // wait for the job2 to complete and print whether the job2 was successful
        int returnValue = job1.waitForCompletion(true) ? 0 : 1;
        long total_documents = job1.getCounters().findCounter(Counter.TOTAL_DOCUMENTS).getValue();
        if (job1.isSuccessful()) {
            System.out.println("Job " + job1.getJobName() + " completed successfully.");
        } else {
            System.out.println("Job " + job1.getJobName() + " failed.");
            return returnValue;
        }

       /* JOB 2 */
       Configuration confJob2 = new Configuration();
       Job job2 = Job.getInstance(confJob2, "chi-squared-second");
       job2.setJarByClass(ChiSquaredDriver.class);

       // add file paths for input and output to the job2 based on the args passed
       FileInputFormat.addInputPath(job2, new Path(args[1]));
       FileOutputFormat.setOutputPath(job2, new Path(args[2]));
       job2.setMapOutputKeyClass(Text.class);
       job2.setMapOutputValueClass(DocumentTokenValue.class);
       job2.setOutputFormatClass(ChiSquaredOutputFormat.class);
       job2.setNumReduceTasks(1);

       // set job2 configurations
       job2.getConfiguration().set("mapreduce.output.basename", "chi-squared-second");
       job2.getConfiguration().set("mapreduce.output.textoutputformat.recordseparator", "\n");
       job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", "\t");
       job2.getConfiguration().setLong(Counter.TOTAL_DOCUMENTS.toString(), total_documents);

       // add Map and Reduce classes to the job2
       job2.setMapperClass(DocumentTokenMapper.class);
       job2.setReducerClass(DocumentTokenReducer.class);

       // wait for the job2 to complete and print whether the job2 was successful
       returnValue = job2.waitForCompletion(true) ? 0 : 1;
       if (job2.isSuccessful()) {
           System.out.println("Job " + job2.getJobName() + " completed successfully.");
       } else {
           System.out.println("Job " + job2.getJobName() + " failed.");
       }
       return returnValue;
    }
}
