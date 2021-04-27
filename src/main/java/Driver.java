import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Entry point for the chi-squared program. Contains the main and run methods and passes the relevant arguments. Sets up
 * the Hadoop jobs with the respective Map and Reduce classes.
 *
 * @author Matthias Eder, 01624856
 * @since 16.04.2021
 */
public class Driver extends Configured implements Tool {

    /**
     * Main function. Calls the run method and passes input args.
     *
     * @param args input and output file paths as a string array
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
    }

    /**
     * Schedules the Hadoop jobs and handles configurations.
     *
     * @param args arguments passed from main
     * @return 0 if job was successful, else 1 (prints accordingly)
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.printf("%s needs exactly six arguments but received %d.\n" +
                            "1st: path to input file - was: %s\n" +
                            "2nd: path to output of first job - was: %s\n" +
                            "3rd: path to output of second job - was: %s\n" +
                            "4th: path to output of third job - was: %s\n" +
                            "5th: path to output of fourth job - was: %s\n" +
                            "6th: path to stopwords file - was: %s\n",
                    getClass().getSimpleName(),
                    args.length,
                    args[0],
                    args[1],
                    args[2],
                    args[3],
                    args[4],
                    args[5]);
            return -1;
        }

        /* JOB 1 */
        Configuration confJob1 = new Configuration();
        Job job1 = Job.getInstance(confJob1, "chi-squared-first");
        job1.setJarByClass(Driver.class);

        // add file paths for input and output to the first job based on the args passed
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(TextIntWritable.class);
        job1.setNumReduceTasks(1);

        // set job configurations
        job1.getConfiguration().set("mapreduce.output.basename", "chi-squared-first");
        job1.getConfiguration().set("mapreduce.output.textoutputformat.recordseparator", "\n");
        job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", "\t");

        // add Map and Reduce classes to the job
        job1.setMapperClass(ReviewMapper.class);
        job1.setReducerClass(ReviewReducer.class);

        // set distributed file cache for additional files passed to hdfs (e.g. stopwords.txt)
        job1.addCacheFile(new Path(args[5]).toUri());

        // wait for the job2 to complete and print whether the job2 was successful
        int returnValue = job1.waitForCompletion(true) ? 0 : 1;
        long total_documents = job1.getCounters().findCounter(Util.Counter.TOTAL_DOCUMENTS).getValue();
        if (job1.isSuccessful()) {
            System.out.println("Job " + job1.getJobName() + " completed successfully.");
        } else {
            System.out.println("Job " + job1.getJobName() + " failed.");
            return returnValue;
        }

        /* JOB 2 */
        Configuration confJob2 = new Configuration();
        Job job2 = Job.getInstance(confJob2, "chi-squared-second");
        job2.setJarByClass(Driver.class);

        // add file paths for input and output to the job based on the args passed
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(TextIntWritable.class);
        job2.setNumReduceTasks(1);

        // set job configurations
        job2.getConfiguration().set("mapreduce.output.basename", "chi-squared-second");
        job2.getConfiguration().set("mapreduce.output.textoutputformat.recordseparator", "\n");
        job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", "\t");
        job2.getConfiguration().setLong(Util.Counter.TOTAL_DOCUMENTS.toString(), total_documents);

        // add Map and Reduce classes to the job
        job2.setMapperClass(DocumentTokenMapper.class);
        job2.setReducerClass(DocumentTokenReducer.class);

        // wait for the job to complete and print whether the job was successful
        returnValue = job2.waitForCompletion(true) ? 0 : 1;
        if (job2.isSuccessful()) {
            System.out.println("Job " + job2.getJobName() + " completed successfully.");
        } else {
            System.out.println("Job " + job2.getJobName() + " failed.");
            return returnValue;
        }

        /* JOB 3 */
        Configuration confJob3 = new Configuration();
        Job job3 = Job.getInstance(confJob3, "chi-squared-third");
        job3.setJarByClass(Driver.class);

        // add file paths for input and output to the job based on the args passed
        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(ChiSquaredValue.class);
        job3.setNumReduceTasks(1);

        // set job configurations
        job3.getConfiguration().set("mapreduce.output.basename", "chi-squared-third");
        job3.getConfiguration().set("mapreduce.output.textoutputformat.recordseparator", "\n");
        job3.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");

        // add Map and Reduce classes to the job
        job3.setMapperClass(ChiSquaredMapper.class);
        job3.setReducerClass(ChiSquaredReducer.class);

        // wait for the job to complete and print whether the job was successful
        returnValue = job3.waitForCompletion(true) ? 0 : 1;
        if (job3.isSuccessful()) {
            System.out.println("Job " + job3.getJobName() + " completed successfully.");
        } else {
            System.out.println("Job " + job3.getJobName() + " failed.");
            return returnValue;
        }

        /* JOB 4 */
        Configuration confJob4 = new Configuration();
        Job job4 = Job.getInstance(confJob4, "chi-squared-fourth");
        job4.setJarByClass(Driver.class);

        // add file paths for input and output to the job based on the args passed
        FileInputFormat.addInputPath(job4, new Path(args[3]));
        FileOutputFormat.setOutputPath(job4, new Path(args[4]));
        job4.setMapOutputKeyClass(NullWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setNumReduceTasks(1);

        // set job configurations
        job4.getConfiguration().set("mapreduce.output.basename", "chi-squared-fourth");
        job4.getConfiguration().set("mapreduce.output.textoutputformat.recordseparator", "\n");
        job4.getConfiguration().set("mapreduce.output.textoutputformat.separator", "");

        // add Map and Reduce classes to the job
        job4.setMapperClass(DictionaryMapper.class);
        job4.setReducerClass(DictionaryReducer.class);

        // wait for the job to complete and print whether the job was successful
        returnValue = job4.waitForCompletion(true) ? 0 : 1;
        if (job3.isSuccessful()) {
            System.out.println("Job " + job4.getJobName() + " completed successfully.");
        } else {
            System.out.println("Job " + job4.getJobName() + " failed.");
        }
        return returnValue;
    }
}
