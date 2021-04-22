import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Custom output format. Separates fields with a colon and tab, separates records with a new line.
 *
 * @param <K> the key of the key-value pair
 * @param <V> the value of the key-value pair
 *
 * @author Matthias Eder, 01624856
 * @since 18.04.2021
 */
public class ChiSquaredOutputFormat<K, V> extends FileOutputFormat<K, V> {

    public static String FIELD_SEPARATOR = "mapreduce.output.textoutputformat.separator";
    public static String RECORD_SEPARATOR = "mapreduce.output.textoutputformat.recordseparator";

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        String fieldSeparator = conf.get(FIELD_SEPARATOR, "\t");
        String recordSeparator = conf.get(RECORD_SEPARATOR, "\n");

        //compress output logic
        CompressionCodec codec = null;
        String extension = "";

        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        Path file = this.getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);

        if (isCompressed) {
            return new ChiSquaredLineRecordWriter<>(new DataOutputStream(codec.createOutputStream(fileOut)), fieldSeparator, recordSeparator);
        } else {
            return new ChiSquaredLineRecordWriter<>(fileOut, fieldSeparator, recordSeparator);
        }

    }
}
