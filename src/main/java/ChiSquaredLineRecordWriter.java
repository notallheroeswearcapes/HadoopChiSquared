import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Custom Line-Record-Writer.
 *
 * @param <K> the key of the key-value pair
 * @param <V> the value of the key-value pair
 *
 * @author Matthias Eder, 01624856
 * @since 18.04.2021
 */
public class ChiSquaredLineRecordWriter<K, V> extends RecordWriter<K, V> {

    protected DataOutputStream out;
    private final byte[] fieldSeparator;
    private final byte[] recordSeparator;

    public ChiSquaredLineRecordWriter(DataOutputStream out, String fieldSeparator, String recordSeparator) {
        this.out = out;
        this.fieldSeparator = fieldSeparator.getBytes(StandardCharsets.UTF_8);
        this.recordSeparator = recordSeparator.getBytes(StandardCharsets.UTF_8);
    }

    public ChiSquaredLineRecordWriter(DataOutputStream out) {
        this(out, "\t", "\n");
    }

    private void writeObject(Object o) throws IOException {
        if (o instanceof Text) {
            Text to = (Text) o;
            this.out.write(to.getBytes(), 0, to.getLength());
        } else {
            this.out.write(o.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public synchronized void write(K key, V value) throws IOException {
        boolean nullKey = key == null || key instanceof NullWritable;
        boolean nullValue = value == null || value instanceof NullWritable;
        if (!nullKey || !nullValue) {
            if (!nullKey) {
                this.writeObject(key);
            }
            if (!nullKey && !nullValue) {
                this.out.write(this.fieldSeparator);
            }
            if (!nullValue) {
                this.writeObject(value);
            }
            this.out.write(recordSeparator);
        }
    }

    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException {
        this.out.close();
    }
}
