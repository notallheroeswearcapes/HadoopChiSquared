import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

/**
 * Reducer class of the fourth and last job. Outputs an alphabetically sorted list of all occurring tokens.
 *
 * @author Matthias Eder, 01624856
 * @since 25.04.2021
 */
public class DictionaryReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    private final NullWritable emittedKey = NullWritable.get();
    private final Text emittedValue = new Text();

    /**
     * Reducer of the fourth and last job. Merges the 150 selected tokens of each category after the the chi squared
     * calculation to one space-separated dictionary sorted alphabetically in ascending order.
     *
     * @param key     the key of the key-value pair: null to merge all tokens
     * @param values  the value of the key-value pair: the merged dictionary as a string-encoded and space-separated map
     * @param context the Context
     * @throws IOException          thrown in case writing to the context fails
     * @throws InterruptedException thrown in case writing to the context fails
     */
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TreeSet<String> dictionary = new TreeSet<>();

        for (Text val : values) {
            dictionary.add(val.toString());
        }

        emittedValue.set(Util.encodeSetAsText(dictionary, Util.SPACE_DELIMITER));
        context.write(emittedKey, emittedValue);
    }
}
