import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class of the fourth and last job.
 *
 * @author Matthias Eder, 01624856
 * @since 25.04.2021
 */
public class DictionaryMapper extends Mapper<Object, Text, NullWritable, Text> {

    private final NullWritable emittedKey = NullWritable.get();
    private final Text emittedValue = new Text();

    /**
     * Map function of the fourth and last job. Takes the output of the chi squared calculation as input. Emits once for
     * each token found in the selected and sorted list of the chi squared file.
     * Emits: null, token
     *
     * @param key     the key of the key-value pair
     * @param value   the value of the key-value pair: 150 space-separated term-value pairs of the chi squared file
     * @param context the Context
     * @throws IOException          thrown in case writing to the context fails
     * @throws InterruptedException thrown in case writing to the context fails
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // split values with a space as delimiter from chi squared output (will contain category as well)
        String[] valueEntry = value.toString().split(Util.SPACE_DELIMITER);

        // iterate over all value pairs starting with the second entry to skip the prefixed category
        String[] pair;
        for (int i = 1; i < valueEntry.length; i++) {
            pair = valueEntry[i].split(Util.TOKEN_COUNT_DELIMITER);
            emittedValue.set(new Text(pair[0])); // set the token of the chi squared pair as emitted value
            context.write(emittedKey, emittedValue); // emit null and the token
        }
    }
}
