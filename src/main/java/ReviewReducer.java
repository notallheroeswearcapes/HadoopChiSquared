import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;


/**
 * Reducer class for first class. TODO
 *
 * @author Matthias Eder, 01624856
 * @since 16.04.2021
 */
public class ReviewReducer extends Reducer<Text, ReviewValue, Text, Text> {

    /**
     * Reducer function of first job.
     *
     * @param key     the given key of the key-value pair
     * @param values  the given value of the key-value pair
     * @param context the given Context
     * @throws IOException          thrown in case writing to the context fails
     * @throws InterruptedException thrown in case writing to the context fails
     */
    @Override
    public void reduce(Text key, Iterable<ReviewValue> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Integer> tokenOccurrences = new Hashtable<>();
        for (ReviewValue val : values) {
            if (tokenOccurrences.containsKey(val.getToken().toString())) {
                tokenOccurrences.put(val.getToken().toString(), tokenOccurrences.get(val.getToken().toString()) + val.getCount().get());
            } else {
                tokenOccurrences.put(val.getToken().toString(), val.getCount().get());
            }
        }
        context.write(key, Util.encodeMapAsText(tokenOccurrences, Util.CONCAT_DELIMITER, Util.TOKEN_COUNT_DELIMITER));
    }
}
