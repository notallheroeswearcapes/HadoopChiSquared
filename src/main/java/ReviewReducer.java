import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;


/**
 * Reducer class for first class. Reduces the received key-value pairs by category. Counts the value A per category and
 * token.
 *
 * @author Matthias Eder, 01624856
 * @since 16.04.2021
 */
public class ReviewReducer extends Reducer<Text, TextIntWritable, Text, Text> {

    /**
     * Reducer function of first job. Reduces by category. Counts the number of documents with a token per category and
     * the number of documents per category.
     * Emits:   category, token_1:#docsToken_1Category,...,token_n:#docsToken_nCategory,NUM_DOCS:#docsPerCategory
     *
     * @param key     the given key of the key-value pair: category
     * @param values  the given value of the key-value pair: iterable of TextIntWritable objects for one category
     * @param context the given Context
     * @throws IOException          thrown in case writing to the context fails
     * @throws InterruptedException thrown in case writing to the context fails
     */
    @Override
    public void reduce(Text key, Iterable<TextIntWritable> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Integer> tokenOccurrences = new Hashtable<>();
        String token;
        for (TextIntWritable val : values) {
            token = val.getText().toString();
            if (tokenOccurrences.containsKey(token)) {
                tokenOccurrences.put(token, tokenOccurrences.get(token) + val.getCount().get());
            } else {
                tokenOccurrences.put(token, val.getCount().get());
            }
        }
        context.write(key, Util.encodeMapAsText(tokenOccurrences, Util.CONCAT_DELIMITER, Util.TOKEN_COUNT_DELIMITER));
    }
}
