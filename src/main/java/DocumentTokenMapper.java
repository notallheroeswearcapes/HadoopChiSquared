import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Mapper class for the second job. Gets all tokens of a category and their value for the number of documents with the
 * token per category. Counts total occurrences of a token. Maps the input for the DocumentTokenReducer.
 *
 * @author Matthias Eder, 01624856
 * @since 18.04.2021
 */
public class DocumentTokenMapper extends Mapper<Object, Text, Text, TextIntWritable> {

    private final Text emittedKey = new Text();
    private final TextIntWritable emittedValue = new TextIntWritable();

    /**
     * Map function of the second job. Counts the number of occurrences of a token. Also emits a second time for each
     * token of a category to get the number of documents per category.
     * Emits:   token, (category, #docsPerToken)
     * Emits:   token, (category,NUM_DOCS, #docsPerCategory)
     *
     * @param key     the key of the key-value pair
     * @param value   the value of the key key-value pair: one line of output of the first job
     * @param context the Context
     * @throws IOException          thrown in case writing to the context fails
     * @throws InterruptedException thrown in case writing to the context fails
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] valueEntry = value.toString().split(Util.KEY_VALUE_DELIMITER);
        emittedValue.setText(valueEntry[0]); // set category as part of the value
        String[] reviewValues = valueEntry[1].split(Util.CONCAT_DELIMITER);

        String[] pair;
        String token, count, numDocs = "";
        ArrayList<String> tokenList = new ArrayList<>();
        for (String val : reviewValues) {
            pair = val.split(Util.TOKEN_COUNT_DELIMITER);
            token = pair[0];
            count = pair[1];
            if (token.equals(Util.DOCUMENTS_PER_CATEGORY)) {
                numDocs = count;
                continue;
            }
            tokenList.add(token);
            emittedKey.set(token); // set token as key
            emittedValue.setCount(count); // set count as part of the value
            context.write(emittedKey, emittedValue);
        }

        emittedValue.setText(valueEntry[0] + Util.CONCAT_DELIMITER + Util.DOCUMENTS_PER_CATEGORY);
        emittedValue.setCount(numDocs);
        for (String t : tokenList) {
            emittedKey.set(t);
            context.write(emittedKey, emittedValue);
        }
    }
}
