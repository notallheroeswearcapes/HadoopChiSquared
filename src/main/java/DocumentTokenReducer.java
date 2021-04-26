import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

/**
 * Reducer class for the second job. Calculates the remaining necessary values to calculate B, C and D in the third job.
 * Outputs all category-token combinations and the respective values.
 *
 * @author Matthias Eder, 01624856
 * @since 18.04.2021
 */
public class DocumentTokenReducer extends Reducer<Text, TextIntWritable, Text, Text> {

    private final Text emittedKey = new Text();
    private final Text emittedValue = new Text();

    /**
     * Reduce function of the third job. Aggregates all necessary values and adds the number of documents per category
     * as well as the total number of documents.
     * Emits:   (category,token),  #docsPerTokenAndCategory,#docsPerToken,#docsPerCategory,#docsTotal
     *
     * @param key       the key of the key-value pair: token
     * @param values    the value of the key-value pair: iterable of TextIntWritable objects for one token
     * @param context   the Context
     * @throws IOException          thrown in case writing to the context fails
     * @throws InterruptedException thrown in case writing to the context fails
     */
    @Override
    public void reduce(Text key, Iterable<TextIntWritable> values, Context context)
            throws IOException, InterruptedException {
        int tokenOccurrences = 0;
        Map<String, Integer> tokenCategoryOccurrences = new Hashtable<>();
        Map<String, Integer> documentsPerCategory = new Hashtable<>();
        String category;

        // iterate over all values of form: category, count
        // save token occurrences per category in tokenCategoryOccurrences
        // save number of documents per category in documentsPerCategory
        // count total token occurrences in tokenOccurrences
        for (TextIntWritable val : values) {
            if (val.getText().toString().contains(Util.DOCUMENTS_PER_CATEGORY)) {
                category = val.getText().toString().split(Util.CONCAT_DELIMITER)[0];
                documentsPerCategory.put(category, val.getCount().get());
                continue;
            }
            tokenCategoryOccurrences.put(val.getText().toString(), val.getCount().get());
            tokenOccurrences += val.getCount().get();
        }

        // iterate over occurrences per token
        for (Map.Entry<String, Integer> entry : tokenCategoryOccurrences.entrySet()) {
            // do not emit if #docs with token per category is 0
            if (entry.getValue() == 0) {
                continue;
            }

            // emit key:    category, token
            // emit value:  x, y, z, n
            emittedKey.set(entry.getKey() + Util.CONCAT_DELIMITER + key);
            emittedValue.set(entry.getValue() + Util.CONCAT_DELIMITER + // #docs with token per category
                    tokenOccurrences + Util.CONCAT_DELIMITER + // #docs per token
                    documentsPerCategory.get(entry.getKey()) + Util.CONCAT_DELIMITER + // #docs per category
                    context.getConfiguration().getLong(Util.Counter.TOTAL_DOCUMENTS.toString(), 0) //#docs in total
            );
            context.write(emittedKey, emittedValue);
        }
    }
}
