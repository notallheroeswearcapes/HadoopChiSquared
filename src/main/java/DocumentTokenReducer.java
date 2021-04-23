import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

public class DocumentTokenReducer extends Reducer<Text, DocumentTokenValue, Text, Text> {

    private final Text emittedKey = new Text();
    private final Text emittedValue = new Text();

    public void reduce(Text key, Iterable<DocumentTokenValue> values, Context context)
            throws IOException, InterruptedException {
        int tokenOccurrences = 0;
        Map<String, Integer> tokenCategoryOccurrences = new Hashtable<>();
        Map<String, Integer> documentsPerCategory = new Hashtable<>();
        String category;

        // iterate over all values of form: category, count
        // save token occurrences per category in tokenCategoryOccurrences
        // save number of documents per category in documentsPerCategory
        // count total token occurrences in tokenOccurrences
        for (DocumentTokenValue val : values) {
            if (val.getCategory().toString().contains(Util.DOCUMENTS_PER_CATEGORY)) {
                category = val.getCategory().toString().split(Util.CONCAT_DELIMITER)[0];
                documentsPerCategory.put(category, val.getCount().get());
                continue;
            }
            tokenCategoryOccurrences.put(val.getCategory().toString(), val.getCount().get());
            tokenOccurrences += val.getCount().get();
        }

        // iterate over occurrences per token
        for (Map.Entry<String, Integer> entry : tokenCategoryOccurrences.entrySet()) {
            // emit key:    category, token
            // emit value:  x, y, z, n
            emittedKey.set(entry.getKey() + Util.CONCAT_DELIMITER + key);
            emittedValue.set(tokenOccurrences + Util.CONCAT_DELIMITER + // #docs per token
                    entry.getValue() + Util.CONCAT_DELIMITER + // #docs with token per category
                    documentsPerCategory.get(entry.getKey()) + Util.CONCAT_DELIMITER + // #docs per category
                    context.getConfiguration().getLong(Util.Counter.TOTAL_DOCUMENTS.toString(), 0) //#docs in total
            );
            context.write(emittedKey, emittedValue);
        }
    }
}
