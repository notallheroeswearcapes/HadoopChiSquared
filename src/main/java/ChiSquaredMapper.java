import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class for the third job. Maps by category to prepare the values for the chi squared calculation in the
 * ChiSquaredReducer.
 *
 * @author Matthias Eder, 01624856
 * @since 22.04.2021
 */
public class ChiSquaredMapper extends Mapper<Object, Text, Text, ChiSquaredValue> {

    private final Text emittedKey = new Text();
    private final ChiSquaredValue emittedValue = new ChiSquaredValue();

    /**
     * Map function of the third job. Splits the four values for each category-token pair and wraps them inside a
     * ChiSquaredValue. Emits per category to achieve the required output format.
     * Emits:   category, (token, #docsPerTokenAndCategory, #docsPerToken, #docsPerCategory, #docsTotal)
     *
     * @param key     the key of the key-value pair
     * @param value   the value of the key-value pair: one combination of category and token with the respective values
     * @param context the Context
     * @throws IOException          thrown in case writing to the context fails
     * @throws InterruptedException thrown in case writing to the context fails
     */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] valueEntry = value.toString().split(Util.KEY_VALUE_DELIMITER);
        String[] keyPart = valueEntry[0].split(Util.CONCAT_DELIMITER);
        String category = keyPart[0];
        String token = keyPart[1];

        // set category as the key
        emittedKey.set(category);

        // split the value part of the read entry and set the fields of the emitted value
        String[] valuePart = valueEntry[1].split(Util.CONCAT_DELIMITER);
        if (valuePart.length != 4) {
            System.err.println("The value part of the read entry should contain exactly 4 calculated values.");
            throw new InterruptedException("Not enough values.");
        }
        emittedValue.setToken(token);
        emittedValue.setDocsPerTokenAndCategory(valuePart[0]);
        emittedValue.setDocsPerToken(valuePart[1]);
        emittedValue.setDocsPerCategory(valuePart[2]);
        emittedValue.setDocsTotal(valuePart[3]);

        // emit key-value pair of form: category, ChiSquaredValue
        context.write(emittedKey, emittedValue);
    }
}
