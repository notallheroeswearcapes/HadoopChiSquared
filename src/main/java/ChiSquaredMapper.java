import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class for the third job.
 *
 * @author Matthias Eder, 01624856
 * @since 18.04.2021
 */
public class ChiSquaredMapper extends Mapper<Object, Text, Text, ChiSquaredValue> {

    private final Text emittedKey = new Text();
    private final ChiSquaredValue emittedValue = new ChiSquaredValue();

    /**
     * TODO
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
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
