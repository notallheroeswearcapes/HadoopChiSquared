import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reducer class for the third job. Calculates the chi-squared values. Outputs all tokens and their respective chi
 * squared values in a sorted list in descending order per category.
 *
 * @author Matthias Eder, 01624856
 * @since 22.04.2021
 */
public class ChiSquaredReducer extends Reducer<Text, ChiSquaredValue, Text, Text> {

    private final Text emittedKey = new Text();
    private final Text emittedValue = new Text();

    /**
     * Reduce function of the third job. Computes the values for A, B, C and D accordingly for each category-token
     * combination, calculates chi squared values and sorts them in descending order taking only the highest 150 values.
     * Emits:   category, token_1:chi_squared_1 ... token_n:chi_squared_n
     *
     * @param key     the key of the key-value pair: category
     * @param values  the value of the key-value pair: iterable of ChiSquared objects for one category
     * @param context the Context
     * @throws IOException          thrown in case writing to the context fails
     * @throws InterruptedException thrown in case writing to the context fails
     */
    @Override
    protected void reduce(Text key, Iterable<ChiSquaredValue> values, Context context)
            throws IOException, InterruptedException {

        double N, A, B, C, D;
        double chiSquared;
        Map<String, Double> chiSquaredValues = new LinkedHashMap<>();

        // set the emitted key (category) to the input key received by the reducer
        emittedKey.set(key);

        // compute the required values of A, B, C, and D to calculate the chi squared value of each token of a category
        for (ChiSquaredValue val : values) {
            N = val.getDocsTotal().get();
            A = val.getDocsPerTokenAndCategory().get();
            B = val.getDocsPerToken().get() - A;
            C = val.getDocsPerCategory().get() - A;
            D = N - (A + B + C);
            chiSquared = Util.calculateChiSquared(A, B, C, D, N);
            chiSquaredValues.put(val.getToken().toString(), chiSquared);
        }

        // sort map with token-chi-square pairs
        Map<String, Double> sortedChiSquared =
                chiSquaredValues.entrySet().stream()
                        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                        .limit(Util.MAX_ENTRIES)
                        .collect(Collectors.toMap(
                                Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new
                        ));

        emittedValue.set(Util.encodeMapAsText(sortedChiSquared, Util.SPACE_DELIMITER, Util.TOKEN_COUNT_DELIMITER));
        context.write(emittedKey, emittedValue);
    }
}
