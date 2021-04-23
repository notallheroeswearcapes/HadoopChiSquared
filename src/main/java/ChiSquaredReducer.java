import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ChiSquaredReducer extends Reducer<Text, ChiSquaredValue, Text, Text> {

    private final Text emittedKey = new Text();
    private final Text emittedValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<ChiSquaredValue> values, Context context)
            throws IOException, InterruptedException {

        int N, A, B, C, D;
        double chiSquared;
        Map<String, Double> chiSquaredValues = new LinkedHashMap<>();

        // set the emitted key (category) to the input key received by the reducer
        emittedKey.set(key);

        for (ChiSquaredValue val : values) {
            N = val.getDocsTotal().get();
            A = val.getDocsPerToken().get();
            B = val.getDocsPerTokenAndCategory().get() - A;
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

        emittedValue.set(Util.encodeMapAsText(sortedChiSquared, " ", Util.TOKEN_COUNT_DELIMITER));
        context.write(emittedKey, emittedValue);
    }
}
