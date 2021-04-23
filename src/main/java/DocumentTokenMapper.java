import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class DocumentTokenMapper extends Mapper<Object, Text, Text, DocumentTokenValue> {

    private final Text emittedKey = new Text();
    private final DocumentTokenValue emittedValue = new DocumentTokenValue();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] valueEntry = value.toString().split(Util.KEY_VALUE_DELIMITER);
        emittedValue.setCategory(valueEntry[0]); // set category as part of the value
        String[] reviewValues = valueEntry[1].split(Util.CONCAT_DELIMITER);

        String[] pair;
        String token, count, numDocs = "";
        ArrayList<String> tokenList = new ArrayList<>();
        for (String val : reviewValues) {
            pair = val.split(Util.TOKEN_COUNT_DELIMITER);
            token = pair[0];
            count = pair[1];
            if (token.equals(Util.DOCUMENTS_PER_CATEGORY))  {
                numDocs = count;
                continue;
            }
            tokenList.add(token);
            emittedKey.set(token); // set token as key
            emittedValue.setCount(count); // set count as part of the value
            context.write(emittedKey, emittedValue);
        }

        emittedValue.setCategory(valueEntry[0] + Util.CONCAT_DELIMITER + Util.DOCUMENTS_PER_CATEGORY);
        emittedValue.setCount(numDocs);
        for (String t : tokenList) {
            emittedKey.set(t);
            context.write(emittedKey, emittedValue);
        }
    }
}
