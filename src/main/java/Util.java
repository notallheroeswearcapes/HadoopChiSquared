import org.apache.hadoop.io.Text;

import java.util.Hashtable;
import java.util.Map;
import java.util.StringJoiner;

public class Util {

    public static final String CONCAT_DELIMITER = ",";
    public static final String KEY_VALUE_DELIMITER = "\t";
    public static final String TOKEN_COUNT_DELIMITER = ":";
    public static final String DOCUMENTS_PER_CATEGORY = "NUM_DOCS";

    public static Text encodeHashtableAsText(Hashtable<String, Integer> tokenOccurrences) {
        StringJoiner sj = new StringJoiner(",");
        for (Map.Entry<String, Integer> entry : tokenOccurrences.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue().toString();
            sj.add(String.join(":", key,val));
        }
        return new Text(sj.toString());
    }
}
