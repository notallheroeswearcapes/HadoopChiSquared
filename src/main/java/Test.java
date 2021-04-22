import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.StringJoiner;

public class Test {

    public static void main(String[] args) {
        Hashtable<Text, Integer> tokenOccurrences = new Hashtable<>();

        ArrayList<ReviewValue> values = new ArrayList<>();
        values.add(new ReviewValue("spend", 1));
        values.add(new ReviewValue("like", 1));
        values.add(new ReviewValue("spend", 1));
        values.add(new ReviewValue("forever", 1));
        values.add(new ReviewValue("emma", 1));
        values.add(new ReviewValue("like", 1));

        for (ReviewValue val : values) {
            if (tokenOccurrences.containsKey(val.getToken())) {
                tokenOccurrences.put(val.getToken(), tokenOccurrences.get(val.getToken()) + val.getCount().get());
            } else {
                tokenOccurrences.put(val.getToken(), val.getCount().get());
            }
        }

        System.out.println(encodeTokenOccurrencesAsText(tokenOccurrences));
    }

    private static Text encodeTokenOccurrencesAsText(Hashtable<Text, Integer> tokenOccurrences) {
        StringJoiner sj = new StringJoiner(",");
        for (Map.Entry<Text, Integer> entry : tokenOccurrences.entrySet()) {
            sj.add(String.join(":", entry.getKey().toString(), entry.getValue().toString()));
        }
        return new Text(sj.toString());
    }
}
