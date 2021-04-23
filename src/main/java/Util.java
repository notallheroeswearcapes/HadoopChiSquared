import org.apache.hadoop.io.Text;

import java.util.*;

public class Util {

    public static final String CONCAT_DELIMITER = ",";
    public static final String KEY_VALUE_DELIMITER = "\t";
    public static final String TOKEN_COUNT_DELIMITER = ":";
    public static final String DOCUMENTS_PER_CATEGORY = "NUM_DOCS";
    public static final long MAX_ENTRIES = 150;

    public static enum Counter {
        TOTAL_DOCUMENTS
    }

    public static <K, V> Text encodeMapAsText(Map<K, V> map, String entryDelimiter, String pairDelimiter) {
        StringJoiner sj = new StringJoiner(entryDelimiter);
        for (Map.Entry<K, V> entry : map.entrySet()) {
            String key = entry.getKey().toString();
            String val = entry.getValue().toString();
            sj.add(String.join(pairDelimiter, key,val));
        }
        return new Text(sj.toString());
    }

    /**
     * Calculates the chi-square statistic measuring the dependence between a token t and a category c.
     *
     * @param A number of documents in c which contain t
     * @param B number of documents not in c which contain t
     * @param C number of documents in c without t
     * @param D number of documents not in c without t
     * @param N total number of documents
     * @return the calculated chi-square value
     */
    public static double calculateChiSquared(int A, int B, int C, int D, int N) {
        double numerator = N * Math.pow((A * D - B * C), 2);
        double denominator = (A + B) * (A + C) * (B + D) * (C + D);
        return numerator / denominator;
    }
}
