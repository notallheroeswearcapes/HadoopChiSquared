import org.apache.hadoop.io.Text;

import java.util.*;

/**
 * Auxiliary class for global variables, counter enums and helper functions.
 *
 * @author Matthias Eder, 01624856
 * @since 17.04.2021
 */
public class Util {

    public static final String CONCAT_DELIMITER = ",";
    public static final String KEY_VALUE_DELIMITER = "\t";
    public static final String TOKEN_COUNT_DELIMITER = ":";
    public static final String SPACE_DELIMITER = " ";
    public static final String DOCUMENTS_PER_CATEGORY = "NUM_DOCS";
    public static final long MAX_ENTRIES = 150;

    public enum Counter {
        TOTAL_DOCUMENTS
    }

    /**
     * Helper function to encode a map of generic types as a Text.
     *
     * @param map            the map to be encoded
     * @param entryDelimiter the delimiter between entries
     * @param pairDelimiter  the delimiter between one pair (one entry)
     * @param <K>            generic type of the map key
     * @param <V>            generic type of the map value
     * @return the encoded map as a Text
     */
    public static <K, V> Text encodeMapAsText(Map<K, V> map, String entryDelimiter, String pairDelimiter) {
        StringJoiner sj = new StringJoiner(entryDelimiter);
        for (Map.Entry<K, V> entry : map.entrySet()) {
            String key = entry.getKey().toString();
            String val = entry.getValue().toString();
            sj.add(String.join(pairDelimiter, key, val));
        }
        return new Text(sj.toString());
    }

    /**
     * Helper function to encode a set of strings as a Text.
     *
     * @param set            the set to be encoded
     * @param entryDelimiter the delimiter between entries
     * @return the encoded map as a Text
     */
    public static Text encodeSetAsText(Set<String> set, String entryDelimiter) {
        StringJoiner sj = new StringJoiner(entryDelimiter);
        for (String entry : set) {
            sj.add(entry);
        }
        return new Text(sj.toString());
    }

    /**
     * Calculates the chi-squared statistic measuring the dependence between a token t and a category c.
     *
     * @param A number of documents in c which contain t
     * @param B number of documents not in c which contain t
     * @param C number of documents in c without t
     * @param D number of documents not in c without t
     * @param N total number of documents
     * @return the calculated chi-squared value
     */
    public static double calculateChiSquared(double A, double B, double C, double D, double N) {
        double numerator = N * Math.pow(((A * D) - (B * C)), 2);
        double res = (A + B) * (A + C) * (B + D) * (C + D);
        return numerator / res;
    }
}
