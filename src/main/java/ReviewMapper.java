import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Mapper class for the first job. Gets a body of text, splits and filters the text. Maps the input for the
 * ReviewReducer.
 *
 * @author Matthias Eder, 01624856
 * @since 15.04.2021
 */
public class ReviewMapper extends Mapper<Object, Text, Text, ReviewValue> {

    private final Text emittedKey = new Text();
    private final ReviewValue emittedValue = new ReviewValue();
    private final Set<String> stopWords = new HashSet<>();
    private final static String DELIMITERS =
            "\\s|\\d|\\.|!|\\?|,|;|:|\\(|\\)|\\[|]|\\{|}|-|_|\"|'|`|~|#|&|\\*|%|\\$|/|\\\\";

    /**
     * Setup for the mapper. Reads the cached files.
     *
     * @param context the Context.
     */
    @Override
    protected void setup(Context context) {
        try {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles.length > 0) {
                for (URI uri : cacheFiles) {
                    readFile(uri, context);
                }
            }
        } catch (IOException ex) {
            System.err.println("Exception encountered while setting up the mapper: " + ex.getMessage());
        }
    }

    /**
     * Map function of first job. Pre-processes the input.
     * Emits:   category, (NUM_DOCS, #docsPerCategory)
     * Emits:   category, (token, #docsWithTokenPerCategory)
     * Increments counter for total number of documents read.
     *
     * @param key     the key of the key-value pair
     * @param value   the value of the key-value pair: one JSON line of the input file
     * @param context the Context
     * @throws IOException          thrown in case writing to the context fails
     * @throws InterruptedException thrown in case writing to the context fails
     */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // read in json input file
        final JSONObject obj = new JSONObject(value.toString());
        String reviewText = obj.getString("reviewText");
        String category = obj.getString("category");

        // pre-process review text
        String[] reviewSplit = reviewText.split(DELIMITERS); // tokenization
        reviewSplit = Arrays.stream(reviewSplit)
                .map(String::toLowerCase) // case folding
                .filter(s -> !stopWords.contains(s) && s.length() > 1) // filter stopwords and single characters
                .toArray(String[]::new);

        // set category as the key
        emittedKey.set(category);

        // emit once for number of documents per category
        emittedValue.set(Util.DOCUMENTS_PER_CATEGORY, 1);
        context.write(emittedKey, emittedValue);

        // count number of occurrences of a term per category
        Set<String> tokenSet = new HashSet<>();
        for (String token : reviewSplit) {
            if (!tokenSet.contains(token)) {
                // if token has not been counted for this document, add it to the set and set the token of the key
                tokenSet.add(token);

                // set value and emit occurrence of a token in a category
                emittedValue.set(token, 1);
                context.write(emittedKey, emittedValue);
            }
        }

        // increment counter for total documents
        context.getCounter(Util.Counter.TOTAL_DOCUMENTS).increment(1);
    }

    /**
     * Reads the stopword file. Saves stopwords to a HashSet.
     *
     * @param fileUri the URI of the stopword file
     */
    private void readFile(@NotNull URI fileUri, Context context) {
        try {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path(fileUri.toString());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String stopWord;
            while ((stopWord = reader.readLine()) != null) {
                stopWords.add(stopWord.toLowerCase());
            }
        } catch (IOException ex) {
            System.err.println("Exception encountered while reading stopword file: " + ex.getMessage());
        }
    }
}
