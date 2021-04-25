import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Custom WritableComparable for the calculation of the chi squared values. Contains the number of documents per token
 * and category, the number of documents per token, the number of documents per category and the total number of
 * documents for one category-token combination.
 *
 * @author Matthias Eder, 01624856
 * @since 23.04.2021
 */
public class ChiSquaredValue implements WritableComparable<ChiSquaredValue> {

    private Text token;
    private IntWritable docsPerTokenAndCategory;
    private IntWritable docsPerToken;
    private IntWritable docsPerCategory;
    private IntWritable docsTotal;

    public ChiSquaredValue() {
        token = new Text();
        docsPerTokenAndCategory = new IntWritable();
        docsPerToken = new IntWritable();
        docsPerCategory = new IntWritable();
        docsTotal = new IntWritable();
    }

    public ChiSquaredValue(Text token,
                           IntWritable docsPerTokenAndCategory,
                           IntWritable docsPerToken,
                           IntWritable docsPerCategory,
                           IntWritable docsTotal) {
        this.token = token;
        this.docsPerTokenAndCategory = docsPerTokenAndCategory;
        this.docsPerToken = docsPerToken;
        this.docsPerCategory = docsPerCategory;
        this.docsTotal = docsTotal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChiSquaredValue that = (ChiSquaredValue) o;
        return token.equals(that.token)
                && docsPerTokenAndCategory.equals(that.docsPerTokenAndCategory)
                && docsPerToken.equals(that.docsPerToken)
                && docsPerCategory.equals(that.docsPerCategory)
                && docsTotal.equals(that.docsTotal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, docsPerTokenAndCategory, docsPerToken, docsPerCategory, docsTotal);
    }

    @Override
    public int compareTo(@NotNull ChiSquaredValue o) {
        int cmp = token.compareTo(o.token);
        if (cmp != 0) {
            return cmp;
        }
        cmp = docsPerTokenAndCategory.get() - o.docsPerTokenAndCategory.get();
        if (cmp != 0) {
            return cmp;
        }
        cmp = docsPerToken.get() - o.docsPerToken.get();
        if (cmp != 0) {
            return cmp;
        }
        cmp = docsPerCategory.get() - o.docsPerCategory.get();
        if (cmp != 0) {
            return cmp;
        }
        return docsTotal.get() - o.docsTotal.get();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        token.write(dataOutput);
        docsPerTokenAndCategory.write(dataOutput);
        docsPerToken.write(dataOutput);
        docsPerCategory.write(dataOutput);
        docsTotal.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        token.readFields(dataInput);
        docsPerTokenAndCategory.readFields(dataInput);
        docsPerToken.readFields(dataInput);
        docsPerCategory.readFields(dataInput);
        docsTotal.readFields(dataInput);
    }

    public Text getToken() {
        return token;
    }

    public void setToken(Text token) {
        this.token = token;
    }

    public void setToken(String token) {
        this.token = new Text(token);
    }

    public IntWritable getDocsPerToken() {
        return docsPerToken;
    }

    public void setDocsPerToken(IntWritable docsPerToken) {
        this.docsPerToken = docsPerToken;
    }

    public void setDocsPerToken(String docsPerToken) {
        this.docsPerToken = new IntWritable(Integer.parseInt(docsPerToken));
    }

    public IntWritable getDocsPerTokenAndCategory() {
        return docsPerTokenAndCategory;
    }

    public void setDocsPerTokenAndCategory(IntWritable docsPerTokenAndCategory) {
        this.docsPerTokenAndCategory = docsPerTokenAndCategory;
    }

    public void setDocsPerTokenAndCategory(String docsPerTokenAndCategory) {
        this.docsPerTokenAndCategory = new IntWritable(Integer.parseInt(docsPerTokenAndCategory));
    }

    public IntWritable getDocsPerCategory() {
        return docsPerCategory;
    }

    public void setDocsPerCategory(IntWritable docsPerCategory) {
        this.docsPerCategory = docsPerCategory;
    }

    public void setDocsPerCategory(String docsPerTokenAndCategory) {
        this.docsPerCategory = new IntWritable(Integer.parseInt(docsPerTokenAndCategory));
    }

    public IntWritable getDocsTotal() {
        return docsTotal;
    }

    public void setDocsTotal(IntWritable docsTotal) {
        this.docsTotal = docsTotal;
    }

    public void setDocsTotal(String docsTotal) {
        this.docsTotal = new IntWritable(Integer.parseInt(docsTotal));
    }
}
