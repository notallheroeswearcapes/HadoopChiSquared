import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 *
 */
public class ReviewValue implements WritableComparable<ReviewValue> {

    private Text token;
    private IntWritable count;

    public ReviewValue() {
        token = new Text();
        count = new IntWritable(0);
    }

    public ReviewValue(Text token, IntWritable count) {
        this.token = token;
        this.count = count;
    }

    public ReviewValue(String token, int count) {
        this.token = new Text(token);
        this.count = new IntWritable(count);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        token.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        token.readFields(dataInput);
        count.readFields(dataInput);
    }

    public void set(String token, int count) {
        this.token = new Text(token);
        this.count = new IntWritable(count);
    }

    public Text getToken() {
        return token;
    }

    public void setToken(Text token) {
        this.token = token;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }

    public Text toText() {
        return new Text(this.token + ":" + this.count);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReviewValue that = (ReviewValue) o;
        return token.equals(that.token) && count.equals(that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token, count);
    }

    @Override
    public int compareTo(@NotNull ReviewValue o) {
        int cmp = token.compareTo(o.token);
        if (cmp != 0) {
            return cmp;
        }
        return count.get() - o.count.get();
    }

}
