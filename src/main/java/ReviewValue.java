import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
    public boolean equals(Object obj) {
        ReviewValue r = (ReviewValue) obj;
        return r.token.toString().equals(this.token.toString()) && r.count.get() == this.count.get();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + token.hashCode();
        result = prime * result + count.hashCode();
        return result;
    }

    @Override
    public int compareTo(@NotNull ReviewValue o) {
        int k = this.token.compareTo(o.token);
        if (k != 0) {
            return k;
        } else {
            return this.count.compareTo(o.count);
        }
    }

}
