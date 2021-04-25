import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Custom WritableComparable with two fields: one for a Text and one for an IntWritable.
 *
 * @author Matthias Eder, 01624856
 * @since 18.04.2021
 */
public class TextIntWritable implements WritableComparable<TextIntWritable> {

    private Text text;
    private IntWritable count;

    public TextIntWritable() {
        text = new Text();
        count = new IntWritable();
    }

    public TextIntWritable(Text text, IntWritable count) {
        this.text = text;
        this.count = count;
    }

    public TextIntWritable(String text, int count) {
        this.text = new Text(text);
        this.count = new IntWritable(count);
    }

    public void set(String text, int count) {
        this.text = new Text(text);
        this.count = new IntWritable(count);
    }

    public Text getText() {
        return text;
    }

    public void setText(Text text) {
        this.text = text;
    }

    public void setText(String text) {
        this.text = new Text(text);
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }

    public void setCount(String count) {
        this.count = new IntWritable(Integer.parseInt(count));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextIntWritable that = (TextIntWritable) o;
        return text.equals(that.text) && count.equals(that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(text, count);
    }

    @Override
    public int compareTo(@NotNull TextIntWritable o) {
        int cmp = text.compareTo(o.text);
        if (cmp != 0) {
            return cmp;
        }
        return count.get() - o.count.get();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        text.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        text.readFields(dataInput);
        count.readFields(dataInput);
    }
}
