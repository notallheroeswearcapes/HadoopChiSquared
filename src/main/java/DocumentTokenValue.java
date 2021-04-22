import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DocumentTokenValue implements WritableComparable<DocumentTokenValue> {

    private Text category;
    private IntWritable count;

    public DocumentTokenValue() {
        category = new Text();
        count = new IntWritable(0);
    }

    public DocumentTokenValue(Text category, IntWritable count) {
        this.category = category;
        this.count = count;
    }

    public DocumentTokenValue(String category, int count) {
        this.category = new Text(category);
        this.count = new IntWritable(count);
    }

    public Text getCategory() {
        return category;
    }

    public void setCategory(Text category) {
        this.category = category;
    }

    public void setCategory(String category) {
        this.category = new Text(category);
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
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + category.hashCode();
        result = prime * result + count.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int compareTo(@NotNull DocumentTokenValue o) {
        int k = this.category.compareTo(o.category);
        if (k != 0) {
            return k;
        } else {
            return this.count.compareTo(o.count);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        category.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        category.readFields(dataInput);
        count.readFields(dataInput);
    }
}
