import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ChiSquaredMapper extends Mapper<Object, Text, Text, ChiSquaredValue> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    }
}
