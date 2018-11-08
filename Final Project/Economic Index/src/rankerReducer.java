import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class rankerReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
  public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    for(Text val : values) {
      context.write(key, val);
    }
  }
}
