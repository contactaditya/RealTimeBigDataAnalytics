import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class EDIReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  private Text toke = new Text();
  private DoubleWritable val = new DoubleWritable();
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    double numOfIndicators = conf.getDouble("numOfIndicators", 1.0);
    toke.set(key);
    double sum = 0;
    int counter = 0;
    for(DoubleWritable v : values) {
      counter++;
      sum += v.get();
    }
    if(counter == numOfIndicators) {
      val.set(sum);
      context.write(toke, val);
    }
  }
}
