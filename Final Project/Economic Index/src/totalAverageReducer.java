import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class totalAverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  private Text toke = new Text();
  private DoubleWritable val = new DoubleWritable();
  private Double sum = (double) 0;
  private int counter = 0; 
	
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    for(DoubleWritable val : values) {
      counter++;
      sum += val.get() / 1000;
    }
    val.set((sum/counter) * 1000);
    context.write(toke, val);
  }
}
