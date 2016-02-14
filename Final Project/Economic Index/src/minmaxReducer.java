import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
public class minmaxReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private Text toke = new Text();
	private DoubleWritable val = new DoubleWritable();
	private Double max = (double) 0;
	private Double min = Double.MAX_VALUE;
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		for(DoubleWritable val : values){
			Double current = val.get();
			max = current > max? current : max;
			min = current < min? current : min;
		}
		toke.set("");
		val.set(min);
		context.write(toke, val);
		val.set(max);
		context.write(toke, val);
	}
}