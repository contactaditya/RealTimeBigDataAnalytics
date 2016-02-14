import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
public class singleReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private Text toke = new Text();
	private DoubleWritable val = new DoubleWritable();
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		double min = conf.getDouble("min", 1.0);
		double max = conf.getDouble("max", 1.0);
		double scoreAtMin = conf.getDouble("scoreAtMin", 0.0);
		double neutralPoint = conf.getDouble("neutralPoint", 0.0);
		double scoreAtNeutral = conf.getDouble("scoreAtNeutral", 0.0);
		double scoreAtMax = conf.getDouble("scoreAtMax", 100);
		toke.set(key);
		double value = 0.0;
		for(DoubleWritable v : values){
			value = v.get();
			break;
		}
		if(value == min){
			val.set(scoreAtMin);
		}
		else if(value == max){
			val.set(scoreAtMax);
		}
		else if(value == neutralPoint){
			val.set(scoreAtNeutral);
		}
		else{
			if(value < neutralPoint){
				if(scoreAtMin == scoreAtNeutral){
					val.set(scoreAtNeutral);
				}
				else{
					double diff = ((neutralPoint - value)/(neutralPoint - min)) * (scoreAtNeutral - scoreAtMin);
					val.set(scoreAtNeutral - diff);
				}
			}
			else{
				if(scoreAtMax == scoreAtNeutral){
					val.set(scoreAtNeutral);
				}
				else{
					double diff = ((value - neutralPoint)/(max - neutralPoint)) * (scoreAtMax - scoreAtNeutral);
					val.set(scoreAtNeutral + diff);
				}
			}
		}
		context.write(toke, val);
	}
}
