import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
public class twoWayReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private Text toke = new Text();
	private DoubleWritable val = new DoubleWritable();
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		double left = conf.getDouble("leftBase", 1.0);
		double right = conf.getDouble("rightBase", 1.0);
		double optimal = conf.getDouble("optimalPoint", 1.0);
		double weight = conf.getDouble("weight", 1.0);
		boolean hasBoundry = conf.getBoolean("hasBoundry", false);
		toke.set(key);
		double value = 0.0;
		for(DoubleWritable v : values){
			value = v.get();
			break;
		}
		if(!hasBoundry){
			if(value == optimal){
				val.set(weight);
			}
			else if(value < optimal){
				val.set(weight - (Math.abs(optimal - value)/Math.abs(optimal - left)) * weight);
			}
			else{
				val.set(weight - (Math.abs(value - optimal)/Math.abs(right - optimal)) * weight);
			}
			context.write(toke, val);
		}
		else{
			double leftB = conf.getDouble("leftBoundry", left);
			double rightB = conf.getDouble("rightBoundry", right);
			double scorelimit = conf.getDouble("scorelimit", weight);
			if(value == optimal){
				val.set(weight);
			}
			else if(value < optimal && value > left){
				val.set((Math.abs(value - left)/Math.abs(optimal - left)) * weight);
			}
			else if(value > optimal && value < right){
				val.set((Math.abs(right - value)/Math.abs(right - optimal)) * weight);
			}
			else if(value < left && value > leftB){
				val.set((Math.abs(left - value)/Math.abs(left - leftB)) * scorelimit);
			}
			else if(value > right && value < rightB){
				val.set((Math.abs(value - right)/Math.abs(rightB - right)) * scorelimit);
			}
			else{
				val.set(scorelimit);
			}
			context.write(toke, val);
		}
	}
}

