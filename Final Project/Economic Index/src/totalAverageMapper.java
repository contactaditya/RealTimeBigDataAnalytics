import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.DoubleWritable;
import java.lang.Exception;
public class totalAverageMapper extends Mapper<Object, Text, Text, DoubleWritable>{
	private Text toke = new Text();
	private DoubleWritable val = new DoubleWritable();
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		line = line.trim();
		String[] tokens = line.split(";");
		toke.set("");
		if(tokens.length > 1){
			try{
				val.set(Double.parseDouble(tokens[1]));
				context.write(toke, val);
			}
			catch(Exception ex){
			}
		}
	}
}