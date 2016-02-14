import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.DoubleWritable;
import java.lang.Exception;
public class EDIMapper extends Mapper<Object, Text, Text, DoubleWritable>{
	private Text toke = new Text();
	private DoubleWritable val = new DoubleWritable();
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		line = line.trim();
		String[] tokens = line.split(";");
		if(tokens.length > 1){
			tokens[0] = tokens[0].trim();
			tokens[1] = tokens[1].trim();
			try{
				toke.set(tokens[0]);
				val.set(Double.parseDouble(tokens[1]));
				context.write(toke, val);
			}
			catch(Exception ex){
			}
		}
	}
}
