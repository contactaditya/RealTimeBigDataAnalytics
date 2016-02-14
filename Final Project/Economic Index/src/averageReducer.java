import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
public class averageReducer extends Reducer<Text, Text, Text, Text> {
	private Text res = new Text();
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		Path filterFile = new Path("inrelevants.txt");
		HashSet<String> ignoreSet = new HashSet<String>();
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filterFile)));
		String line = br.readLine();
		while(line != null){
			line = line.trim();
			ignoreSet.add(line);
			line = br.readLine();
		}
		double sum = 0;
		double count = 0;
		String country = key.toString();
		if(!ignoreSet.contains(country)){
			for(Text val : values){
				count += 1;
				sum += Double.parseDouble(val.toString());
			}
			if(count != 0){
				res.set(String.valueOf(sum/count));
				context.write(key, res);
			}
		}
	}
}