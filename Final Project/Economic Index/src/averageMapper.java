import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.DoubleWritable;
import java.lang.Exception;
import java.util.HashSet;

  public class averageMapper extends Mapper<Object, Text, Text, Text> {
    private Text toke = new Text();
    private Text val = new Text();
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      String line = value.toString();
      line = line.trim();
      String[] tokens = line.split(",");
      int years = conf.getInt("yr", 0) > 0 ? conf.getInt("yr", 0) : tokens.length;
      tokens[0] = tokens[0].trim();
      toke.set(tokens[0]);
      if(tokens[0].length() > 0 && tokens[0].charAt(0) == '"') {
	String specialName = tokens[0] + "," + tokens[1];
	specialName = specialName.substring(1, specialName.length() - 1);
	toke.set(specialName);
      }
      boolean overflows = conf.getBoolean("overflows", false);
      for(int i = tokens.length - 1; i >= tokens.length - years; i--) {
	double num = Double.MIN_VALUE;
	try {
	  if(!tokens[i].isEmpty() && tokens[i].charAt(0) == '"') {
	    if(tokens[i].length() > 2) {
	      tokens[i] = tokens[i].substring(1, tokens[i].length() - 1);
	    }
	    else {
	      tokens[i] = "";
	    }
	  }
	  num = !tokens[i].isEmpty()? Double.parseDouble(tokens[i]) : Double.MIN_VALUE;		
	} catch(NumberFormatException e) {
	  break;
	}
	if(num != Double.MIN_VALUE) {
	  val.set(String.valueOf(num));
	  context.write(toke, val);
	}
      }
    }
  }
