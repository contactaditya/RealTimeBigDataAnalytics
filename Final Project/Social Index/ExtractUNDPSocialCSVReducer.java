/*
 * Aditya Gupta, Shikuan Huang, Xiangbo Liang
 * Professor Suzanne McIntosh
 * Realtime and Big Data Analytics
 * 16, April 2015
 * 
 * Reducer For UNDP Social Indicators Data
 */

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExtractUNDPSocialCSVReducer
    extends Reducer<Text, Text, Text, Text> {
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException{
	float total = 0;
	float count = 0;
	float min = 1000000;
	float max = -1000000;
	for (Text value : values) {
	  float val = Float.parseFloat(value.toString());			  
	  total += val;
	  count++;
	  
	  if (val > max) {
		  max = val;
	  }
	  if (val < min) {
		  min = val;
	  }
	}
	float average = total / count;
	String outputStr = String.valueOf(min) + " "
	    + String.valueOf(average) + " " + String.valueOf(max);
	
	context.write(key, new Text(outputStr));
  }
}
