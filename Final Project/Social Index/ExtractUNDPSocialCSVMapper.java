/*
 * Aditya Gupta, Shikuan Huang, Xiangbo Liang
 * Professor Suzanne McIntosh
 * Realtime and Big Data Analytics
 * 16, April 2015
 * 
 * Mapper For UNDP Social Indicators Data
 */

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class ExtractUNDPSocialCSVMapper extends Mapper<LongWritable, Text, Text, Text> {
	
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    if (!line.isEmpty()) {  
      Configuration conf = context.getConfiguration();
      String colsStr = conf.get("cols");
      int cols = 0;
	  
      String[] tokens = line.split(",");
      if (colsStr.equals("all")) {
	cols = tokens.length - 2;
      }
      else {
	cols = Integer.parseInt(colsStr);  
      }
		  
      String HDIRank = tokens[0]; 
      String country = tokens[1];
		  
      if (country.charAt(0) == '\"') {
        country += "," + tokens[2];
	if (colsStr.equals("all")) {
	  cols--;
	}
      }
		 
      for (int i = tokens.length - 1; i >= tokens.length - cols; i--) {
	String nextVal = tokens[i];
	if (nextVal.equals("")) {
	  cols++;
	}
	else if (!nextVal.equals("..")) {
	  context.write(new Text(country), new Text(nextVal));
	}
      } 
    }
  }
}
