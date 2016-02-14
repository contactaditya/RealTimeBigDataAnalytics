/*
 * Aditya Gupta, Shikuan Huang, Xiangbo Liang
 * Professor Suzanne McIntosh
 * Realtime and Big Data Analytics
 * 5, May 2015
 * 
 * MapReduce Job to get the comparison values used to compute the weight assignments
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FindFilteredIndicatorMaxReducer
    extends Reducer<Text, Text, Text, Text> {
	@Override
	  public void reduce(Text key, Iterable<Text> values, Context context)
	      throws IOException, InterruptedException { 
		double max = 0;
		for (Text value : values) {
		  double current = Double.parseDouble(value.toString());
		  if (current > max) {
		    max = current;
		  }
		}
		
		context.write(key, new Text(String.valueOf(max)));
	}
}
