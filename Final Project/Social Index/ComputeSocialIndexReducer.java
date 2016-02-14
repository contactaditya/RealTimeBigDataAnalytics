/*
 * Aditya Gupta, Shikuan Huang, Xiangbo Liang
 * Professor Suzanne McIntosh
 * Realtime and Big Data Analytics
 * 5, May 2015
 * 
 * MapReduce Job to compute the social index
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

public class ComputeSocialIndexReducer 
	extends Reducer<Text, Text, Text, Text> {
	@Override
	  public void reduce(Text key, Iterable<Text> values, Context context)
	      throws IOException, InterruptedException { 
		double socialIndex = 0;
		for (Text value : values) {
		  double currentIndicatorValue = Double.parseDouble(value.toString());
		  socialIndex += currentIndicatorValue;
		}
		
		context.write(key, new Text(String.valueOf(socialIndex)));
	}
}
