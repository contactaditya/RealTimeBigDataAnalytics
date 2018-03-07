/*
 * Aditya Gupta, Shikuan Huang, Xiangbo Liang
 * Professor Suzanne McIntosh
 * Realtime and Big Data Analytics
 * 5, May 2015
 * 
 * MapReduce Job to get the list of common countries for the UNDP and WB data
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class GetCommonCountriesMapper extends Mapper<LongWritable, Text, Text, Text> {
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    if (!line.isEmpty()) {
      String country = "";
      StringTokenizer tokens = new StringTokenizer(line);
      int numOfTokens = tokens.countTokens();
      for (int i = 0; i < numOfTokens - 1; i++) {
        String nextVal = tokens.nextToken();
	country += nextVal;
	if (i != numOfTokens - 2) {
	  country += " ";
        }
      }
	  
      context.write(new Text(country), new Text(country));
    }
  }
}
