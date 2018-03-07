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
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class FindFilteredIndicatorMaxMapper extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    if (!line.isEmpty()) {
      String[] tokens = line.split(";");
		  
      double expectedSchoolFemale = 0;
      double expectedSchoolMale = 0;
      double meanSchoolFemale = 0;
      double meanSchoolMale = 0;
      double unemploymentFemale = 0;
      double unemploymentMale = 0;
		  
      for (int i = 1; i < tokens.length; i += 2) {
	switch(tokens[i]) {
	  case "ExpectedYearsSchoolingFemale":
	    expectedSchoolFemale = Double.valueOf(tokens[i + 1]);
	    break; 
	  case "ExpectedYearsSchoolingMale":
	    expectedSchoolMale = Double.valueOf(tokens[i + 1]);
	    break;
	  case "MeanYearsSchoolingFemale":
	    meanSchoolFemale = Double.valueOf(tokens[i + 1]);
	    break;
	  case "MeanYearsSchoolingMale":
	    meanSchoolMale = Double.valueOf(tokens[i + 1]);
	    break;
	  case "UnemploymentRateFemale":
	    unemploymentFemale = Double.valueOf(tokens[i + 1]);
	    break;
	  case "UnemploymentRateMale":
	    unemploymentMale = Double.valueOf(tokens[i + 1]);
	    break;
	  default:
	    context.write(new Text(tokens[i]), new Text(tokens[i + 1]));
	}
      }
		  
      double expectedSchoolDifference = Math.abs(expectedSchoolFemale - expectedSchoolMale);
      double meanSchoolDifference = Math.abs(meanSchoolFemale - meanSchoolMale);
      double unemploymentDifference = Math.abs(unemploymentFemale - unemploymentMale);
		
      context.write(new Text("maxExpectedSchoolingDifference"), new Text(String.valueOf(expectedSchoolDifference)));
      context.write(new Text("maxMeanSchoolingDifference"), new Text(String.valueOf(meanSchoolDifference)));
      context.write(new Text("maxUnemploymentRateDifference"), new Text(String.valueOf(unemploymentDifference)));
    }
  }
}
