/*
 * Aditya Gupta, Shikuan Huang, Xiangbo Liang
 * Professor Suzanne McIntosh
 * Realtime and Big Data Analytics
 * 5, May 2015
 * 
 * MapReduce Job to get the list of common countries for the UNDP and WB data
 */

import java.nio.file.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GetCommonCountriesJob {
  public static void main(String[] args) throws Exception {
	if (args.length < 2) {
	  System.err.println("Usage: GetCommonCountries <WB GDP Per Capita input path> <output path>");
	}

	Job job = new Job();
	job.setJarByClass(ExtractUNDPSocialCSVJob.class);
	job.setJobName("GetCommonCountriesJob");
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	job.setMapperClass(GetCommonCountriesMapper.class);
	job.setReducerClass(GetCommonCountriesReducer.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
