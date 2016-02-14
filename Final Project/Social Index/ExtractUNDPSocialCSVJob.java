/*
 * Aditya Gupta, Shikuan Huang, Xiangbo Liang
 * Professor Suzanne McIntosh
 * Realtime and Big Data Analytics
 * 16, April 2015
 * 
 * MapReduce Job For UNDP Social Indicators Data
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ExtractUNDPSocialCSVJob {
  public static void main(String[] args) throws Exception {
	if (args.length < 2) {
	  System.err.println("Usage: ExtractSocialJob <input path> <output path> [number of cols to read from the end]");
	  System.exit(1);
	}
	
	org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
	
	if (args.length > 2) {
		conf.set("cols", args[2]);
	}
	else {
		conf.set("cols", "all");
	}
	
	Job job = new Job(conf);
	job.setJarByClass(ExtractUNDPSocialCSVJob.class);
	job.setJobName("ExtractSocialJob");
	
	FileInputFormat.addInputPath(job,  new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	job.setMapperClass(ExtractUNDPSocialCSVMapper.class);
	job.setReducerClass(ExtractUNDPSocialCSVReducer.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
