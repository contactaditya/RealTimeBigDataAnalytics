/*
 * Aditya Gupta, Shikuan Huang, Xiangbo Liang
 * Professor Suzanne McIntosh
 * Realtime and Big Data Analytics
 * 5, May 2015
 * 
 * MapReduce Job to get the comparison values used to compute the weight assignments
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FindFilteredIndicatorMaxJob {
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
		  System.err.println("Usage: FindFilteredIndicatorMaxJob <input path> <output path>");
		}
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ";");
		
		Job job = new Job(conf);
		job.setJarByClass(FindFilteredIndicatorMaxJob.class);
		job.setJobName("FindFilteredIndicatorMaxJob");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FindFilteredIndicatorMaxMapper.class);
		job.setReducerClass(FindFilteredIndicatorMaxReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
