/*
 * Aditya Gupta, Shikuan Huang, Xiangbo Liang
 * Professor Suzanne McIntosh
 * Realtime and Big Data Analytics
 * 5, May 2015
 * 
 * MapReduce Job to combine the social indicators for each country
 */

import java.io.IOException;
import java.util.*;     
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

 public class AggregateSocialDataJob {
    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("mapreduce.output.textoutputformat.separator", ";");
		        
      Job job = new Job(conf);
      job.setJarByClass(AggregateSocialDataJob.class);
      job.setJobName("AggregateSocialDataJob");
  	   	
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
           
      job.setMapperClass(AggregateSocialDataMapper.class);
      job.setReducerClass(AggregateSocialDataReducer.class);
           
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
           
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
 }
