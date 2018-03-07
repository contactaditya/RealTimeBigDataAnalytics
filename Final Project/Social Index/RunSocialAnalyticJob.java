/*
 * Aditya Gupta, Shikuan Huang, Xiangbo Liang
 * Professor Suzanne McIntosh
 * Realtime and Big Data Analytics
 * 5, May 2015
 * 
 * MapReduce Job to run the other social MR jobs
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunSocialAnalyticJob {
  public static void main(String[] args) throws Exception {
    // Extract countries common to both World Bank and UNDP data
    {
      Job job = new Job();
      job.setJarByClass(ExtractUNDPSocialCSVJob.class);
      job.setJobName("GetCommonCountriesJob");
		
      FileInputFormat.addInputPath(job, new Path(new String("GDPPerCapita.txt")));
      FileOutputFormat.setOutputPath(job, new Path(new String("CommonCountries")));
		
      job.setMapperClass(GetCommonCountriesMapper.class);
      job.setReducerClass(GetCommonCountriesReducer.class);
		
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      job.waitForCompletion(true);
    }
	
    // Combine all social indicators for each country
    {
      Configuration conf = new Configuration();
      conf.set("mapreduce.output.textoutputformat.separator", ";");
		        
      Job job = new Job(conf);
      job.setJarByClass(AggregateSocialDataJob.class);
      job.setJobName("AggregateSocialDataJob");
  	   	
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
           
      job.setMapperClass(AggregateSocialDataMapper.class);
      job.setReducerClass(AggregateSocialDataReducer.class);
           
      FileInputFormat.addInputPath(job, new Path(new String("ProcessedDataFileNames.txt")));
      FileOutputFormat.setOutputPath(job, new Path(new String("AggregateSocialData")));
           
      job.waitForCompletion(true);
    }
	   
    // Filter countries and indices
    {
      Configuration conf = new Configuration();
      conf.set("mapreduce.output.textoutputformat.separator", ";");

      Job job = new Job(conf);
      job.setJarByClass(FilterCountriesAndIndicesJob.class);
      job.setJobName("FilterCountriesAndIndicesJob");
		
      FileInputFormat.addInputPath(job, new Path(new String("AggregateSocialData")));
      FileOutputFormat.setOutputPath(job, new Path(new String("FilteredCountriesAndIndices")));
		
      job.setMapperClass(FilterCountriesAndIndicesMapper.class);
      job.setReducerClass(FilterCountriesAndIndicesReducer.class);
		
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      job.waitForCompletion(true);
    }
	  
    // Find the indicator values to base weight assignments on
    {
      Configuration conf = new Configuration();
      conf.set("mapreduce.output.textoutputformat.separator", ";");
		
      Job job = new Job(conf);
      job.setJarByClass(FindFilteredIndicatorMaxJob.class);
      job.setJobName("FindFilteredIndicatorMaxJob");
		
      FileInputFormat.addInputPath(job, new Path(new String("FilteredCountriesAndIndices")));
      FileOutputFormat.setOutputPath(job, new Path(new String("FilteredIndicatorMaxValues")));
		
      job.setMapperClass(FindFilteredIndicatorMaxMapper.class);
      job.setReducerClass(FindFilteredIndicatorMaxReducer.class);
		
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      job.waitForCompletion(true);
    }
	  
    // Compute the social index 
    {
      Configuration conf = new Configuration();
      conf.set("mapreduce.output.textoutputformat.separator", ";");

      Job job = new Job(conf);
      job.setJarByClass(ExtractUNDPSocialCSVJob.class);
      job.setJobName("ComputeSocialIndexJob");
		
      FileInputFormat.addInputPath(job, new Path(new String("FilteredCountriesAndIndices")));
      FileOutputFormat.setOutputPath(job, new Path(new String("SocialIndex")));
		
      job.setMapperClass(ComputeSocialIndexMapper.class);
      job.setReducerClass(ComputeSocialIndexReducer.class);
		
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      job.waitForCompletion(true);
    }
	  
    // Order by social index
    {
      Configuration conf = new Configuration();
      conf.set("mapreduce.output.textoutputformat.separator", ";");

      Job job = new Job(conf);
      job.setJarByClass(OrderBySocialIndexJob.class);
      job.setJobName("OrderBySocialIndexJob");
		
      FileInputFormat.addInputPath(job, new Path(new String("SocialIndex")));
      FileOutputFormat.setOutputPath(job, new Path(new String("OrderedSocialIndex")));
		
      job.setMapperClass(OrderBySocialIndexMapper.class);
      job.setReducerClass(OrderBySocialIndexReducer.class);
		
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  }
}
