/*
 * Aditya Gupta, Shikuan Huang, Xiangbo Liang
 * Professor Suzanne McIntosh
 * Realtime and Big Data Analytics
 * 5, May 2015
 * 
 * MapReduce Job to order countries by social index
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

 public class OrderBySocialIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
   @Override
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
     String line = value.toString();
     if (!line.isEmpty()) {
       String[] tokens = line.split(";");
       double invertedSocialIndex = 1.0 / (Double.valueOf(tokens[1]));
       context.write(new Text(String.valueOf(invertedSocialIndex)), new Text(tokens[0]));
     }
   }
 }
