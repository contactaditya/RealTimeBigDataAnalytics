/*
 * Aditya Gupta, Shikuan Huang, Xiangbo Liang
 * Professor Suzanne McIntosh
 * Realtime and Big Data Analytics
 * 5, May 2015
 * 
 * MapReduce Job to combine the social indicators for each country
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.*;     

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AggregateSocialDataMapper extends Mapper<LongWritable, Text, Text, Text> 
{	          
  @Override
  public void map(LongWritable key, Text value, Context context) 
      throws IOException, InterruptedException {
  	HashMap<String, String> commonCountries = new HashMap<String, String>(); 
    String country = "";
       
    Path path = new Path("CommonCountries/part-r-00000");
    FileSystem fs = FileSystem.get(new Configuration());
    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
    String line = br.readLine();
    while (line != null) {
      if (line.isEmpty()) {
        break;
      }
      
      country = "";
      StringTokenizer lineTokens = new StringTokenizer(line);
      int numOfTokens = lineTokens.countTokens();
      for (int k = 0; k < numOfTokens; k++) {
        String nextVal = lineTokens.nextToken();
        country += nextVal;
        if (k != numOfTokens - 1) {
	      country += " ";
        }
      }
      
      commonCountries.put(country, country);
      line = br.readLine();
    }
       
    String fileNameFromInput = value.toString();
    if (!fileNameFromInput.isEmpty()) {
    	String prefix = fileNameFromInput.substring(0, 2);
        
        // Read UNDP data
        if (prefix.equals("UN"))	        	   
        {
    	  if (!fileNameFromInput.equals("UNRankingHDI.txt"))
    	  {
    	    Path UNDPFile = new Path(fileNameFromInput);
    	    FileSystem fs1 = FileSystem.get(new Configuration());
    	    BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(UNDPFile)));
    	    
    	    String UNDPLineFromInput = br1.readLine(); 
    	    while (UNDPLineFromInput != null) {
    		  country = "";
    	   
    		  StringTokenizer lineTokens = new StringTokenizer(UNDPLineFromInput);
    		  int numOfTokens = lineTokens.countTokens();
    		  for (int k = 0; k < numOfTokens - 3; k++) {
    		    String nextVal = lineTokens.nextToken();
    		    country += nextVal;
    		    if (k != numOfTokens - 4) {
    			  country += " ";
    		    }
    		  }
        		   
     		  // Synchronize country names
    	      switch (country) {
    	          case "Bahamas":
    	            country = "Bahamas, The";
    	            break;
    	          case "Congo (Democratic Republic of the)":
    	            country = "Congo, Dem. Rep.";
    	            break;
    	          case "Moldova (Republic of)":
    	            country = "Moldova";
    	            break;
    	          case "Congo":
    	            country = "Congo, Rep.";
    	            break;
    	          case "Tanzania (United Republic of)":
    	            country = "Tanzania";
    	            break;
    	          case "Venezuela (Bolivarian Republic of)":
    	            country = "Venezuela, RB";
    	            break;
    	          case "Yemen":
    	            country = "Yemen, Rep.";
    	            break;
    	          case "Viet Nam":
    	            country = "Vietnam";
    	            break;
    	          case "Côte d'Ivoire":
    	            country = "Cote d'Ivoire";
    	            break;
    	          case "C�te d'Ivoire":
    	            country = "Cote d'Ivoire";
    	            break;
    	          case "Iran (Islamic Republic of)":
    	            country = "Iran, Islamic Rep.";
    	            break;
    	          case "Bolivia (Plurinational State of)":
    	            country = "Bolivia";
    	            break;
    	          case "Lao People's Democratic Republic":
    	            country = "Lao PDR";
    	            break;
    	          case "The former Yugoslav Republic of Macedonia":
    	            country = "Macedonia, FYR";
    	            break;
    	          case "Egypt":
    	            country = "Egypt, Arab Rep.";
    	            break;
    	          case "Cape Verde":
    	            country = "Cabo Verde";
    	            break;
    	          case "Gambia":
    	            country = "Gambia, The";
    	            break;
    	          case "Slovakia":
    	            country = "Slovak Republic";
    	            break;
    	          case "Kyrgyzstan":
    	            country = "Kyrgyz Republic";
    	            break;
    	          case "Korea (Republic of)":
    	            country = "Korea, Rep.";
    	            break;
    	          case "Hong Kong, China (SAR)":
    	            country = "Hong Kong SAR, China";
    	            break;
              }	 
        	      
       		  if (commonCountries.get(country) != null) {
       			String min = lineTokens.nextToken();  
     			String avg = lineTokens.nextToken();
     			String max = lineTokens.nextToken();
     			String indicatorString = fileNameFromInput.substring(2, fileNameFromInput.length() - 4) + ";" + avg;
     		    context.write(new Text(country), new Text(indicatorString));		 
     		  }
       		  
       		  UNDPLineFromInput = br1.readLine();
            }
          }
    	  // Read HDI data
    	  else
    	  {
    	    Path path1 = new Path(fileNameFromInput);
    	    FileSystem fs1 = FileSystem.get(new Configuration());
    	    BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(path1)));
    	    String HDILineFromInput = br1.readLine(); 
    	    while (HDILineFromInput != null) {
    		  country = "";
    	   
    		  String[] tokens = HDILineFromInput.split(";");
    		  country = tokens[1];
    		  switch (country) {
    	          case "Bahamas":
    	            country = "Bahamas, The";
    	            break;
    	          case "Congo (Democratic Republic of the)":
    	            country = "Congo, Dem. Rep.";
    	            break;
    	          case "Moldova (Republic of)":
    	            country = "Moldova";
    	            break;
    	          case "Congo":
    	            country = "Congo, Rep.";
    	            break;
    	          case "Tanzania (United Republic of)":
    	            country = "Tanzania";
    	            break;
    	          case "Venezuela (Bolivarian Republic of)":
    	            country = "Venezuela, RB";
    	            break;
    	          case "Yemen":
    	            country = "Yemen, Rep.";
    	            break;
    	          case "Viet Nam":
    	            country = "Vietnam";
    	            break;
    	          case "Côte d'Ivoire":
    	            country = "Cote d'Ivoire";
    	            break;
    	          case "C�te d'Ivoire":
    	            country = "Cote d'Ivoire";
    	            break;
    	          case "Iran (Islamic Republic of)":
    	            country = "Iran, Islamic Rep.";
    	            break;
    	          case "Bolivia (Plurinational State of)":
    	            country = "Bolivia";
    	            break;
    	          case "Lao People's Democratic Republic":
    	            country = "Lao PDR";
    	            break;
    	          case "The former Yugoslav Republic of Macedonia":
    	            country = "Macedonia, FYR";
    	            break;
    	          case "Egypt":
    	            country = "Egypt, Arab Rep.";
    	            break;
    	          case "Cape Verde":
    	            country = "Cabo Verde";
    	            break;
    	          case "Gambia":
    	            country = "Gambia, The";
    	            break;
    	          case "Slovakia":
    	            country = "Slovak Republic";
    	            break;
    	          case "Kyrgyzstan":
    	            country = "Kyrgyz Republic";
    	            break;
    	          case "Korea (Republic of)":
    	            country = "Korea, Rep.";
    	            break;
    	          case "Hong Kong, China (SAR)":
    	            country = "Hong Kong SAR, China";
    	            break;
              }	 
    	        			  
       		  if (commonCountries.get(country) != null) {
     			String avg = tokens[2];
     			String indicatorString = fileNameFromInput.substring(2, fileNameFromInput.length() - 4) + ";" +avg;
     		    context.write(new Text(country),new Text(indicatorString));		 
     		  }
    	        	
       		  HDILineFromInput = br1.readLine();
    	    }
    	        	   
          }
        }	      
        // Read World Bank data
        else
        {
    	  Path path1 = new Path(fileNameFromInput);
    	  FileSystem fs1 = FileSystem.get(new Configuration());
          BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(path1)));
          String WBLineFromInput = br1.readLine(); 
           
          while (WBLineFromInput!=null) {
        	country = "";
    	    StringTokenizer lineTokens = new StringTokenizer(WBLineFromInput);
    	    int numOfTokens = lineTokens.countTokens();
    	    for (int k = 0; k < numOfTokens - 1; k++) {
    		  String nextVal = lineTokens.nextToken();
    		  country += nextVal;
    		  if (k != numOfTokens -2) {
    		    country += " ";
    		  }
    		}
    		if (commonCountries.get(country) != null) {
    		  String avg = lineTokens.nextToken();
    		  String indicatorString = (fileNameFromInput.substring(2, fileNameFromInput.length() - 4) + ";" + avg);
    		  context.write(new Text(country), new Text(indicatorString));
    	    }
    		  		  
    		WBLineFromInput = br1.readLine();
          }       	  	        	   
        } 
    }
    		  	     		     
  }
}


	 	  
	 
