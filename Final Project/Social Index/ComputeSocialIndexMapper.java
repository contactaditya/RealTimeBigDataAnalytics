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
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

 public class ComputeSocialIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
		  
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      HashMap<String, Double> maxValues = new HashMap<String, Double>();
		
      Path path = new Path("FilteredIndicatorMaxValues/part-r-00000");
      FileSystem fs = FileSystem.get(new Configuration());
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
      String line = br.readLine();
      while (line != null) {      
	if (line.isEmpty()) {
	  break;
	}
		  
	String[] tokens = line.split(";");
	maxValues.put(tokens[0], Double.valueOf(tokens[1]));
	line = br.readLine();
      }
		
      double educationScore = 0;
      double unemploymentScore = 0;
		 
      double expectedSchoolingFemale = 0;
      double expectedSchoolingMale = 0;
      double meanSchoolingFemale = 0;
      double meanSchoolingMale = 0;
      double unemploymentFemale = 0;
      double unemploymentMale = 0;
		
      line = value.toString();
      if (!line.isEmpty()) {
	String[] tokens = line.split(";");
	String country = tokens[0];
	for (int i = 1; i < tokens.length; i += 2) {
	  String indicatorName = tokens[i];
	  double indicatorValue = Double.parseDouble(tokens[i + 1]);
			
	  double score = 0;
	  switch (indicatorName) {
	     case "EducationIndex": 
	           educationScore = 25 * (indicatorValue / maxValues.get("EducationIndex"));
	           context.write(new Text(country), new Text(String.valueOf(educationScore)));
	           break;
	     case "ExpenditureOnHealth": 
		   score = 35 * .15 * (indicatorValue / maxValues.get("ExpenditureOnHealth"));
		   context.write(new Text(country), new Text(String.valueOf(score)));
		   break;
             case "GenderInequalityIndex": 
		   score = -10 * (indicatorValue / maxValues.get("GenderInequalityIndex"));
		   context.write(new Text(country), new Text(String.valueOf(score)));
		   break;
	     case "HomicideRate":
		   score = -20 * .65 * (indicatorValue / maxValues.get("HomicideRate"));
		   context.write(new Text(country), new Text(String.valueOf(score)));
		   break;
             case "LifeExpectancy":
		   score = 35 * .6 * (indicatorValue / maxValues.get("LifeExpectancy"));
		   context.write(new Text(country), new Text(String.valueOf(score)));
		   break;
             case "PopulationUrban":
		   score = 25 * .1 * (indicatorValue / 100);
		   context.write(new Text(country), new Text(String.valueOf(score)));
		   break;
	     case "RankingHDI":
		   score = 5 * (indicatorValue / maxValues.get("RankingHDI"));
		   context.write(new Text(country), new Text(String.valueOf(score)));
		   break;
	     case "PercentPopElectricityAccess":
		   score = 25 * .20 * (indicatorValue / 100);
		   context.write(new Text(country), new Text(String.valueOf(score)));
		   break;
	     case "RefugeePopByCountryOrigin":
		   score = -20 * .35 * (indicatorValue / maxValues.get("RefugeePopByCountryOrigin"));
		   context.write(new Text(country), new Text(String.valueOf(score)));
		   break;
	     case "UnemploymentRateTotal":
		   unemploymentScore = -20 * (indicatorValue / maxValues.get("UnemploymentRateTotal"));
		   context.write(new Text(country), new Text(String.valueOf(unemploymentScore)));
		   break;
	     case "PercentPopImprovedSanitationAccess":
		   score = 25 * .25 * (indicatorValue / 100);
		   context.write(new Text(country), new Text(String.valueOf(score)));
	           break;
	     case "PercentPopImprovedWaterAccess":
		   score = 25 * .25 * (indicatorValue / 100);
		   context.write(new Text(country), new Text(String.valueOf(score)));
		   break;
	     case "InternetUsersPer100":
		   score = 25 * .10 * (indicatorValue / maxValues.get("InternetUsersPer100"));
		   context.write(new Text(country), new Text(String.valueOf(score)));
		   break;
	     case "MaternalMortalityRatioPer100000":
		   score = 35 * .1 * (1 - (indicatorValue / maxValues.get("MaternalMortalityRatioPer100000")));
		   context.write(new Text(country), new Text(String.valueOf(score)));
		   break;
	     case "MortalityRateUnder5Per1000Births":
		   score = 35 * .15 * (1 - (indicatorValue / maxValues.get("MortalityRateUnder5Per1000Births")));
		   context.write(new Text(country), new Text(String.valueOf(score)));
	           break;
	     case "MobileCellularSubPer100000":
		   score = 25 * .1 * (indicatorValue / maxValues.get("MobileCellularSubPer100000"));
		   context.write(new Text(country), new Text(String.valueOf(score)));
		   break;
	     case "IncomeIndex":
		   score = 15 * (indicatorValue / maxValues.get("IncomeIndex"));
		   context.write(new Text(country), new Text(String.valueOf(score)));
		   break;
	     case "ExpectedYearsSchoolingFemale":
		   expectedSchoolingFemale = indicatorValue;
		   break;
	     case "ExpectedYearsSchoolingMale":
		   expectedSchoolingMale = indicatorValue;
		   break;
	     case "MeanYearsSchoolingFemale":
		   meanSchoolingFemale = indicatorValue;
		   break;
	     case "MeanYearsSchoolingMale":
		   meanSchoolingMale = indicatorValue;
		   break;
	     case "UnemploymentRateFemale":
		   unemploymentFemale = indicatorValue;
		   break;
	     case "UnemploymentRateMale":
		   unemploymentMale = indicatorValue;
		   break;
	  }
      }
		  
      double expectedSchoolingDifference = Math.abs(expectedSchoolingFemale - expectedSchoolingMale);
      double meanSchoolingDifference = Math.abs(meanSchoolingFemale - meanSchoolingMale);
      double unemploymentRateDifference = Math.abs(unemploymentFemale - unemploymentMale); 
		  
      double expectedSchoolModifier = -(educationScore * .05) * (expectedSchoolingDifference / maxValues.get("maxExpectedSchoolingDifference"));
      double meanSchoolModifier = -(educationScore * .05) * (meanSchoolingDifference / maxValues.get("maxMeanSchoolingDifference"));
      double unemploymentModifier = -(unemploymentScore * .05) * (unemploymentRateDifference / maxValues.get("maxUnemploymentRateDifference"));
		  
      context.write(new Text(country), new Text(String.valueOf(expectedSchoolModifier)));
      context.write(new Text(country), new Text(String.valueOf(meanSchoolModifier)));
      context.write(new Text(country), new Text(String.valueOf(unemploymentModifier)));
    }
  }
}
