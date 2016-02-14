/*
 * Aditya Gupta, Shikuan Huang, Xiangbo Liang
 * Professor Suzanne McIntosh
 * Realtime and Big Data Analytics
 * 5, May 2015
 * 
 * MapReduce Job to filter countries and indices
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class FilterCountriesAndIndicesMapper
	extends Mapper<LongWritable, Text, Text, Text> {
		  
	  @Override
	  public void map(LongWritable key, Text value, Context context) 
	      throws IOException, InterruptedException {
		  
		  // Read whole country line and tokenize
		  // Store all indicator names in hash map
		  // Check if all indicators exist
		  // If so output whole country line as key with empty value
		  
		  List<String> indicatorCheckList = new ArrayList<String>();
		  indicatorCheckList.add("EducationIndex");
		  indicatorCheckList.add("ExpectedYearsSchoolingFemale");
		  indicatorCheckList.add("ExpectedYearsSchoolingMale");
		  indicatorCheckList.add("ExpenditureOnHealth");
		  indicatorCheckList.add("GenderInequalityIndex");
		  indicatorCheckList.add("HomicideRate");
		  indicatorCheckList.add("LifeExpectancy");
		  indicatorCheckList.add("MeanYearsSchoolingFemale");
		  indicatorCheckList.add("MeanYearsSchoolingMale");
		  indicatorCheckList.add("PopulationUrban");
		  indicatorCheckList.add("RankingHDI");
		  indicatorCheckList.add("IncomeIndex");
		  indicatorCheckList.add("PercentPopElectricityAccess");  
		  indicatorCheckList.add("RefugeePopByCountryOrigin");
		  indicatorCheckList.add("UnemploymentRateFemale");
		  indicatorCheckList.add("UnemploymentRateMale");
		  indicatorCheckList.add("UnemploymentRateTotal");
		  indicatorCheckList.add("PercentPopImprovedSanitationAccess");
		  indicatorCheckList.add("PercentPopImprovedWaterAccess");
		  indicatorCheckList.add("InternetUsersPer100");
		  indicatorCheckList.add("MaternalMortalityRatioPer100000");
		  indicatorCheckList.add("MortalityRateUnder5Per1000Births");
		  indicatorCheckList.add("MobileCellularSubPer100"); 
		 
		HashMap<String, String> indicatorList = new HashMap<String, String>();
		String countryLine = value.toString();
		if (!countryLine.isEmpty()) {
		  String[] tokens = countryLine.split(";");
		  for (int i = 1; i < tokens.length; i += 2) {
			indicatorList.put(tokens[i], tokens[i + 1]);
		  }
		  
		  boolean hasAllRequiredIndicators = true;
		  for (int k = 0; k < indicatorCheckList.size(); k++) {
			if (indicatorList.get(indicatorCheckList.get(k)) == null) {
			  hasAllRequiredIndicators = false;
			}
		  }
		  
		  if (hasAllRequiredIndicators) {
			context.write(value, new Text(""));
		  }
		}
		
	  }
}
