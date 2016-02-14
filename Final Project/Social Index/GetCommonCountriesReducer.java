/*
 * Aditya Gupta, Shikuan Huang, Xiangbo Liang
 * Professor Suzanne McIntosh
 * Realtime and Big Data Analytics
 * 5, May 2015
 * 
 * MapReduce Job to get the list of common countries for the UNDP and WB data
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GetCommonCountriesReducer
    extends Reducer<Text, Text, Text, Text> {
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException { 
	 
	HashMap<String, String> HDIMap = new HashMap<String, String>();
	  
	Path path = new Path("HDIExtracted.txt");
	FileSystem fs = FileSystem.get(new Configuration());
	BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
	String line = br.readLine();
	while (line != null) {      
      String[] tokens = line.split(";");
      String country = tokens[1];
      
      // Synchronize country names
	  switch(country) {
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
	  
	  HDIMap.put(country, country);
	  line = br.readLine();
	}
	
	String findHDICountry = HDIMap.get(key.toString());
	if (findHDICountry != null) {
	  context.write(key, new Text(""));
	}
	
  }
}
