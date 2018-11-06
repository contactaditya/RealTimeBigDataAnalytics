import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EDICalculator {
   public static void main(String[] args) throws Exception {
     if(args.length < 2) {
       System.err.println("Usage: <directory containing your input files> <configueration file>");
       System.exit(-1);
     }
     Path configFile = new Path(args[1]);
     String rootInput = args[0];
     FileSystem fs = FileSystem.get(new Configuration());
     BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(configFile)));
     String line = br.readLine();
     try {
	   int numOfIndicators = Integer.parseInt(line.split(":")[1]);
	   line = br.readLine();
	   StringBuilder normalizedIndicators = new StringBuilder();
	   while(line != null) {
	     String[] params = line.split(";");
	     String indicator = params[0];
	     String outputdir = "EDIoutput/indicators/" + indicator + "/";
	     if(args.length > 2 && args[2].equals("true") && fs.exists(new Path(outputdir + "normalized"))) {
	       fs.delete(new Path(outputdir + "normalized"), true);
	     }
	     String years = params[1].split(":")[1];
	     if(!fs.exists(new Path(outputdir+"average/part-r-00000"))) {
	       average(new String[]{rootInput+"/" + indicator + ".csv", outputdir + "average", years});
	     }
	     String normalizer = params[2].split(":")[0];
	     String[] confargs = params[2].split(":")[1].split(",");
	     String[] normalizerArgs = normalizer.equals("singleNormalizer") ? new String[confargs.length + 3] : new String[confargs.length + 2];
	     normalizerArgs[0] = outputdir + "average/part-r-00000";
	     normalizerArgs[1] = outputdir + "normalized";
	     if(normalizer.equals("singleNormalizer")) {
	       for(int i = 0; i < confargs.length; i++) {
	         normalizerArgs[i + 3] = confargs[i];
	       }
	       if(!fs.exists(new Path(outputdir + "minmax/part-r-00000"))) {
		 minmax(new String[]{outputdir + "average/part-r-00000", outputdir + "minmax"});
	       }
	       if(normalizerArgs[3].equals("avg")) {
		 if(!fs.exists(new Path(outputdir + "overallAverage/part-r-00000"))) {
		   totalAverage(new String[]{outputdir + "average/part-r-00000", outputdir + "overallAverage"});
		 }
		 BufferedReader avgbr = new BufferedReader(new InputStreamReader(fs.open(new Path(outputdir+"overallAverage/part-r-00000"))));
		 String avgstr = avgbr.readLine();
		 avgstr.trim();
		 normalizerArgs[3] = avgstr.substring(1);
	       }
	       normalizerArgs[2] = outputdir + "minmax/part-r-00000";
	       if(!fs.exists(new Path(outputdir + "normalized/part-r-00000"))) {
		 singleNormalizer(normalizerArgs);
	       }
	     }
	     else {
	       for(int i = 0; i < confargs.length; i++) {
		 normalizerArgs[i + 2] = confargs[i];
	       }
	       if(!fs.exists(new Path(outputdir + "normalized/part-r-00000"))) {
		 twoWayNormalizer(normalizerArgs);
	       }
	     }
	     line = br.readLine();
	     if(!fs.exists(new Path(outputdir + "ranked-normalized/part-r-00000"))) { 
	       Ranker(new String[]{outputdir + "normalized/part-r-00000", outputdir + "ranked-normalized"});
	     }
	     if(line != null) {
	       normalizedIndicators.append(outputdir + "normalized/part-r-00000,");
	     }
	     else {
	       normalizedIndicators.append(outputdir + "normalized/part-r-00000");
	     }
	   }
	   if(fs.exists(new Path("EDIoutput/EDI-unordered"))) {
	     fs.delete(new Path("EDIoutput/EDI-unordered"), true);
	   }
	   if(fs.exists(new Path("EDIoutput/EDI-ordered"))) {
	     fs.delete(new Path("EDIoutput/EDI-ordered"), true);
	   }
	   EDIassembler(new String[]{normalizedIndicators.toString(), "EDIoutput/EDI-unordered", String.valueOf(numOfIndicators)});
	   Ranker(new String[]{"EDIoutput/EDI-unordered/part-r-00000", "EDIoutput/EDI-ordered"});
	 }
	 catch(IOException e) {
	   System.err.println("Parsing configuration file throws exceptions:");
	   System.err.println(e.getMessage());
	   System.exit(-1);
	 }
	 //average(new String[]{"GDP.csv", "gdpoutput","10"});
         //minmax(new String[]{"gdpoutput", "gdpoutput/minmax"});	
       }
	
       private static void average(String[] args) throws Exception {
	 if(args.length < 2 || args.length > 4) {
	   System.err.println("Indicator did not configured properly for average calculation: " + args[1]);
	   System.err.println("Usage: String[] <input path> <output path> [years(int)]");
	   System.exit(-1);
	 }
	 Configuration conf = new Configuration();
         if(args.length < 3) {
	   conf.setInt("yr", 0);
	 }
	 else {
	   try {
	     conf.setInt("yr", Integer.parseInt(args[2]));
	   }
	   catch(Exception e) {
	     System.err.println("Indicator did not configured properly for average calculation: " + args[1]);
	     System.err.println("Usage: String[] <input path> <output path> [years(int)]");
	     System.exit(-1);
	   }
	 }
	 conf.set("mapred.textoutputformat.separator", ";");
	 Job job = new Job(conf);
	 // Path ignoreFilePath = new Path("inrelevants.txt");
	 // DistributedCache.addCacheFile(ignoreFilePath.toUri(), conf);
	 job.setJarByClass(EDICalculator.class);
	 job.setJobName("Calculate Average");
	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 job.setMapperClass(averageMapper.class);
         job.setReducerClass(averageReducer.class);
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(Text.class);
	 if(!job.waitForCompletion(true)) {
	   System.err.println("average calculation falied for file: " + args[1]);
	   System.exit(1);
	 }
       }
	
       private static void minmax(String[] args) throws Exception {
	 if(args.length < 2) {
	   System.err.println("Indicator did not configured properly for min max calculation: " + args[1]);
	   System.err.println("Usage: String[] <input path> <output path>");
	   System.exit(-1);
	 }
         Configuration conf = new Configuration();
         conf.set("mapred.textoutputformat.separator", ";");
         Job job = new Job(conf);
         job.setJarByClass(EDICalculator.class);
         job.setJobName("find min and max");
         FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 job.setMapperClass(minmaxMapper.class);
         job.setCombinerClass(minmaxReducer.class);
         job.setReducerClass(minmaxReducer.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(DoubleWritable.class);
	 if(!job.waitForCompletion(true)) {
	   System.err.println("min and mex calculation falied for file: " + args[1]);
	   System.exit(1);
	 }
       }
	
       private static void totalAverage(String[] args) throws Exception {
         if(args.length < 2) {
           System.err.println("Indicator did not configured properly for overall average calculation: " + args[1]);
           System.err.println("Usage: String[] <input path> <output path>");
           System.exit(-1);
         }
         Configuration conf = new Configuration();
         conf.set("mapred.textoutputformat.separator", ";");
         Job job = new Job(conf);
         job.setJarByClass(EDICalculator.class);
         job.setJobName("find total average");
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         job.setMapperClass(totalAverageMapper.class);
         job.setCombinerClass(totalAverageReducer.class);
         job.setReducerClass(totalAverageReducer.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(DoubleWritable.class);
         if(!job.waitForCompletion(true)) {
           System.err.println("overall average calculation falied for file: " + args[1]);
           System.exit(1);
         }
       }
	
       private static void singleNormalizer(String[] args) throws Exception {
         if(args.length < 7) {
           System.err.println("Indicator did not configured properly for single normalizer: " + args[1]);
           System.err.println("Usage: String[]: <input path> <output path> <minmaxpath> <neutral Point, valid value: min, max, or numerical value between min and max> <score at min> <score at max> <score at neutral>");
           System.exit(-1);
         }
         Configuration conf = new Configuration();
         String minmaxpath = args[2];
         Path maxminfile = new Path(minmaxpath);
         FileSystem fs = FileSystem.get(conf);
         BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(maxminfile)));
         double[] maxmin = new double[2];
         String line = br.readLine();
         maxmin[0] = Double.parseDouble(line.substring(1));
         line = br.readLine();
         maxmin[1] = Double.parseDouble(line.substring(1));
         conf.setDouble("min", maxmin[0]); 
         conf.setDouble("max", maxmin[1]);
         try {
           if(args[3].equals("min")) {
             conf.setDouble("neutralPoint", maxmin[0]);
           }
           else if(args[3].equals("max")) {
	     conf.setDouble("neutralPoint", maxmin[1]);
           }
           else {
	     conf.setDouble("neutralPoint", Double.parseDouble(args[3]));
           }
           conf.setDouble("scoreAtMin", Double.parseDouble(args[4]));
           conf.setDouble("scoreAtMax", Double.parseDouble(args[5]));
           conf.setDouble("scoreAtNeutral", Double.parseDouble(args[6]));
         }
         catch(Exception ex) {
           System.err.println(ex.getMessage());
           System.err.println("Indicator did not configured properly for single normalizer: " + args[1]);
           System.err.println("Usage: String[]: <input path> <output path> <minmaxpath> <neutral Point, valid value: min, max, or numerical value between min and max> <score at min> <score at max> <score at neutral>");
           System.exit(-1);
         }
         conf.set("mapred.textoutputformat.separator", ";");
         Job job = new Job(conf);
         job.setJarByClass(EDICalculator.class);
         job.setJobName("Single Normalizer");
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         job.setMapperClass(singleMapper.class);
         job.setReducerClass(singleReducer.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(DoubleWritable.class);
         if(!job.waitForCompletion(true)){
           System.err.println("singleNormalizer falied for file: " + args[1]);
           System.exit(1);
         }
       }
	
       private static void twoWayNormalizer(String[] args) throws Exception {
         if(args.length < 6) {
           System.err.println("Indicator did not configured properly for two way normalizer: " + args[1]);
           System.err.println("Usage: String[] <input path> <output path><left><right><optimal><weight>[leftBoundry][rightBoundry][scorelimit]");
           System.exit(-1);
         }
         Configuration conf = new Configuration();
         conf.setDouble("leftBase", Double.parseDouble(args[2]));
         conf.setDouble("rightBase", Double.parseDouble(args[3]));
         conf.setDouble("optimalPoint", Double.parseDouble(args[4]));
         conf.setDouble("weight", Double.parseDouble(args[5]));
         if(args.length > 6) {
           conf.setBoolean("hasBoundry", true);
           conf.setDouble("leftBoundry", Double.parseDouble(args[6]));
           conf.setDouble("rightBoundry", Double.parseDouble(args[7]));
           conf.setDouble("scorelimit", Double.parseDouble(args[8]));
         }
         conf.set("mapred.textoutputformat.separator", ";");
         Job job = new Job(conf);
         job.setJarByClass(EDICalculator.class);
         job.setJobName("Two Way Normalizing");
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         job.setMapperClass(twoWayMapper.class);
         job.setReducerClass(twoWayReducer.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(DoubleWritable.class);
         if(!job.waitForCompletion(true)) {
           System.err.println("Two way normalizer falied for file: " + args[1]);
           System.exit(1);
         }
       }
	
       private static void EDIassembler(String[] args) throws Exception {
         if(args.length < 3) {
           System.err.println("Indicator did not configured properly for EDI assembler: " + args[1]);
           System.err.println("String[] <paths for input file, seperated by comma> <output path><numIndicators>");
           System.exit(-1);
         }
         Configuration conf = new Configuration();
         conf.setDouble("numOfIndicators", Double.parseDouble(args[2]));
         conf.set("mapred.textoutputformat.separator", ";");
         Job job = new Job(conf);
         job.setJarByClass(EDICalculator.class);
         job.setJobName("EDI Assembling");
         FileInputFormat.addInputPaths(job, args[0]);
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         job.setMapperClass(EDIMapper.class);
         job.setReducerClass(EDIReducer.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(DoubleWritable.class);
         if(!job.waitForCompletion(true)) {
           System.err.println("EDI assembling falied for file: " + args[1]);
           System.exit(1);
         }
       }
	
       public static void Ranker(String[] args) throws Exception {	
         if(args.length < 2){
           System.err.println("Usage: String[] <input path> <output path>");
           System.exit(-1);
         }
         Configuration conf = new Configuration();
         conf.set("mapred.textoutputformat.separator", ";");
         Job job = new Job(conf);
         job.setJarByClass(EDICalculator.class);
         job.setJobName("Sort");
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         job.setMapperClass(rankerMapper.class);
         job.setReducerClass(rankerReducer.class);
         job.setOutputKeyClass(DoubleWritable.class);
         job.setOutputValueClass(Text.class);
         if(!job.waitForCompletion(true)) {
           System.err.println("Ranker falied");
           System.exit(1);
         }
       }
     }
