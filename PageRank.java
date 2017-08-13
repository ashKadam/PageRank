// 
// Name: Ashwini Kadam
// email: akadam3@uncc.edu
// Student ID: 800967986
// Assignment 3: PageRank Algortihm Implementation
//
package org.myorg;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class PageRank extends Configured implements Tool {
   private static final String DOC_COUNT = "doc_count";		// variable to store number of files in corpus
   private static final String IFILE_PATH = "interFile";	// intermediate files path to store map-reduce results during iterations
   private static final Logger LOG = Logger.getLogger(PageRank.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner.run( new PageRank(), args);
      System.exit(res);
   }

   public int run(String[] args) throws  Exception {
	FileSystem fs = FileSystem.get(getConf());

	// This job counts the number of files(N) present in corpus and store value in varialbe 'docCount'
      	Job jobCount  = Job.getInstance(getConf(), "PageRank");
      	jobCount.setJarByClass(this .getClass()); 
      	FileInputFormat.addInputPaths(jobCount, args[0]);
      	FileOutputFormat.setOutputPath(jobCount, new Path("interFile"));
      	jobCount.setMapperClass(MapCount.class);
      	jobCount.setReducerClass(ReduceCount.class);
      	jobCount.setOutputKeyClass(Text.class);
      	jobCount.setOutputValueClass(IntWritable.class);
	jobCount.waitForCompletion(true);
	fs.delete(new Path("interFile"), true);	// Delete intermediate count file	
	
     	long docCount = jobCount.getCounters().findCounter("docCount", "docCount").getValue();

	// This job initializes the page rank for each page and store results in output file 'interFile_itr0' 
	// It uses document count we calculated in earlier job 'jobCount'. 
	Job jobInitRank  = Job.getInstance(getConf(), "PageRank");
	jobInitRank.setJarByClass(this.getClass());
	FileInputFormat.addInputPaths(jobInitRank, args[0]);
	FileOutputFormat.setOutputPath(jobInitRank, new Path(IFILE_PATH + "_itr0"));
	jobInitRank.getConfiguration().setStrings(DOC_COUNT, docCount + "");
	jobInitRank.setMapperClass(MapInitRank.class);
	jobInitRank.setReducerClass(ReduceInitRank.class);
	jobInitRank.setOutputKeyClass(Text.class);
	jobInitRank.setOutputValueClass(Text.class);
	jobInitRank.waitForCompletion(true);

	// We have loop for 10 terations here which will calculate new page rank for each page in each iteration
	// Here, each job will calculate page rank from previous rank and store output in itermediate file.
	// This intermediate file will act as input for next iteration.    
	for(int i = 0; i < 10; i++){
		Job jobPageRank  = Job.getInstance(getConf(), "PageRank");
		jobPageRank.setJarByClass( this.getClass());
		FileInputFormat.addInputPaths(jobPageRank,  IFILE_PATH +"_itr"+i);
		FileOutputFormat.setOutputPath(jobPageRank, new Path(IFILE_PATH +"_itr"+(i+1)));    // Output file acting as input for next iteration
		jobPageRank.setMapperClass(MapPageRank.class);
		jobPageRank.setReducerClass(ReducePageRank.class);
		jobPageRank.setOutputKeyClass(Text.class);
		jobPageRank.setOutputValueClass(Text.class);
		jobPageRank.waitForCompletion(true);
		fs.delete(new Path(IFILE_PATH +"_itr"+i), true); 	// delete current intermediate input file
	}

	// Job to sort the pages by descending order of their page ranks.
	Job jobSort  = Job.getInstance(getConf(), "PageRank");
	jobSort.setJarByClass(this.getClass());
	FileInputFormat.addInputPaths(jobSort, IFILE_PATH+"_itr10");	//final intermediate output file from previous iterations
	FileOutputFormat.setOutputPath(jobSort, new Path(args[1]));	// Store final output to the user defined file path
	jobSort.setMapperClass(MapSort.class);
	jobSort.setReducerClass(ReduceSort.class);
	jobSort.setMapOutputKeyClass(DoubleWritable.class);
	jobSort.setMapOutputValueClass(Text.class);
	jobSort.setOutputKeyClass(Text.class);
	jobSort.setOutputValueClass(DoubleWritable.class);
	jobSort.waitForCompletion(true);
	return 1; 

	}
   
	public static class MapCount extends Mapper<LongWritable, Text, Text, IntWritable>{

		private static final Pattern NEW_FILE_ENTRY = Pattern.compile("<title>(.*?)</title>"); // Pattern to match title tag of page

    	  	public void map( LongWritable offset, Text lineText, Context context)
        		throws  IOException,  InterruptedException {
	
			 for (String currentLine : NEW_FILE_ENTRY.split(lineText.toString())) {
			    if (currentLine.isEmpty()) {
			       continue;
			    }
			    
			    context.write(new Text(currentLine),new IntWritable(1));	// For each matched title, we will add it to context with count 1
			}
      		}
   	}

   	public static class ReduceCount extends Reducer<Text, IntWritable, Text, IntWritable>{
      
	      	 @Override 
	      	public void reduce( Text word, Iterable<IntWritable> counts, Context context)
		 	throws IOException, InterruptedException{
		 	int numLines = 0;
	      		for ( IntWritable count : counts) {
		    		numLines  += count.get();	// calculate total number of title tags found. 1 title tag pair -> 1 document
		 	}
		 	context.write(word, new IntWritable(numLines));
			context.getCounter("docCount", "docCount").increment(numLines); // Set counter of context to number of documents found.
		}
	}


	public static class MapInitRank extends Mapper<LongWritable, Text, Text, Text>{
		private static final Pattern title = Pattern.compile("<title>(.*?)</title>"); // Pattern to match title tag of page -> <title>TitleOfPage</title>
		private static final Pattern text = Pattern.compile("<text+\\s*[^>]*>(.*?)</text>"); //Pattern to match -> <text(optional attributes)>SomeText</text>
		private static final Pattern link = Pattern.compile("\\[\\[(.*?)\\]\\]"); // Pattern to match outgoing links of page -> [[outLink]]
		double docCount;

		public void setup(Context context) throws IOException, InterruptedException{
			docCount = context.getConfiguration().getDouble(DOC_COUNT, 1);	// get number of dcos present in input as we calculated earlier
		}

		public void map( LongWritable offset, Text lineText, Context context)
				throws  IOException, InterruptedException {
			String line  = lineText.toString();
			if(line != null && !line.isEmpty()){
				Text fileTitle = new Text();
				Matcher titleMatch = title.matcher(line);
				if(titleMatch.find()){
					fileTitle  = new Text(titleMatch.group(1));	// find the title of file by matching pattern
				}

				Matcher textMatch = text.matcher(line);
				Matcher linkMatch = null;
				if(textMatch.find()){
					linkMatch = link.matcher(textMatch.group(1));	// find outgoing links of file
				}

				double initRank = (double)1/(docCount);		// Initialize page rank for each page with formula: 1/N ... N = docCount
				StringBuilder linkGraph = new StringBuilder("##"+initRank+"##");	// bulid link graph by adding initial rank for each page 

				int count=1;
				while(linkMatch != null && linkMatch.find()) {
					String outLinks = linkMatch.group(1);
					if(count>1)
						linkGraph.append("@#!"+linkMatch.group(1)); 	// Append outgoing link to page (more than 1) with delimiter @#!
					else if(count==1){
						linkGraph.append(outLinks);			// Append any outlink found to graph
						count++;					// Count is used just to check if more than 1 outlinks are present
						}
				}

				
				context.write(fileTitle, new Text(linkGraph.toString()));
			}        
		}
	}

	public static class ReduceInitRank extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text word, Text counts, Context context)
				throws IOException,  InterruptedException {
			context.write(word,counts);		// This function will do nothing, just write input data to conext
		}
	}

	public static class MapPageRank extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable offset, Text lineText, Context context)
				throws  IOException,  InterruptedException{
			String line  = lineText.toString();
			String[] tokens = line.split("##");	// delimit '##' will split line into fileTitle, initial PageRank and Outgoing links	        
			
			if(tokens.length<3){
				context.write(new Text(tokens[0]),new Text("##"+tokens[1])); // Outgoing links are not present for this page
			}
			else if(tokens.length==3){
				String[] outLinks=tokens[2].split("@#!");	// Split outgoing links by its delimeter and process it seperately
				context.write(new Text(tokens[0]),new Text("##"+tokens[1]+"##"+tokens[2]));

				float initialrank = Float.parseFloat(tokens[1]);
				float pageRank = 0.85f * (initialrank/(float)(outLinks.length));	// Calculate pageRank = 0.85 * ((1/N) / NumOutLinks))
					
				for(int i = 0; i < outLinks.length; i++){	
					String pages = outLinks[i];
					pages=pages.replace(" ",""); 			// Remove extra spaces present outgoing link title, if any
					context.write(new Text(pages+"$*"), new Text("##"+Float.toString(pageRank)));
				}
			}
		}
	}

	public static class ReducePageRank extends Reducer<Text, Text, Text, Text>{
		static String temp="";
		static String keyvalue="";
		
		@Override 
		public void reduce(Text word, Iterable<Text> counts, Context context)
				throws IOException,  InterruptedException {			        
			String key = word.toString(),pageRankstr="";
			String[] arr;
			float rank=0.0f;
			key=key.trim();  
			if(!key.contains("$*")){				// Split the key and check if it has page rank for outlinks
				if(!keyvalue.equals("")){
					context.write(new Text(keyvalue), new Text(temp));
				}

				for(Text value:counts){
					pageRankstr=value.toString();
				}
				keyvalue=key;
				temp=pageRankstr;
			}
			else{ 
				key=key.substring(0,key.length()-2); 	// Remove delimiter from key
				key.trim();

				for(Text value:counts){ 
					pageRankstr=value.toString();
					pageRankstr=pageRankstr.substring(2);	
					rank+=Float.parseFloat(pageRankstr);	// calculate page rank		        		  
				}
				rank+=0.15f;


				if(key.equals(keyvalue)){
					arr=temp.split("##");
					if(arr.length>2){
						temp="##"+rank+"##"+arr[2]; 
					}
					else{
						temp="##"+rank; 
					}
					keyvalue="";
					context.write(new Text(key),new Text(temp));
				}
				else{
					if(!keyvalue.equals("")){ 
						context.write(new Text(keyvalue), new Text(temp) ); 
						keyvalue="";
					}
					context.write(new Text(key), new Text("##"+ Float.toString(rank) ) );
				}
			}
		}
	}

	public static class MapSort extends Mapper<LongWritable, Text, DoubleWritable, Text>{
		public void map(LongWritable offset, Text lineText, Context context)
				throws  IOException, InterruptedException{  
			String line = lineText.toString();
			String[] array = line.split("##");
			double rank = Double.parseDouble(array[1]);
			context.write(new DoubleWritable(-1 * rank), new Text(array[0])); // Write -ve values of page rank and sort it in decreasing order
		}
	}

	public static class ReduceSort extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{
		public void reduce(DoubleWritable counts, Iterable<Text> word, Context context) 
			throws IOException, InterruptedException{
			for (Text w : word) {
				context.write(new Text(w.toString()), new DoubleWritable(counts.get() * (-1)));	// Write back the output in positive terms
			}
		}
	}

}
