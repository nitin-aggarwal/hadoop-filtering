package org.hadoop.sbu.graph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Calendar;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hadoop.sbu.files.HDFSStats;
import org.hadoop.sbu.graph.algo.Edmonds;
import org.hadoop.sbu.util.UndirectedGraph;

public class MaximumMatchingEK  extends Configured implements Tool{

	// File in "/user/USER/source" on HDFS to be processed
	public static String[] filenames = {"Graph1"};
	public static long blockSize = 6000;
	public static String user = HDFSStats.getUserName();
	
	public static final Log LOG = LogFactory.getLog("graph.maxMatching");
	static enum MaxMatchCounters {
		inputEdges,
		sampledEdges,
		matchingEdges,
		outEdges,
		vertices
	};
	
	// the type parameters are the input keys type, the input values type, the
	// output keys type, the output values type
	public static class MaxMatchingMapper extends Mapper<LongWritable, Text, Text, Text> {
		Configuration conf;

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] nodes = value.toString().split("\t");
			conf = context.getConfiguration();
			boolean filterFlag = conf.getBoolean("filterFlag", false);
			if(filterFlag)
				context.write(new Text(nodes[0] + "\t" + nodes[1]),new Text(nodes[2]));
			else	{
				//when filtering is not required, we sample the edges with high probability 
				if(sampling(context)){
					//write <key, value> to context where the key is the <WEIGHT>TAB<SOURCE>
					//and the value is the <DESTINATION> node
					context.write(new Text(nodes[0] + "\t" + nodes[1]),new Text(nodes[2]));
					context.getCounter(MaxMatchCounters.sampledEdges).increment(1L);
				}
			}
			context.getCounter(MaxMatchCounters.inputEdges).increment(1L);
		}

		/* Sampling the edge should be considered or not, with high probability */
		private boolean sampling(Context context){
			long numEdges = conf.getLong("numEdges", 0);
			double reductionFactor = (double)numEdges/conf.getLong("edgeThreshold", 1);
			if(reductionFactor > 1){
				int randNum = (int) Math.floor(Math.random()*reductionFactor);
				if(randNum == 0)
					return true;
			}
			else if(reductionFactor == 1)
				return true;
			return false;
		}
	}

	static class MaxMatchingReducer extends Reducer<Text, Text, Text, Text> {
		static UndirectedGraph<String> inGraph = new UndirectedGraph<String>();
		static UndirectedGraph<String> outGraph = new UndirectedGraph<String>();
		Configuration conf;
		boolean filterFlag;

		public void setup(Context context){
			conf = context.getConfiguration();
			String outGraphFile = conf.get("MaxMatchFile");
			try {
				setOutGraph(outGraphFile);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/* Initialize outGraph values from values written in previous step	 */
		void setOutGraph(String outGraphFile) throws IOException	{
			FileSystem fs = FileSystem.get(URI.create(outGraphFile),conf);
			if(fs.exists(new Path(outGraphFile))){
				BufferedReader fin = new BufferedReader(new InputStreamReader(fs.open(new Path(outGraphFile))));
				String line;
				while((line = fin.readLine())!= null ){
					String[] nodesPair = line.split("\t");
					outGraph.addNode(nodesPair[0]);
					outGraph.addNode(nodesPair[1]);
					outGraph.addEdge(nodesPair[0], nodesPair[1]);
				}
			}
		}

		public void reduce(Text inputKey, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			filterFlag = conf.getBoolean("filterFlag", false);
			String srcNode = inputKey.toString().split("\t")[1];
			//Case 1: Maximum Matching computation
			if(!filterFlag){
				//construct input Graph
				for(Text destNode : values){
					//add nodes in graph
					inGraph.addNode(srcNode);
					inGraph.addNode(destNode.toString());
					//add edge
					inGraph.addEdge(srcNode, destNode.toString());
				}
			}
			//Case 2: Filtering of graph
			else	{
				//check if source node is present in maximum matching
				//if yes, then we ignore all edges incident on this node
				if(!outGraph.containsNode(srcNode)){
					for(Text destNode : values){
						if(!outGraph.containsNode(destNode.toString())){
							context.write(inputKey, destNode);
							context.getCounter(MaxMatchCounters.outEdges).increment(1L);
						}		
					}
				}
			}
		}
		
		protected void cleanup(Context context) throws IOException,InterruptedException {
			super.cleanup(context);
			// Max matching Computation
			if(!filterFlag){
				//create a temporary graph for storing max matching in this input graph
				UndirectedGraph<String> tempOutGraph = new UndirectedGraph<String>();
				tempOutGraph = Edmonds.maximumMatching(inGraph);
				//merge the temporary graph with main Max matching graph
				outGraph.mergeAll(tempOutGraph);

				//write combined MaximumMatching in file
				String path = conf.get("MaxMatchFile");
				FileSystem fs1 = FileSystem.get(URI.create(path),conf);
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs1.create(new Path(path),true)));

				Iterator<String> itr = outGraph.iterator();
				while(itr.hasNext()){
					String node = (String) itr.next();
					for(String nodeDest : outGraph.edgesFrom(node))	{
						if(node.compareTo(nodeDest) < 0)	{
							bw.write(node + "\t" + nodeDest+"\n");
							context.getCounter(MaxMatchCounters.matchingEdges).increment(1L);
						}
					}
				}
				bw.close();
			}
		}
	}

	private Job getJob(String[] args, boolean filterFlag, long numEdges, int numReducer, String filename) throws IOException {

		Configuration conf = new Configuration();

		Path maxMatchFile = new Path("output/maxMatching-"+filename);
		conf.set("MaxMatchFile", maxMatchFile.toString());
		conf.setLong("numEdges", numEdges);
		conf.setLong("edgeThreshold", blockSize);
		if(filterFlag)
			conf.setBoolean("filterFlag", true);
		
		Job job = new Job(conf);
		job.setJobName("MaxMatching");
		
		job.setNumReduceTasks(numReducer);

		job.setJarByClass(MaximumMatchingEK.class);

		// the keys are the unique identifiers for a Node (ints in this case).
		job.setOutputKeyClass(Text.class);
		// the values are the string representation of a Node
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MaxMatchingMapper.class);
		job.setReducerClass(MaxMatchingReducer.class);
		return job;
	}

	/** Main driver for maximum matching MapReduce program */
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		for(String filename: filenames)	{
			
			// File with all the statistics
			String path="logs/MaxMatch-"+filename;
			FileSystem fs1 = FileSystem.get(URI.create(path),conf);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs1.create(new Path(path),true)));
			// File with the final maximum matching edges
			String maxPath="logs/MaxMatch-matching-"+filename;
			FileSystem fs2 = FileSystem.get(URI.create(maxPath),conf);
			BufferedWriter bw2 = new BufferedWriter(new OutputStreamWriter(fs2.create(new Path(maxPath),true)));
			long match = 0;
			
			int iterationCount = 0;
			long numEdges = 0,outEdges = 0;
			boolean flagStop = false;
			Job job = null;
			String input = "source/"+filename;
			bw.write("********************************************************\n");
			bw.write("Processing File: "+filename+"\n");
			Calendar startTime = Calendar.getInstance();
			do {
				bw.write("************  Iteration: "+(iterationCount+1)+" ***********\n");
				String output = "output1/MaxMatch-"+filename +"-"+ (iterationCount + 1);
				//Iteration 0: To compute edges
				if(iterationCount == 0)
					job = getJob(args, false, 0, 0,filename);
				//for sampling and computing max matching
				else if(iterationCount % 2 == 1){
					if(iterationCount > 1)
						numEdges = outEdges;
					job = getJob(args, false, numEdges, 1,filename);
					input = "output1/MaxMatch-"+filename +"-"+ (iterationCount);
				}
				//for filtering of graph
				else{
					job = getJob(args, true, 0, 3, filename);
					input = "output1/MaxMatch-"+filename +"-"+(iterationCount - 1);
				}
				//initial cases when same input file has to be used
				if(iterationCount == 0 || iterationCount == 1 || iterationCount == 2)
					input = "source/"+filename;
				
				FileInputFormat.addInputPath(job, new Path(input));
				FileOutputFormat.setOutputPath(job, new Path(output));
				job.waitForCompletion(true);
	
				Counters jobCntrs = job.getCounters();
				numEdges = jobCntrs.findCounter(MaxMatchCounters.inputEdges).getValue();
				outEdges = jobCntrs.findCounter(MaxMatchCounters.outEdges).getValue();
				long sampledEdges = jobCntrs.findCounter(MaxMatchCounters.sampledEdges).getValue();
				long matchingEdges = jobCntrs.findCounter(MaxMatchCounters.matchingEdges).getValue();
				long vertexSize = jobCntrs.findCounter(MaxMatchCounters.vertices).getValue();
				bw.write("Input edges set: " + numEdges+"\n");
				bw.write("Sampled Edges: " + sampledEdges+"\n");
				bw.write("Maximal Matching Edges: " + matchingEdges+"\n");
				bw.write("Reduced edges set: "+ outEdges+"\n");
				bw.write("Vertices: "+ vertexSize+"\n");
				
				if(iterationCount%2 == 1)	{
					String temp = output + "/part-r-00000";
					FileSystem fs3 = FileSystem.get(URI.create(temp),conf);
					BufferedReader fin = new BufferedReader(new InputStreamReader(fs3.open(new Path(temp))));
					String line;
					while((line = fin.readLine())!= null)	{
						bw2.write(line+"\n");
						match++;
					}
				}
				
				if(numEdges == sampledEdges)
					break;
			
				
				//for program termination 
				if( (iterationCount != 0) && (iterationCount % 2 == 0) && (numEdges < blockSize) )
					flagStop = true;
				
				if(flagStop)
					break;
		
				iterationCount++;
			} while (true);
			Calendar finishTime = Calendar.getInstance();
			bw.write("Total Iterations: "+iterationCount+" Match: "+match+"\n");
			bw.write("Time Taken: "+(finishTime.getTimeInMillis()-startTime.getTimeInMillis())/1000.0+" seconds\n");
			bw.close();
			bw2.close();
		}
		return 0;
	}

	private static int printUsage() {
		System.out.println("Correct Usage: MaximumMatching filename");
		return -1;
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length == 1)	{
			blockSize = Integer.parseInt(args[0]);
		}
		else if(args.length == 2)	{
			filenames = args[0].split(" ");
			blockSize = Integer.parseInt(args[1]);
		}
		else if(args.length > 2)	{
			printUsage();
			return;
		}
		int res = ToolRunner.run(new Configuration(), new MaximumMatchingEK(), args);
		System.exit(res);
	}
}

