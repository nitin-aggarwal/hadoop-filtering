package org.hadoop.sbu.graph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

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

public class VertexCoverCLR  extends Configured implements Tool{

	// File in "/user/USER/source" on HDFS to be processed
	public static String[] filenames = {"edges1","Graph1","Graph2","Graph3"};
	public static long blockSize = 2500000;
	public static String user = HDFSStats.getUserName();
	
	public static final Log LOG = LogFactory.getLog("graph.VCCLRMatching");
	static enum VCCLRCounters {
		inputEdges,
		sampledEdges,
		matchingEdges,
		outEdges,
		vertices
	};
	
	// the type parameters are the input keys type, the input values type, the
	// output keys type, the output values type
	public static class VCCLRMapper extends Mapper<LongWritable, Text, Text, Text> {
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
					context.getCounter(VCCLRCounters.sampledEdges).increment(1L);
				}
			}
			context.getCounter(VCCLRCounters.inputEdges).increment(1L);
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
			else if(reductionFactor > 0 && reductionFactor <= 1)
				return true;
			return false;
		}
	}

	static class VCCLRReducer extends Reducer<Text, Text, Text, Text> {
		static Set<Integer> vertexSet = new HashSet<Integer>();
		Configuration conf;
		boolean filterFlag;
		
		public void setup(Context context){
			conf = context.getConfiguration();
			vertexSet.add(new Integer(0));
			filterFlag = conf.getBoolean("filterFlag", false);
			String outGraphFile = conf.get("vertexFile");
			try {
				setOutGraph(outGraphFile);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/* Initialize vertexSet values from values written in previous step	 */
		void setOutGraph(String outGraphFile) throws IOException	{
			FileSystem fs = FileSystem.get(URI.create(outGraphFile),conf);
			if(fs.exists(new Path(outGraphFile))){
				BufferedReader fin = new BufferedReader(new InputStreamReader(fs.open(new Path(outGraphFile))));
				String line;
				while((line = fin.readLine())!= null)
					vertexSet.add(new Integer(line));
			}
		}

		public void reduce(Text inputKey, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Integer srcNode = new Integer(inputKey.toString().split("\t")[1]);
			//Case 1: Maximum Matching computation
			if(!filterFlag){
				//Construct input vertex Set and computing Maximum Matching
				for(Text destNode : values){
					//add nodes in graph
					Integer otherNode = new Integer(destNode.toString());
					if(!vertexSet.contains(srcNode) && !vertexSet.contains(otherNode))	{
						vertexSet.add(srcNode);
						vertexSet.add(otherNode);
						context.getCounter(VCCLRCounters.matchingEdges).increment(1L);
					}
				}
			}
			//Case 2: Filtering of graph
			else	{
				//check if source node is present in maximum matching
				//if yes, then we ignore all edges incident on this node
				if(!vertexSet.contains(srcNode)){
					for(Text destNode : values){
						Integer otherNode = new Integer(destNode.toString());
						if(!vertexSet.contains(otherNode)){
							context.write(inputKey, destNode);
							context.getCounter(VCCLRCounters.outEdges).increment(1L);
						}		
					}
				}
			}
		}
		
		protected void cleanup(Context context) throws IOException,InterruptedException {
			super.cleanup(context);
			// Max matching Computation
			if(!filterFlag){
				//write vertex Set in a file
				String path = conf.get("vertexFile");
				FileSystem fs1 = FileSystem.get(URI.create(path),conf);
				BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(fs1.create(new Path(path),true)));

				for(Integer vertex: vertexSet)	{
					bw1.write(vertex +"\n");
					context.getCounter(VCCLRCounters.vertices).increment(1L);
				}
				bw1.close();
			}
		}
	}

	private Job getJob(String[] args, boolean filterFlag, long numEdges, int numReducer, String filename) throws IOException {

		Configuration conf = new Configuration();

		Path vertexFile = new Path("logs/VCCLR-vertex-"+filename);
		conf.set("vertexFile", vertexFile.toString());
		conf.setLong("numEdges", numEdges);
		conf.setLong("edgeThreshold", blockSize);
		if(filterFlag)
			conf.setBoolean("filterFlag", true);
		
		Job job = new Job(conf);
		job.setJobName("VCCLR");
		
		job.setNumReduceTasks(numReducer);

		job.setJarByClass(VertexCoverCLR.class);

		// the keys are the unique identifiers for a Node (ints in this case).
		job.setOutputKeyClass(Text.class);
		// the values are the string representation of a Node
		job.setOutputValueClass(Text.class);

		job.setMapperClass(VCCLRMapper.class);
		job.setReducerClass(VCCLRReducer.class);
		return job;
	}

	/** Main driver for maximum matching MapReduce program */
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		for(String filename: filenames)	{
			
			// File with all the statistics
			String path="logs/VCCLR-"+filename;
			FileSystem fs1 = FileSystem.get(URI.create(path),conf);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs1.create(new Path(path),true)));
			
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
				String output = "output1/VCCLR-"+filename +"-"+ (iterationCount + 1);
				//Iteration 0: To compute edges
				if(iterationCount == 0)
					job = getJob(args, false, 0, 0,filename);
				//for sampling and computing max matching
				else if(iterationCount % 2 == 1){
					if(iterationCount > 1)
						numEdges = outEdges;
					job = getJob(args, false, numEdges, 1,filename);
					input = "output1/VCCLR-"+filename +"-"+ (iterationCount);
				}
				//for filtering of graph
				else{
					job = getJob(args, true, 0, 3, filename);
					input = "output1/VCCLR-"+filename +"-"+(iterationCount - 1);
				}
				//initial cases when same input file has to be used
				if(iterationCount == 0 || iterationCount == 1 || iterationCount == 2)
					input = "source/"+filename;
				
				FileInputFormat.addInputPath(job, new Path(input));
				FileOutputFormat.setOutputPath(job, new Path(output));
				job.waitForCompletion(true);
	
				Counters jobCntrs = job.getCounters();
				numEdges = jobCntrs.findCounter(VCCLRCounters.inputEdges).getValue();
				outEdges = jobCntrs.findCounter(VCCLRCounters.outEdges).getValue();
				long sampledEdges = jobCntrs.findCounter(VCCLRCounters.sampledEdges).getValue();
				long matchingEdges = jobCntrs.findCounter(VCCLRCounters.matchingEdges).getValue();
				long vertexSize = jobCntrs.findCounter(VCCLRCounters.vertices).getValue();
				bw.write("Input edges set: " + numEdges+"\n");
				bw.write("Sampled Edges: " + sampledEdges+"\n");
				bw.write("Maximal Matching Edges: " + matchingEdges+"\n");
				bw.write("Reduced edges set: "+ outEdges+"\n");
				bw.write("Vertices: "+ vertexSize+"\n");
				
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
			bw.write("Total Iterations: "+iterationCount+"\n");
			bw.write("Time Taken: "+(finishTime.getTimeInMillis()-startTime.getTimeInMillis())/1000.0+" seconds\n");
			bw.close();
		}
		return 0;
	}

	private static int printUsage() {
		System.out.println("Correct Usage: MaximumMatchingCLR filenames blocksize");
		System.out.println("********** OR ***************");
		System.out.println("Correct Usage: MaximumMatchingCLR blockSize");
		System.out.println("********** OR ***************");
		System.out.println("Correct Usage: MaximumMatchingCLR");
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
		int res = ToolRunner.run(new Configuration(), new VertexCoverCLR(), args);
		System.exit(res);
	}
}

