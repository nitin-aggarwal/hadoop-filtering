package org.hadoop.sbu.graph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Calendar;
import java.util.HashSet;

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



public class EdgeCoverCLR  extends Configured implements Tool{

	// File in "/user/USER/source" on HDFS to be processed
	public static String[] filenames = {"edges1","Graph1","Graph2","Graph3"};
	public static String user = HDFSStats.getUserName();
	
	public static final Log LOG = LogFactory.getLog("graph.EdgeCoverCLR");
	static enum ECCLRCounters {
		inputEdges,
		outEdges
	};
	
	// the type parameters are the input keys type, the input values type, the
	// output keys type, the output values type
	public static class MaxMatchingMapper extends Mapper<LongWritable, Text, Text, Text> {
		Configuration conf;
		HashSet<Integer> vertexSet = new HashSet<Integer>();
		
		/* setup method to create maximum matching graph 
		 * based on values computed in last iteration, if any
		 */
		public void setup(Context context){
			conf = context.getConfiguration();
			String outGraphFile = conf.get("MaxMatchFile");
			try {
				constructVertexSet(outGraphFile);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/* initialize outGraph values from values written in previous step */ 
		void constructVertexSet(String outGraphFile) throws IOException{
			FileSystem fs = FileSystem.get(URI.create(outGraphFile),conf);
			if(fs.exists(new Path(outGraphFile))){
				BufferedReader fin = new BufferedReader(new InputStreamReader(fs.open(new Path(outGraphFile))));
				String line;
				while((line = fin.readLine())!= null ){
					String[] nodesPair = line.split("\t");
					vertexSet.add(new Integer(nodesPair[1]));
					vertexSet.add(new Integer(nodesPair[2]));
				}
			}
		}
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] nodes = value.toString().split("\t");
			conf = context.getConfiguration();

			//write <key, value> to context where the key is the weight tab src node, 
			//and the value is the dest node
			if(!vertexSet.contains(new Integer(nodes[1])) || !vertexSet.contains(new Integer(nodes[2])))	{
				context.write(new Text(nodes[0] + "\t" + nodes[1]),new Text(nodes[2]));
				context.getCounter(ECCLRCounters.inputEdges).increment(1L);
			}
		}
	}



	/**
	 * Reducer class for computing Min Edge Cover for input edges emitted by mapper
	 * @author hduser
	 *
	 */
	static class MaxMatchingReducer extends Reducer<Text, Text, Text, Text> {
		Configuration conf;
		HashSet<Integer> vertexSet = new HashSet<Integer>();
		
		/* setup method to create maximum matching graph 
		 * based on values computed in last iteration, if any
		 */
		public void setup(Context context){
			conf = context.getConfiguration();
			String outGraphFile = conf.get("MaxMatchFile");
			try {
				constructVertexSet(outGraphFile);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/* initialize outGraph values from values written in previous step */ 
		void constructVertexSet(String outGraphFile) throws IOException{
			FileSystem fs = FileSystem.get(URI.create(outGraphFile),conf);
			if(fs.exists(new Path(outGraphFile))){
				BufferedReader fin = new BufferedReader(new InputStreamReader(fs.open(new Path(outGraphFile))));
				String line;
				while((line = fin.readLine())!= null ){
					String[] nodesPair = line.split("\t");
					vertexSet.add(new Integer(nodesPair[1]));
					vertexSet.add(new Integer(nodesPair[2]));
				}
			}
		}
		public void reduce(Text inputKey, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String srcNode = inputKey.toString().split("\t")[1];
			
			//check if source node is present in maximum matching
			//if yes, then we ignore all edges incident on this node
			for(Text destNode : values){
				if(!vertexSet.contains(new Integer(srcNode)) || !vertexSet.contains(new Integer(destNode.toString())))	{
					context.write(inputKey,destNode);
					vertexSet.add(new Integer(srcNode));
					vertexSet.add(new Integer(destNode.toString()));
					context.getCounter(ECCLRCounters.outEdges).increment(1L);
				}
			}
		}

	}

	/**
	 * method to create job for MapReduce operations
	 * @param args
	 * @param numReducer
	 * @return job with requested params
	 * @throws IOException
	 */
	private Job getJob(String[] args, int numReducer, String filename) throws IOException {
		//set configuration params
		Configuration conf = new Configuration();
		//set path for file containing Maximum Matching edges
		Path maxMatchFile = new Path("logs/MMCLR-matching-"+filename);
		conf.set("MaxMatchFile", maxMatchFile.toString());
		
		//set job params
		Job job = new Job(conf);
		job.setJobName("EdgeCoverCLR");
		job.setNumReduceTasks(numReducer);
		job.setJarByClass(EdgeCoverCLR.class);
		// the keys are the unique identifiers for a Node (ints in this case).
		job.setOutputKeyClass(Text.class);
		// the values are the string representation of a Node
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MaxMatchingMapper.class);
		job.setReducerClass(MaxMatchingReducer.class);

		return job;
	}

	/** Main driver for Min Edge Cover MapReduce program */
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		long inputEdges = 0;
		long outEdges = 0;
		for(String filename: filenames)	{
			
			// Log File for edge cover Statistics
			String path="logs/ECCLR-"+filename;
			FileSystem fs = FileSystem.get(URI.create(path),conf);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path),true)));
			
			// File containing the final edge cover edges
			String maxPath="logs/ECCLR-edgeCover-"+filename;
			FileSystem fs1 = FileSystem.get(URI.create(path),conf);
			BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(fs1.create(new Path(maxPath),true)));
			long match = 0;
			
			FileSystem fs2 = FileSystem.get(URI.create("logs/MMCLR-matching-"+filename),conf);
			BufferedReader fin = new BufferedReader(new InputStreamReader(fs2.open(new Path("logs/MMCLR-matching-"+filename))));
			
			String line;
			// Write the maximal matching in the edge cover
			while((line = fin.readLine())!= null)	{
				bw1.write(line+"\n");
				match++;
			}
			fin.close();
			String input = "source/"+filename;
			String output = "output1/ECCLR-"+filename;
			bw.write("********************************************************\n");
			bw.write("Processing File: "+filename+"\n");
			Job job = getJob(args,1,filename);
			Calendar startTime = Calendar.getInstance();
			
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);

			Counters jobCntrs = job.getCounters();
			inputEdges = jobCntrs.findCounter(ECCLRCounters.inputEdges).getValue();
			outEdges = jobCntrs.findCounter(ECCLRCounters.outEdges).getValue();
			bw.write("Input edges set: " + inputEdges+"\n");
			bw.write("Reduced edges set: "+ outEdges+"\n");
			
			output +="/part-r-00000";
			FileSystem fs3 = FileSystem.get(URI.create(output),conf);
			fin = new BufferedReader(new InputStreamReader(fs3.open(new Path(output))));
			while((line = fin.readLine())!= null)	{
				bw1.write(line+"\n");
				match++;
			}

			Calendar finishTime = Calendar.getInstance();
			bw.write("Total Edge Cover edges: "+match+"\n");
			bw.write("Time Taken: "+(finishTime.getTimeInMillis()-startTime.getTimeInMillis())/1000.0+" seconds\n");
			bw.close();
			bw1.close();
		}
		return 0;
	}

	//main program== 0
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new EdgeCoverCLR(), args);
		System.exit(res);
	}


}

