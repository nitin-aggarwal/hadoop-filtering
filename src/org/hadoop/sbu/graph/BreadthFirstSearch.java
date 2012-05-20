package org.hadoop.sbu.graph;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.hadoop.sbu.files.DeleteAllFiles;
import org.hadoop.sbu.files.HDFSStats;
import org.hadoop.sbu.util.Node;
import org.hadoop.sbu.util.Node.Color;

/**
 * Input format is 
 * ID	EDGES|DISTANCE|COLOR|
 * where ID = the unique identifier for a node 
 * EDGES = the list of nodes with an edge from the node (e.g. 3,8,9,12) 
 * DISTANCE = the to be determined distance of the node from the source 
 * COLOR = field to keep track of when we're finished with a node i.e. (WHITE|GRAY|BLACK) 
 * 
 * It assumes that the source node (the node from which to start the search) has
 * been marked with distance 0 and color GRAY in the original input. All other
 * nodes will have input distance Integer.MAX_VALUE and color WHITE.
 */
public class BreadthFirstSearch extends Configured implements Tool {

	// File in "/user/USER/source" on HDFS to be processed
	public static String filename = "bfs";
	public static String user = HDFSStats.getUserName();
	
	public static final Log LOG = LogFactory.getLog("graph.BreadthFirstSearch");

	public static enum count {
		PROCESSED_GRAY,			// Number of gray nodes processed 
		UNPROCESSED_GRAY		// Number of gray nodes to be processed
	};

	/**
	 * Nodes that are Color.WHITE or Color.BLACK are emitted, as is it is. 
	 * For every edge of a Color.GRAY node, we emit a new Node with distance incremented
	 * by one. The Color.GRAY node is then colored black and is also emitted.
	 */
	static class MapClass extends Mapper<LongWritable, Text, IntWritable, Text> {

		/**
		 * Key - empty
		 * Value - entire row of Input
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			Node node = new Node(value.toString());

			// For each GRAY node, emit each of the edges as a new node
			if (node.getColor() == Node.Color.GRAY) {
				for (int v : node.getEdges()) {
					Node vnode = new Node(v);
					vnode.setDistance(node.getDistance() + 1);
					vnode.setColor(Node.Color.GRAY);
					context.write(new IntWritable(vnode.getId()), vnode.getLine());
					LOG.info("Gray node added");
				}
				// We're done with this node now, color it BLACK
				node.setColor(Node.Color.BLACK);
				context.getCounter(count.PROCESSED_GRAY).increment(1L);
				LOG.info("Black node added");
			}

			// No matter what, we emit the input node
			// If the node came into this method GRAY, it will be output as
			// BLACK
			context.write(new IntWritable(node.getId()), node.getLine());
		}
	}

	/**
	 * A reducer class that just emits the sum of the input values.
	 */
	static class ReduceClass extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		/**
		 * Make a new node which combines all information for this single node id
		 * The new node should have: 
		 * 1. The full list of edgesText
		 * 2. The minimum distance
		 * 3. The darkest Color
		 */
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			List<Integer> edges = null;
			int distance = Integer.MAX_VALUE;
			Node.Color color = Node.Color.WHITE;

			for (Text value : values) {

				Node u = new Node(key.get() + "\t" + value.toString());

				if (u.getEdges().size() > 0) {
					edges = u.getEdges();
				}

				// Save the minimum distance
				if (u.getDistance() < distance) {
					distance = u.getDistance();
				}

				// Save the darkest color
				if (u.getColor().ordinal() > color.ordinal()) {
					color = u.getColor();
				}
			}

			Node n = new Node(key.get());
			n.setDistance(distance);
			n.setEdges(edges);
			n.setColor(color);
			context.write(key, new Text(n.getLine()));

			if (color == Color.GRAY)
				context.getCounter(count.UNPROCESSED_GRAY).increment(1L);
		}
	}

	static int printUsage() {
		System.out.println("Correct Usage: BreadthFirstSearch filename");
		return -1;
	}

	private Job getJob(String[] args, boolean oneReducer) throws IOException {

		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("bfs");
		if (oneReducer)
			job.setNumReduceTasks(1);

		job.setJarByClass(BreadthFirstSearch.class);

		// the keys are the unique identifiers for a Node (ints in this case).
		job.setOutputKeyClass(IntWritable.class);
		// the values are the string representation of a Node
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		return job;
	}

	/** The main driver for BFS MapReduce program */
	public int run(String[] args) throws Exception {

		int iterationCount = 0;
		long numGrayUnProcessed = 1, numGrayProcessed = 0;
		boolean oneReducer = false;
		
		while (numGrayUnProcessed > 0) {
			String input;
			if (iterationCount == 0)
				input = "source/"+filename;
			else
				input = "output/BFS-"+filename+"-" + iterationCount;

			String output = "output/BFS-"+filename+"-" + (iterationCount + 1);
			Job job = getJob(args, oneReducer);
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);

			Counters counters = job.getCounters();
			// Loop until the number of Gray nodes is zero...
			numGrayProcessed = (long) (counters.findCounter(count.PROCESSED_GRAY)).getValue();
			numGrayUnProcessed = (long) (counters.findCounter(count.UNPROCESSED_GRAY)).getValue();
			LOG.info("Gray Nodes Processed Count= " + numGrayProcessed);
			LOG.info("Gray Nodes UnProcessed Count= "+ numGrayUnProcessed);

			if(iterationCount >= 1)
				DeleteAllFiles.deleteFile(user,"output","BFS-"+filename+"-"+iterationCount);
			iterationCount++;
			if (numGrayProcessed >= numGrayUnProcessed)
				oneReducer = true;
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		boolean debug = true;
		if(args.length == 1)	{
			filename = args[0];
		}
		else if(!debug)	{
			printUsage();
			return;
		}
		int res = ToolRunner.run(new Configuration(), new BreadthFirstSearch(),	args);
		System.exit(res);
	}
}