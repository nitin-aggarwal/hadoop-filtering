package org.hadoop.sbu.assignment;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.hadoop.sbu.files.DeleteAllFiles;

public class EstimateReachables extends Configured implements Tool {

	// Configure variables accordingly
	static int noReducers = 2;
	static String filename = "test-02-in.txt";
	static int limitK = 65;
	
	static enum VCounters {
		iteration
	}
	
	static class MapClass extends Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] arr = value.toString().split(" ");
			context.write(new LongWritable(Long.parseLong(arr[0])), new Text(arr[1]));
		}
	}
	
	static class EvalMapClass extends Mapper<LongWritable, Text, LongWritable, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] arr = value.toString().split("\t");
			StringBuilder temp = new StringBuilder();
			for(int z = 1;z<arr.length-1;z++){
				temp.append(arr[z] + "\t");
			}
			temp.append(arr[arr.length-1]+"");
			context.write(new LongWritable(Long.parseLong(arr[0])), new Text(temp.toString()));
		}
	}

	static class ReduceClass extends Reducer<LongWritable, Text, LongWritable, Text> {
		
		Map<Long, List<Integer>> nodeBitMap = new HashMap<Long, List<Integer>>();
		int kReducer;
		int L = 32;
		Configuration conf = null;
		
		
		protected void setup(Context context)	{
			conf = context.getConfiguration();
			kReducer = Integer.parseInt(conf.get("K"));
			try {
				String iterMapfile = conf.get("interMapInput");
				setNodeBitMaps(iterMapfile);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		private void setNodeBitMaps(String fileName) throws IOException{
			FileSystem fs = FileSystem.get(URI.create(fileName),conf);
			BufferedReader fin = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))));
			String line;
			while((line = fin.readLine())!= null ){
				String[] nodeValPair = line.split("\t");
				List<Integer> nodeVal = new ArrayList<Integer>();
				for(int index = 0; index < kReducer; index++){
					nodeVal.add(Integer.valueOf(nodeValPair[index+1]));
				}
				nodeBitMap.put(Long.valueOf(nodeValPair[0]), nodeVal);
			}
		}
		
		private List<Integer> getBitMap(long node){
			List<Integer> nodeBitList = new ArrayList<Integer>();
			for(int i=0;i < kReducer;i++)	{
				nodeBitList.add(new Integer(0));
			}
			int k,l,temp;
			long hashValue;
			hashValue = (long) hash(node);
			k = (int) (hashValue % kReducer);
			temp = (int) (hashValue / kReducer);
			l = compute(temp);
			if(l < 32 && l >= 0)
				nodeBitList.set((int)k, new Integer((int)Math.pow(2,l)));
			return nodeBitList;
		}
		
		private List<Integer> BitWiseOR(List<Integer> nodeValList, long node1){
			List<Integer> node1BitList = nodeBitMap.get(node1);
			if(node1BitList == null)
				node1BitList = getBitMap(node1);
			
			for(int index = 0; index < kReducer; index++){
				nodeValList.set(index, new Integer(node1BitList.get(index) | nodeValList.get(index)));
			}
			return nodeValList;
		}
		
		private boolean CompareList(List<Integer> oldNodeValList, List<Integer> newNodeValList){
			boolean flag = false;
			double m1 = evaluate(oldNodeValList);
			double m2 = evaluate(newNodeValList);
			if(m1 != m2)
				flag = true;
			
			return flag;
		}
		
		private double evaluate(List<Integer> nodeValList){
			int sGlobal = 0;
			double m =0;
			int r = 0;
			for(int z=0;z < kReducer;z++)	{
				r = computeMin(nodeValList.get(z));
				sGlobal += r;
			}
			m = (kReducer/0.77351) * Math.pow(2, (double)sGlobal/kReducer);
			return m;
		}
		
		private int computeMin(int value)	{
			int bit = 1;
			int result;
			for(int i=0;i<32;i++)
			{
				result = value & (bit << i);
				if(result == 0)
					return i;
			}	
			return L;
		}
		
		private long hash(long value)
		{
			long hashValue = 0;
			long a = 1073741827L;
			long b = 17179869209L;
			long q = 4294967291L;
			hashValue = (a*value + b) % q;
			return hashValue;
		}
		private int compute(int value)
		{
			if(value == 0)
				return L;
			else	{
				int bit = 1;
				int result;
				for(int i=0;i<32;i++)	{
					result = value & (bit << i);
					if(result > 0)
						return i;
				}
			}
			return -1;
		}
		
		public void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
			boolean flag ;
			List<Integer> oldNodeValList = nodeBitMap.get(Long.parseLong(key.toString()));
			List<Integer> newNodeValList = new ArrayList<Integer>();
			
			if(oldNodeValList == null)
				oldNodeValList = getBitMap(Long.parseLong(key.toString()));
			newNodeValList.addAll(oldNodeValList);
			for(Text value : values){
				Long node1 = Long.parseLong(value.toString());
				newNodeValList = BitWiseOR(newNodeValList, node1);
			}
			flag = CompareList(oldNodeValList, newNodeValList);
			if(flag)
				context.getCounter(VCounters.iteration).increment(1L);
			StringBuilder temp = new StringBuilder();
			for(int z = 0;z<kReducer-1;z++){
				temp.append(newNodeValList.get(z) + "\t");
			}
			temp.append(newNodeValList.get(kReducer-1)+"");
			context.write(key, new Text(temp.toString()));
        }
	}
	
	static class EvalReduceClass extends Reducer<LongWritable, Text, LongWritable, Text> {
		
		int kReducer;
		int L = 32;
		Configuration conf = null;
		
		protected void setup(Context context)	{
			conf = context.getConfiguration();
			kReducer = Integer.parseInt(conf.get("K"));
		}
		
		private double evaluate(List<Integer> nodeValList){
			int sGlobal = 0;
			double m =0;
			int r = 0;
			for(int z=0;z < kReducer;z++)	{
				r = computeMin(nodeValList.get(z));
				sGlobal += r;
			}
			m = (kReducer/0.77351) * Math.pow(2, (double)sGlobal/kReducer);
			return m;
		}
		
		private int computeMin(int value)	{
			int bit = 1;
			int result;
			for(int i=0;i<32;i++)	{
				result = value & (bit << i);
				if(result == 0)
					return i;
			}	
			return L;
		}
		
		public void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
			
			List<Integer> nodeBitmap = new ArrayList<Integer>();
			String[] integers = null;
			for(Text t: values)	{
				integers = t.toString().split("\t");
				for(int z = 0; z < kReducer; z++)
					nodeBitmap.add(Integer.parseInt(integers[z]));
			}
			double m = evaluate(nodeBitmap);
			context.write(key, new Text(String.valueOf((long)Math.floor(m))));
        }
	}
	
	private Job getJob(String[] args, int K,long iter,int reducers) throws IOException {
		
		Configuration conf = new Configuration();
		conf.setInt("K", K);
		conf.setStrings("interMapInput", "/user/nitin/vertex/distinctC-" + K+"-"+iter);
		Job job = new Job(conf);
		job.setJobName("EstimateReachables");
		job.setNumReduceTasks(reducers);

		job.setJarByClass(EstimateReachables.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		return job;
	}
	private Job getEvalJob(String[] args, int K,int reducers) throws IOException {
		
		Configuration conf = new Configuration();
		conf.setInt("K", K);
		Job job = new Job(conf);
		job.setJobName("Evaluate");
		job.setNumReduceTasks(reducers);

		job.setJarByClass(EstimateReachables.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(EvalMapClass.class);
		job.setReducerClass(EvalReduceClass.class);

		return job;
	}
	
	private long hash(long value)
	{
		long hashValue = 0;
		long a = 1073741827L;
		long b = 17179869209L;
		long q = 4294967291L;
		hashValue = (a*value + b) % q;
		return hashValue;
	}

	/**
	 * The main driver for estimating node reachable map/reduce program. Invoke this method to
	 * submit the map/reduce job.
	 */
	public int run(String[] args) throws Exception {
		int K = 1;
		int iteration = 1;
		
		// File to write the results
		Configuration config = new Configuration();
		String pathF="output/Reachability-"+filename;
		FileSystem fsF = FileSystem.get(URI.create(pathF),config);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsF.create(new Path(pathF),true)));
		
		while(K <= limitK)	{
			boolean status = true;
			iteration = 1;
			
			Configuration conf = new Configuration();
			while(status)	{
				String input = "source/"+filename;
				String output = "vertex/distinct-" + K+"-"+iteration;
		
				Job job = getJob(args,K,iteration-1,noReducers);
				
				FileInputFormat.addInputPath(job, new Path(input));
				FileOutputFormat.setOutputPath(job, new Path(output));
				job.waitForCompletion(true);
				
				Counters jobCntrs = job.getCounters();
				long iterationVote = jobCntrs.findCounter(VCounters.iteration).getValue();
	
				System.out.println("Iteration: "+ iteration+" Nodes: " + iterationVote);
				if(iterationVote == 0){
					status = false;
				}
				
				// Construct a combined file of reducers output
				String path="vertex/distinctC-"+K+"-"+iteration;
				FileSystem fs1 = FileSystem.get(URI.create(path),conf);
				BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(fs1.create(new Path(path),true)));
				for(int i=0;i<noReducers;i++)	{
					String file = "vertex/distinct-" + K+"-"+(iteration)+"/part-r-0000"+i;
					FileSystem fs = FileSystem.get(URI.create(file),conf);
					BufferedReader fin = new BufferedReader(new InputStreamReader(fs.open(new Path(file))));
					String line;
					
					while((line = fin.readLine())!= null ){
						bw1.write(line);
						bw1.write("\n");
					}	
				}
				bw1.flush();
				bw1.close();
				DeleteAllFiles.deleteFile("nitin","vertex","distinct-"+K+"-"+iteration);
				if(iteration > 1)
					DeleteAllFiles.deleteFile("nitin","vertex","distinctC-"+K+"-"+(iteration-1));
				
				iteration++;	
			}
			// Create another job to compute Evaluate function from bitmaps
			String input = "vertex/distinctC-"+K+"-"+(iteration-1);
			String output = "vertex/evaluate-" + K;
	
			Job job = getEvalJob(args,K,1);
			
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
	
			
			// Compute CheckSum, minimum, maximum and average values for K
			long max = 0,min = Long.MAX_VALUE,count = 0;
			long c = 0;
			double average = 0;
			String fileEvaluate = "vertex/evaluate-" + K+"/part-r-00000";
			FileSystem fs = FileSystem.get(URI.create(fileEvaluate),conf);
			BufferedReader fin = new BufferedReader(new InputStreamReader(fs.open(new Path(fileEvaluate))));
			String line;
			
			while((line = fin.readLine())!= null ){
				String[] nodeValPair = line.split("\t");
				long node = Long.parseLong(nodeValPair[1]);
				c = hash(c + node);
				if(node > max)
					max = node;
				if(node < min)
					min = node;
				count++;
				average += node;
			}
			average /= count;
			average = Math.round(average);
					
			System.out.println("Iterations: "+(iteration-1)+" Max: "+max+" Min: "+min+ " Avg: "+average+" K: "+K+" C: "+c);
			bw.write("Iterations: "+(iteration-1)+" Max: "+max+" Min: "+min+ " Avg: "+average+" K: "+K+" C: "+c);
			bw.write("\n");
			bw.flush();
			
			DeleteAllFiles.deleteFile("nitin","vertex","distinctC-"+K+"-"+(iteration-1));
			DeleteAllFiles.deleteFolder("nitin","vertex/evaluate-"+K);
			K = K * 4;
		}
		bw.close();
		//DeleteAllFiles.deleteFolder("nitin","vertex");
		return 0;
	}
	
	
	public static void main(String[] args) throws Exception {
		if(args.length >= 2)	{
			filename = args[0];
			limitK = Integer.parseInt(args[1]);
			noReducers = Integer.parseInt(args[2]);
		}
		/*else	{
			System.out.println("Correct Usage: hadoop jar test.jar org.hadoop.sbu.assignment.EstimateReachables filename k reducers");
			System.out.println("----- where filename - name of file on hdfs in /user/nitin/source folder");
			System.out.println("------      k  - final value of K that starts from 1 (multiplied by factor of 4)");
			System.out.println("------      reducers  - number of reducers");
			System.out.println("------      Outputs are generated on hdfs in /user/nitin/output folder");
		}*/
		int res = ToolRunner.run(new Configuration(), new EstimateReachables(),
				args);
		System.exit(res);
	}
}