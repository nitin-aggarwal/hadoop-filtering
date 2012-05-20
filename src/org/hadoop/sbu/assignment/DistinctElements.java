package org.hadoop.sbu.assignment;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hadoop.sbu.files.DeleteAllFiles;

public class DistinctElements extends Configured implements Tool {

	static String filename = "test-02-in.txt";
	static int limitK = 2;
	
	static class MapClass extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		long L = 32;
		long kMapper;
		
		public long hash(long value)
		{
			long hashValue = 0;
			long a = 1073741827L;
			long b = 17179869209L;
			long q = 4294967291L;
			hashValue = (a*value + b) % q;
			return hashValue;
		}
		public long compute(long value)
		{
			if(value == 0)
				return L;
			else
			{
				long bit = 1;
				long result;
				for(long i=0;i<32;i++)
				{
					result = value & (bit << i);
					if(result > 0)
						return i;
				}
			}
			return -1;
		}
		public void setup(Context context)	{
			Configuration conf = context.getConfiguration();
			kMapper = Integer.parseInt(conf.get("K"));
		}
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			long k,l,temp;
			long hashValue;
			Long num = new Long(value.toString());
			hashValue = hash(num);
			k = hashValue % kMapper;
			temp = hashValue / kMapper;
			l = compute(temp);
			
			context.write(new LongWritable(k), new Text( String.valueOf(l)));
		}
	}

	static class ReduceClass extends
			Reducer<LongWritable, Text, LongWritable, Text> {
		static double sGlobal = 0;
		static long kProcessed = 0;
		long kReducer;
		double m;
		public long computeMin(long value)
		{
				long bit = 1;
				long result;
				for(int i=0;i<32;i++)
				{
					result = value & (bit << i);
					if(result == 0)
						return i;
				}
			
			return -1;
		}
		public void setup(Context context)	{
			Configuration conf = context.getConfiguration();
			kReducer = Integer.parseInt(conf.get("K"));
		}
		public void cleanup(Context context)	{
			try {
				context.write(new LongWritable(kReducer),new Text((long)Math.floor(m)+""));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
					long value = 0;
					for(Text val : values){
						Integer temp = Integer.parseInt(val.toString());
						if(temp >= 0)
							value = value | (1 << temp); 
					}
					long r = computeMin(value);
					sGlobal += r;
					kProcessed++;
					m = (kReducer/0.77351) * Math.pow(2, sGlobal/kReducer);
			}
	}

	static int printUsage() {
		System.out
				.println("DistinctElements");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	private Job getJob(String[] args, int K) throws IOException {

		Configuration conf = new Configuration();
		conf.setInt("K", K);
		Job job = new Job(conf);
		job.setJobName("distinct");
		job.setNumReduceTasks(1);

		job.setJarByClass(DistinctElements.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		return job;
	}

	/**
	 * The main driver for distinct elements map/reduce program. Invoke this method to
	 * submit the map/reduce job.
	 */
	public int run(String[] args) throws Exception {
		int K = 1;
		
		while(K <= limitK)
		{
			String input = "source/"+filename;
			String output = "distinct/distinct-" + K;
	
			Job job = getJob(args,K);
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
			K = K * 2;
		}
		
		// Combine all estimates for different K values
		Configuration conf = new Configuration();
		String path="output/distinct-"+filename;
		FileSystem fs1 = FileSystem.get(URI.create(path),conf);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs1.create(new Path(path),true)));
		for(int i=1;i<limitK;i*=2)	{
			String fileName = "distinct/distinct-" + i+"/part-r-00000";
			FileSystem fs = FileSystem.get(URI.create(fileName),conf);
			BufferedReader fin = new BufferedReader(new InputStreamReader(fs.open(new Path(fileName))));
			String line;
			
			while((line = fin.readLine())!= null ){
				bw.write(line);
				bw.write("\n");
				bw.flush();
			}
			DeleteAllFiles.deleteFile("hduser","distinct","distinct-"+i);
		}
		bw.flush();
		bw.close();
		DeleteAllFiles.deleteFolder("hduser","distinct");
		return 0;
	}

	
	public static void main(String[] args) throws Exception {
		if(args.length >= 2)	{
			filename = args[0];
			limitK = Integer.parseInt(args[1]);
		}
		else	{
			System.out.println("Correct Usage: hadoop jar test.jar org.hadoop.sbu.assignment.DistinctElements filename k");
			System.out.println("----- where filename - name of file on hdfs in /user/hduser/source folder");
			System.out.println("------      k  - final value of K that starts from 1 (multiplied by factor of 2)");
			System.out.println("------      Outputs are generated on hdfs in /user/hduser/output folder");
		}
		int res = ToolRunner.run(new Configuration(), new DistinctElements(),
				args);
		System.exit(res);
	}
}