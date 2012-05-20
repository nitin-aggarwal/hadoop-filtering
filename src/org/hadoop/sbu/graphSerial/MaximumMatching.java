package org.hadoop.sbu.graphSerial;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;

import org.hadoop.sbu.graph.algo.Edmonds;
import org.hadoop.sbu.util.UndirectedGraph;

// Maximum Matching Algorithm using Edmond Karp's Algorithm
public class MaximumMatching	{

	public static String[] filenames = {"edges","edges1","Graph1","Graph2","Graph3","Graph4"};
	public static long blockSize = 7000;
	static UndirectedGraph<String> inGraph = new UndirectedGraph<String>();
	static HashSet<String> vertex = new HashSet<String>();
	static long numEdges = 0;
	static BufferedWriter stats = null;
	
	/** Sample edges on the basis of the input size */
	private static boolean sampling()	{
		double reductionFactor = (double)numEdges/blockSize;
		if(reductionFactor > 1){
			int randNum = (int) Math.floor(Math.random()*reductionFactor);
			if(randNum == 0)
				return true;
		}
		else if(reductionFactor == 1)
			return true;
		return false;
	}

	/** Main driver for maximum matching MapReduce program */
	private static void compute(String filename) throws IOException{

		int iterationCount = 1;
		String input = "inputFiles/"+filename;
		BufferedReader br = null;
		BufferedWriter bw = new BufferedWriter(new FileWriter("output/MM-"+filename));
		stats.write("***** Processing File: "+filename+" ****************"+"\n");
		long edges = 0;
		long sampledEdges = 0;
		long outputEdges = 0;
		long maximalEdges = 0;
		Calendar start = Calendar.getInstance();
		do {
			stats.write("********* Iteration: "+iterationCount+" **********\n");
			String line;
			if(iterationCount == 1)	{
				br = new BufferedReader(new FileReader(input));
				while((line = br.readLine()) != null)
					edges++;
				numEdges = edges;
				br.close();
			}
			else	{
				edges = outputEdges;
				outputEdges = 0;
				numEdges = edges;
			}
				
			stats.write("Total edges: "+edges+"\n");
			// Sampling edges
			br = new BufferedReader(new FileReader(input));
			while((line = br.readLine()) != null)	{
				if((edges < blockSize) || sampling())	{
					sampledEdges++;
					String[] nodes =  line.split("\t");
					inGraph.addNode(nodes[1]);
					inGraph.addNode(nodes[2]);
					//add edge
					inGraph.addEdge(nodes[1], nodes[2]);
				}
			}
			br.close();
			stats.write("Sampled Edges: "+sampledEdges+"\n");
			UndirectedGraph<String> tempOutGraph = new UndirectedGraph<String>();
			tempOutGraph = Edmonds.maximumMatching(inGraph);
			
			// Write computed Maximal Matching to file
			Iterator<String> itr = tempOutGraph.iterator();
			while(itr.hasNext()){
				String node = (String) itr.next();
				for(String nodeDest : tempOutGraph.edgesFrom(node))	{
					if(node.compareTo(nodeDest) < 0)	{
						bw.write(node + "\t" + nodeDest+"\n");
						vertex.add(node);
						vertex.add(nodeDest);
						maximalEdges++;
					}
				}
			}
			bw.flush();
			tempOutGraph.clear();
			stats.write("Maximum Matching Edges: "+maximalEdges+"\n");
			
			// Create new input file, filtering the edges with vertices already covered
			BufferedWriter out = new BufferedWriter(new FileWriter("output/MM-"+filename+"-"+(iterationCount)));
			br = new BufferedReader(new FileReader(input));
			while((line = br.readLine()) != null)	{
				String[] nodes =  line.split("\t");
				if(!vertex.contains(nodes[1]) && !vertex.contains(nodes[2]))	{
					out.write(line+"\n");
					outputEdges++;
				}
			}
			out.close();
			input = "output/MM-"+filename+"-"+(iterationCount);
			File f = new File("output/MM-"+filename+"-"+(iterationCount-1));
			if(f.exists())
				f.delete();
			iterationCount++;
			
			if(edges < blockSize)
				break;
			inGraph.clear();
			sampledEdges = 0;
			System.gc();
			
		} while(true);
		bw.close();
		vertex.clear();
		File f = new File("output/MM-"+filename+"-"+(iterationCount-1));
		if(f.exists())
			f.delete();
		Calendar end = Calendar.getInstance();
		stats.write("Time Taken: "+(end.getTimeInMillis()-start.getTimeInMillis())/1000.0+" seconds\n");
	}

	public static void main(String[] args) throws Exception {
		for(String filename: filenames)	{
			stats = new BufferedWriter(new FileWriter("stats/MM-Stats",true));
			compute(filename);
			stats.close();
		}
	}
}

