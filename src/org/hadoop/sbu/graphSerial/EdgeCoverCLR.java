package org.hadoop.sbu.graphSerial;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashSet;

// Edge Cover Algorithm using CLR based greedy Algorithm
public class EdgeCoverCLR	{

	public static String[] filenames = {"Graph4"};//edges","edges1","Graph1","Graph2","Graph3","Graph4"};
	static HashSet<Integer> vertex = new HashSet<Integer>();
	static BufferedWriter stats = null;
	
	/** Main driver for maximum matching MapReduce program */
	private static void compute(String filename) throws IOException{

		String input = "inputFiles/"+filename;
		BufferedReader br = null;
		BufferedWriter bw = new BufferedWriter(new FileWriter("output/ECCLR-"+filename));
		System.out.println("***** Processing File: "+filename+" ****************"+"\n");
		stats.write("***** Processing File: "+filename+" ****************"+"\n");
		long ecEdges = 0;
		Calendar start = Calendar.getInstance();
		
		// Computes maximum matching
		String line;
		br = new BufferedReader(new FileReader(input));
		while((line = br.readLine()) != null)	{
			String[] nodes =  line.split("\t");
			if(!vertex.contains(nodes[1]) && !vertex.contains(nodes[2]))	{
				vertex.add(new Integer(nodes[1]));
				vertex.add(new Integer(nodes[2]));
				bw.write(line+"\n");
				ecEdges++;
			}
		}
		br.close();
		System.out.println("Maximal Matching completed\n");
		// Add edges of uncovered vertices, for the edge cover
		br = new BufferedReader(new FileReader(input));
		long p = 0;
		while((line = br.readLine()) != null)	{
			String[] nodes = line.split("\t");
			p++;
			if(!vertex.contains(nodes[1]) || !vertex.contains(nodes[2]))	{
				vertex.add(new Integer(nodes[1]));
				vertex.add(new Integer(nodes[2]));
				bw.write(line+"\n");
				ecEdges++;
			}
			if(p%100000 == 0)
				System.out.println(p);
		}
		bw.close();
		Calendar end = Calendar.getInstance();
		stats.write("Edge Cover Edges: "+ecEdges+"\n");
		stats.write("Time Taken: "+(end.getTimeInMillis() - start.getTimeInMillis())/1000.0+" seconds\n");
		vertex.clear();
	}

	public static void main(String[] args) throws Exception {
		for(String filename: filenames){
			stats = new BufferedWriter(new FileWriter("stats/ECCLR-Stats",true));
			compute(filename);
			stats.close();
		}
	}
}

