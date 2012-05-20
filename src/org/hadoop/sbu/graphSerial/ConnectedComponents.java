package org.hadoop.sbu.graphSerial;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ConnectedComponents {

	public static String[] filenames = {"edges","edges1","Graph1","Graph2","Graph3","Graph4"};
	static Set<String> nodes = new HashSet<String>();
	static long nodeCount = 0;
	static long edges = 0;
	static long components = 0;
	static long iterationList = 0;
	static BufferedWriter stats = null;
	
	private static void reset()	{
		nodeCount = 0;
		edges = 0;
		components = 0;
		iterationList = 0;
	}
	
	// Map to hold the info about nodes 
	static Map<Integer, Set<String>> ConnectedComp = new HashMap<Integer, Set<String>>(); 
	
	static BufferedWriter bw = null;
	
	static TreeMap<String, Long> weightSet = new TreeMap<String, Long>();
	static ValueComparator vc = new ValueComparator(weightSet);
	static TreeMap<String, Long> weightSortedSet = new TreeMap<String, Long>(vc);
	
	private static void findNodeCount(String filename)	{
		File f = new File("inputFiles/"+filename); 
		File output = new File("output/CC-"+filename);
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(f));
			BufferedWriter copy = new BufferedWriter(new FileWriter(output));
			String line;
			while((line = br.readLine()) != null)	{
				String[] data = line.split("\t");
				nodes.add(data[1]);
				nodes.add(data[2]);
				edges++;
				copy.write(line);
				copy.write("\n");
			}
			copy.close();
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		
		nodeCount =  nodes.size();
		nodes.clear();
	}
	
	public static void reduce() {
		
		weightSortedSet.putAll(weightSet);
		ConnectedComp.clear();
		
		for (String val : weightSortedSet.keySet()) {
			long weight = weightSortedSet.get(val);
			// boolean values to check if the two nodes belong to the same
			// tree, useful for cycle detection
			boolean ignoreEdgeSameSet1 = false;
			
			// split the srcDestination pair, to get the nodes of the edge
			String[] srcDest = val.toString().split("\t");
			String src = srcDest[0];
			String dest = srcDest[1];
			
			Set<String> nodesSet = new HashSet<String>();
			nodesSet.add(src);
			nodesSet.add(dest);

			// check conditions to ignore the edge
			ignoreEdgeSameSet1 = unionSet(nodesSet, src, dest);
			
			// if all the following three boolean values are false, then
			// adding the edge to the tree will not form a cycle
			if (!ignoreEdgeSameSet1) {
				try	{
					bw.write(weight+"\t"+val);
					bw.write("\n");
					edges++;
				}
				catch(IOException e)	{
					e.printStackTrace();
				}
			}
			
		}
		weightSet.clear();
		weightSortedSet.clear();
		components = ConnectedComp.size();
		
		System.gc();
	}

	// method to unite the set of the two nodes - node1 and node2, this is
	// useful to add edges to the tree without forming cycles
	private static boolean unionSet(Set<String> nodesSet, String node1,String node2) {
		boolean ignoreEdge = false;

		int fnode = Integer.valueOf(node1);
		int snode = Integer.valueOf(node2);

		int repNode1 = getComponent(fnode);
		int repNode2 = getComponent(snode);

		repNode1 = repNode1 != Integer.MAX_VALUE ? repNode1 : fnode;
		repNode2 = repNode2 != Integer.MAX_VALUE ? repNode2 : snode;
		int smallest, larger;
		if (repNode1 < repNode2) {
			smallest = repNode1;
			larger = repNode2;
		} 
		else {
			smallest = repNode2;
			larger = repNode1;
		}
		ignoreEdge = ConnComponent(smallest, larger, nodesSet);
		return ignoreEdge;

	}

	private static boolean ConnComponent(int smallest, int larger,
			Set<String> nodesSet) {
		boolean ignoreEdge = false;
		if (!ConnectedComp.containsKey(smallest) && !ConnectedComp.containsKey(larger)) {
			ConnectedComp.put(smallest, nodesSet);
		} 
		else if (ConnectedComp.containsKey(smallest)) {
			if (ConnectedComp.get(smallest).contains(String.valueOf(larger))) {
				ignoreEdge = true;
			} 
			else if (ConnectedComp.containsKey(larger)) {
				Set<String> nodeLarger = ConnectedComp.get(larger);
				ConnectedComp.remove(larger);
				ConnectedComp.get(smallest).addAll(nodeLarger);
			} 
			else {
				ConnectedComp.get(smallest).addAll(nodesSet);
			}
		} 
		else if (ConnectedComp.containsKey(larger)) {
			ConnectedComp.put(smallest, nodesSet);
			Set<String> nodeLarger = ConnectedComp.get(larger);
			ConnectedComp.remove(larger);
			ConnectedComp.get(smallest).addAll(nodeLarger);
		}
		return ignoreEdge;
	}

	private static int getComponent(int node) {
		Set<Integer> keySet = ConnectedComp.keySet();
		for (int key : keySet) {
			Set<String> nodesSet = ConnectedComp.get(key);
			if (nodesSet.contains(String.valueOf(node))) {
				return key;
			}
		}
		return Integer.MAX_VALUE;
	}	
	private static void iterate(String input, String output)	{
		File inputFile = new File("output/CC-"+input);
		File outputFile = new File("output/CC-"+output);
		BufferedReader br = null;
		long count = 0;
		try	{
			iterationList = 0;
			bw = new BufferedWriter(new FileWriter(outputFile));
			br = new BufferedReader(new FileReader(inputFile));
			 String line;
			 while((line = br.readLine()) != null)	{
				 String[] data = line.split("\t");
				 weightSet.put(data[1]+"\t"+data[2], Long.parseLong(data[0]));
				 count++;
				 if(count == 1.3*nodeCount)	{
					 reduce();
					 iterationList++;
					 stats.write("Reduce Iteration: "+iterationList+" Nodes Processed: "+count+"\n");
					 count = 0;
				 }
			 }
			 if(count > 0){
				 reduce();
				 iterationList++;
			 }
			 bw.close();
		}
		catch(IOException e)	{
			e.printStackTrace();
		}
		inputFile.delete();
	}
	private static void writeComponents(String filename)	{
		File path = new File("output/CC-"+filename+"-comp");
		BufferedWriter bw;
		try {
			bw = new BufferedWriter(new FileWriter(path));
			for (int comp : ConnectedComp.keySet()) {
				bw.write(comp+"\t"+ConnectedComp.get(comp));
				bw.write("\n");
			}
			bw.flush();
			bw.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void writeComponentEdges(String filename, long iteration) throws FileNotFoundException	{
		File path = new File("output/CC-"+filename+"-Compedges");
		File read = new File("output/CC-"+filename+"-"+iteration);
		BufferedWriter bw = null;
		BufferedReader br = null;
		String line = null;
		try {
			bw = new BufferedWriter(new FileWriter(path));
			for (Integer comp : ConnectedComp.keySet()) {
				br = new BufferedReader(new FileReader(read));
				while((line = br.readLine()) != null)	{
					String[] info = line.split("\t");
					Integer node = Integer.parseInt(info[1]);
				
					if(ConnectedComp.get(comp).contains(node.toString())){
						bw.write(comp+"\t"+info[1]+"\t"+info[2]+"\n");
					}
				}
			}
			bw.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws InterruptedException, IOException {
		// TODO Auto-generated method stub
		for(String filename:filenames)	{
			stats = new BufferedWriter(new FileWriter("stats/CC-Stats",true));
			reset();
			long iteration = 1;
			System.out.println("***** Processing File: "+filename+" ****************"+"\n");
			stats.write("***** Processing File: "+filename+" ****************"+"\n");
			Calendar start = Calendar.getInstance();
			findNodeCount(filename);
			stats.write("Nodes: "+nodeCount+" Initial Edges: "+edges+"\n");
			long lastIterationEdges = 0;
			
			while(true)	{
				edges = 0;
				if(iteration <= 1)
					iterate(filename,filename+"-"+iteration);
				else
					iterate(filename+"-"+(iteration-1),filename+"-"+iteration);
				
				stats.write("Iteration: "+iteration+" MST Edges: "+edges+" MSTs: "+components+"\n");
				File del = new File(filename+"-"+(iteration-1));
				if(del.exists())
					del.delete();
				if(lastIterationEdges == edges || iterationList == 1)
					break;
				lastIterationEdges = edges;
				iteration++;
			}
			writeComponents(filename);
			writeComponentEdges(filename, iteration);

			Calendar end = Calendar.getInstance();
			stats.write("Total iterations: "+iteration+"\n");
			stats.write("Time Taken: "+(end.getTimeInMillis()-start.getTimeInMillis())/1000.0+" seconds\n");
			stats.close();
		}
	}
}
