package org.hadoop.sbu.graphSerial;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.hadoop.sbu.util.Node;
import org.hadoop.sbu.util.Node.Color;

/**
 * Perform Breadth First Search in a serial fashion
 * Designed to work for huge files (by minimizing use of in-memory data structures)
 * @category Input Format:
 * 1	2,3|0|GRAY|
 * NODE	EDGES|DISTANCE|COLOR|
 */
public class BreadthFirstSearch {

	static String filename = "bfs";
	
	static Map<Integer,Integer> newGrayNodes = new HashMap<Integer,Integer>();
	static BufferedWriter result = null;
	
	static long whiteNodesCount = 0;
	
	static	{
		File output = new File("output/BFS-"+filename);
		try {
			result = new BufferedWriter(new FileWriter(output));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Reads the input file, and write all GRAY nodes as BLACK in output file,
	 * updates the newGrayNodes map and write white nodes to another file 
	 */
	private static void readInputPhase()	{
		File input = new File("inputFiles/"+filename);
		File grayNodes = new File("output/"+filename+"-Gray");
		File whiteNodes = new File("output/"+filename+"-White");
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(input));
			BufferedWriter gray = new BufferedWriter(new FileWriter(grayNodes));
			BufferedWriter white = new BufferedWriter(new FileWriter(whiteNodes));
			
			String line;
			while((line = br.readLine()) != null)	{
				Node node = new Node(line);
				
				if(node.getColor() == Color.GRAY)	{
					for(Integer nodeList: node.getEdges())	{
						if(newGrayNodes.containsKey(nodeList))	{
							if(newGrayNodes.get(nodeList) > node.getDistance())
								newGrayNodes.put(nodeList, node.getDistance()+1);
						}
						else	{
							newGrayNodes.put(nodeList, node.getDistance()+1);
						}
					}
					node.setColor(Color.BLACK);
					result.write(node.getId()+"\t"+node.getLine());
					result.write("\n");
				}
				else if(node.getColor() == Color.WHITE)	{
					if(newGrayNodes.containsKey(new Integer(node.getId())))	{
						node.setColor(Color.GRAY);
						node.setDistance(newGrayNodes.get(node.getId()));
						gray.write(node.getId()+"\t"+node.getLine());
						gray.write("\n");
						newGrayNodes.remove(node.getId());
					}
					else	{
						white.write(node.getId()+"\t"+node.getLine());
						white.write("\n");
						whiteNodesCount++;
					}
				}
			}
			gray.close();
			white.close();
		} 
		catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	/**
	 * Reads the gray Nodes file, and write all GRAY nodes as BLACK in output file,
	 * updates the newGrayNodes map 
	 */
	private static void readGrayNodes()	{
		File grayNodes = new File("output/"+filename+"-Gray");
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(grayNodes));
			
			String line;
			while((line = br.readLine()) != null)	{
				Node node = new Node(line);
				
				for(Integer edgeNode: node.getEdges())	{
					if(newGrayNodes.containsKey(edgeNode))	{
						if(newGrayNodes.get(edgeNode) > node.getDistance()+1)
							newGrayNodes.put(edgeNode, node.getDistance()+1);
					}
					else	{
						newGrayNodes.put(edgeNode, node.getDistance()+1);
					}
				}
				node.setColor(Color.BLACK);
				result.write(node.getId()+"\t"+node.getLine());
				result.write("\n");
			}
			result.flush();
		}
		catch(IOException e)	{
			e.printStackTrace();
		}
	}
	
	/**
	 * Reads the white nodes file, and write nodes in map as GRAY in gray nodes file,
	 * remove them from newGrayNodes map and write other nodes as WHITE to another file 
	 */
	private static void readWhiteNodes(String whiteSrc, String whiteDst)	{
		File whiteNodes = new File("output/"+filename+"-"+whiteSrc);
		File grayNodes = new File("output/"+filename+"-Gray");
		File newWhiteNodes = new File("output/"+filename+"-"+whiteDst);
		whiteNodesCount = 0;
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(whiteNodes));
			BufferedWriter gray = new BufferedWriter(new FileWriter(grayNodes));
			BufferedWriter white = new BufferedWriter(new FileWriter(newWhiteNodes));
		
			String line;
			while((line = br.readLine()) != null)	{
				Node node = new Node(line);
				
				if(newGrayNodes.containsKey(new Integer(node.getId())))	{
					node.setColor(Color.GRAY);
					node.setDistance(newGrayNodes.get(node.getId()));
					gray.write(node.getId()+"\t"+node.getLine());
					gray.write("\n");
					newGrayNodes.remove(node.getId());
				}
				else	{
					white.write(node.getId()+"\t"+node.getLine());
					white.write("\n");
					whiteNodesCount++;
				}
			}
			gray.close();
			white.close();
		}
		catch(IOException e)	{
			e.printStackTrace();
		}
	}
	
	/** Delete all the temporary files created during the process */
	private static void cleanup()	{
		File f1 = new File("output/"+filename+"-Gray");
		File f2 = new File("output/"+filename+"-White");
		File f3 = new File("output/"+filename+"-White1");
		System.out.println("Cleanup: "+(f1.delete()|f2.delete()|f3.delete()));
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		long iteration = 0;
		try	{
			readInputPhase();
			readGrayNodes();
			
			while(whiteNodesCount != 0)	{
				if(iteration %2 == 0)
					readWhiteNodes("White","White1");
				else
					readWhiteNodes("White1","White");
				readGrayNodes();
				iteration++;
			}
			result.close();
			System.out.println("No. of Iterations: "+iteration);
			cleanup();
		}
		catch(Exception e)	{
			e.printStackTrace();
			cleanup();
		}
	}
}
