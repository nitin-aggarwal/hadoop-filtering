package org.hadoop.sbu.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class MSTInput {

	public static void generate(String in, String out)	{
		File input = new File("inputFiles/"+out);
		File f = new File("assignment/"+in);
		try {
			BufferedReader br = new BufferedReader(new FileReader(f));
			BufferedWriter bw = new BufferedWriter(new FileWriter(input));
			String line;
			while((line=br.readLine()) != null)	{
				String[] nodes = line.split("\\s");
				bw.write(1+"\t"+nodes[0]+"\t"+nodes[1]);
				bw.write("\n");
			}
			bw.close();
		} 
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		generate("test-04-in.txt","Graph4");
		
	}

}
