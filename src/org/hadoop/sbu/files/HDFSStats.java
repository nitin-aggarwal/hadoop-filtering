package org.hadoop.sbu.files;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSStats {

	public static String getUserName(){
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
		Path path = fs.getHomeDirectory();
		return path.getName();
		
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		getUserName();
	}
}
