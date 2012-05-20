package org.hadoop.sbu.files;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ListAllFiles {

	/**
	 * List all the folders in user directory and their immediate contents
	 * @param user
	 */
	public static void listFiles(String user)	{
		String dir = "/user/"+user+"/*/*";
		FileSystem fs;
		Configuration conf = new Configuration();
		
		try {
			fs = FileSystem.get(URI.create(dir), conf);
			Path path = new Path(dir);
			
			FileStatus[] fstatus = fs.globStatus(path);
			Path[] paths = FileUtil.stat2Paths(fstatus);
			for(Path p: paths)
				System.out.println(p);
			
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		listFiles("nitin");
	}
}
