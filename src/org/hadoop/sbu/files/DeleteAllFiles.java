package org.hadoop.sbu.files;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DeleteAllFiles {

	/**
	 * Delete all the files in the folder for the user
	 * @param user
	 * @param folder
	 */
	public static void deleteFolder(String user, String folder)	{
		String dir = "/user/"+user+"/"+folder;
		Path p = new Path(dir);
		
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(conf);
			System.out.println(fs.delete(p, true));
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Delete the file from the provided folder, for the specified user
	 * @param user
	 * @param folder
	 * @param file
	 */
	public static void deleteFile(String user, String folder,String file)	{
		String dir = "/user/"+user+"/"+folder+"/"+file;
		Path p = new Path(dir);
		
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(conf);
			System.out.println(fs.delete(p, true));
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		deleteFolder("nitin", "vertex");
	}

}
