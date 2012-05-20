package org.hadoop.sbu.files;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class CopyFromLocalWithProgress {

	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void copy(String file, String folder)	{
		String path = "inputFiles/";
		String input = file;
		String output = "/user/nitin/"+folder+"/"+input;
		try {
			InputStream is = new BufferedInputStream(new FileInputStream(path+input));
			Configuration conf = new Configuration(); 
			
			FileSystem fs = FileSystem.get(URI.create(output),conf);
			
			/*
			 * create function would automatically create the required directories, if does not exist.
			 * Otherwise, explicit creation is also possible through
			 * FileSystem's mkdirs(Path p) function
			 */
			OutputStream out = fs.create(new Path(output), new Progressable() {
				
				@Override
				public void progress() {
					// TODO Auto-generated method stub
					System.out.print(".");
				}
			});
			
			IOUtils.copyBytes(is, out, 4096,false);
			IOUtils.closeStream(is);
			IOUtils.closeStream(out);
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args){
		// TODO Auto-generated method stub
	
		File f = new File("inputFiles");
		for(String str: f.list())
			copy(str,"source");
	}
}