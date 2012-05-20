package org.hadoop.sbu.files;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class ReadHdfsFileData {

	/**
	 * Read the contents from the regular file specified for the user
	 * @param user
	 * @param filePath
	 */
	public static void readFile(String user, String filePath)	{
		String path = "/user/"+user+"/"+filePath;
		Configuration conf = new Configuration();
		
		FSDataInputStream in = null;
		
		try {
			FileSystem fs = FileSystem.get(URI.create(path),conf);
			in = fs.open(new Path(path));
			IOUtils.copyBytes(in, System.out, 4096, false);
			
			// Read data from the same file by resetting the pointer
			// in.seek(0);		
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			IOUtils.closeStream(in);	
		}
	}
	
	/**
	 * Read the contents from the sequential file specified for the user
	 * @param user
	 * @param filePath
	 */
	public static void readSeqFile(String user, String filePath)	{
		String path = "/user/"+user+"/"+filePath;
		Configuration conf = new Configuration();
		
		SequenceFile.Reader reader = null;
		
		try {
			FileSystem fs = FileSystem.get(URI.create(path),conf);
			reader = new SequenceFile.Reader(fs,new Path(path),conf);
			
			IntWritable key =  (IntWritable) reader.getKeyClass().newInstance();
			Text val = (Text) reader.getValueClass().newInstance();
			while(reader.next(key,val)){
				System.out.println("key:"+ key +"\t value: " + val );
			}
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * @param args
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void main(String[] args) throws InstantiationException, IllegalAccessException {
		// TODO Auto-generated method stub
		readFile("nitin", "output/distinct-sample-01-in.txt");
		readSeqFile("nitin", "files/centroid1/cen133412569144727");
	}

}