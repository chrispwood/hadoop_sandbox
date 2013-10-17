package org.wood.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Scanner {

	public static void main(String[] args) {

		if(args.length<1) {
			System.err.println("Usage. Expected at least one path");
			return;
		}

		Configuration conf = new Configuration();

		/* note that if we wanted to read from hdfs, 
			we would use the factory method:
			FileSystem.get(conf); */

		FileSystem local = null;
		try {
			local = FileSystem.getLocal(conf);
		}
		catch(IOException ioe) {
			System.err.println("Error. Unable to open the local filesystem:");
			ioe.printStackTrace();
		}

		if(local!=null) {

			for(int i=0; i<args.length; i++) {
				Path inputDir = new Path(args[0]);

				try {
					FileStatus[] inputFiles = local.listStatus(inputDir);

					for(int j=0; j<inputFiles.length; j++) {
						FileStatus fstatus = inputFiles[j];
						System.out.println(fstatus.getPath().toString());
						System.out.println("  Group: " + fstatus.getGroup());
						System.out.println("  Owner: " + fstatus.getOwner());
						System.out.println("  Access time: " + fstatus.getAccessTime());
						System.out.println("  Mod time: " + fstatus.getModificationTime());
						System.out.println("  Block size: " + fstatus.getBlockSize());
						System.out.println("  Repl: " + fstatus.getReplication());
					}
				}
				catch(IOException ioe) {
					System.err.println("Error. Unable to read the local path: " + inputDir + ":");
					ioe.printStackTrace();
				}
			}

		}
	}
}