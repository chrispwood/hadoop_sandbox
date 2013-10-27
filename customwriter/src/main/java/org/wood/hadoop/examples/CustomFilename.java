package org.wood.hadoop.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

public class CustomFilename 
		implements WritableComparable<CustomFilename> {

	private String filename;

	public CustomFilename() {
		this.filename = null;
	}

	public CustomFilename(String filename) {
		this.filename = filename.trim();
	}

	public String getFilename() { return filename; }

	@Override
	public void readFields(DataInput in) throws IOException {
		filename = in.readLine().trim();
	}

	@Override 
	public void write(DataOutput out) throws IOException {
		out.writeChars(filename+"\n");
	}

	@Override
	public int compareTo(CustomFilename cmp) {
		return filename.compareTo(cmp.getFilename());
	}

	@Override
	public String toString() { 
		return filename;
	}
}