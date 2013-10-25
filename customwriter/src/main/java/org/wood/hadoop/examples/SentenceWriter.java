package org.wood.hadoop.examples;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;

public class SentenceWriter implements Writable {

	private final String pattern = "\\b\\w+\\b";

	private Pattern p;
	private int sentenceCount;
	private int wordCount;

	public SentenceWriter() {
		p = Pattern.compile(pattern);
		sentenceCount = 0;
		wordCount = 0;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sentenceCount = in.readInt();
		wordCount = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(sentenceCount);
		out.writeInt(wordCount);
	}

	public int getSentenceCount() { return sentenceCount; }
	public int getWordCount() { return wordCount; }

	@Override
	public String toString() {
		return sentenceCount + " : " + wordCount + " : " + wordCount/sentenceCount;
	}
}