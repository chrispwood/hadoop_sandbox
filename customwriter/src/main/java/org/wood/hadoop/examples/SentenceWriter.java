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
	private final Pattern p;
	private final int sentenceCount;

	private String sentence;
	private int wordCount;

	{
		p = Pattern.compile(pattern);
		sentenceCount = 1;
		wordCount = 0;
	}

	public SentenceWriter() {
		this.sentence = null;
	}

	public SentenceWriter(String sentence) {
		this.sentence = sentence.trim();
		calculateWordCount();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sentence = in.readLine().trim();
		wordCount = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChars(sentence+"\n");
		out.writeInt(wordCount);
	}

	public int getSentenceCount() { return sentenceCount; }
	public int getWordCount() { return wordCount; }

	@Override
	public String toString() {
		return sentenceCount + " : " + wordCount + " : " + wordCount/sentenceCount;
	}

	private void calculateWordCount() {
		Matcher m = p.matcher(sentence);
		wordCount = 0;
		while(m.find()) {
			wordCount++;
		}
	}
}