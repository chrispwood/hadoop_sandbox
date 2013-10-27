package org.wood.hadoop.examples;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;


public class AverageSentenceLength {
	

	public static class SentenceMapper
		extends Mapper<Object, Text, CustomFilename, SentenceWriter> {

		private static final String pattern = "(.*?)[\\.!?]";
		private Pattern p;

		public SentenceMapper() {
			p = Pattern.compile(pattern);
		}

		public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			Matcher m = p.matcher(value.toString());
			String filename = fileSplit.getPath().getName();
			CustomFilename cf = new CustomFilename(filename);
			//Text txt = new Text(filename);

			while(m.find()) {
				SentenceWriter sw = new SentenceWriter(m.group(0));
				//sw.addSentence(m.group(0));
				//context.write(txt, sw);
				context.write(cf, sw);
			}
		}
	}	

	public static class AverageSentenceReducer
		extends Reducer<CustomFilename, SentenceWriter, CustomFilename, DoubleWritable> {

		private DoubleWritable dw = new DoubleWritable();

		public void reduce(CustomFilename key, Iterable<SentenceWriter> sentences,
			Context context) throws IOException, InterruptedException {

			int sentenceCount = 0;
			int wordCount = 0;
			for(SentenceWriter sentence : sentences) {
				sentenceCount++;
				wordCount += sentence.getWordCount();
			}

			dw.set(wordCount / (double)sentenceCount);
			context.write(key, dw);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: AverageSentenceLength <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "calculating average number of words in a sentence");
		job.setJarByClass(AverageSentenceLength.class);
		job.setMapperClass(SentenceMapper.class);

		/* Note: in a job configuration, this defines the expected 
			*mapper* output, not the reducer output */
		//job.setOutputKeyClass(Text.class);
		job.setOutputKeyClass(CustomFilename.class);
		job.setOutputValueClass(SentenceWriter.class);

		/* If we wanted to reduce the number of key/value pairs
			sent to the reduce stage, we might write a combiner
			that receives the Map K/V and emits the Reducer K/V:
			job.setCombinerClass(AverageSentenceReducer.class);
		*/
		job.setReducerClass(AverageSentenceReducer.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
