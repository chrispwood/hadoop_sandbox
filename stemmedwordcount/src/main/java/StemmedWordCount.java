/**
 * @author cwood
 * @credit:
 *   http://thekandyancode.wordpress.com/2013/02/04/tokenizing-stopping-and-stemming-using-apache-lucene/
 *   org.apache.hadoop.examples
 * Here I have derived 
 */
package org.wood.hadoop.examples;


import java.io.IOException;
import java.io.StringReader;
import java.net.URL;
import java.util.StringTokenizer;

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StemmedWordCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      TokenStream tokenStream = new StandardTokenizer(
                Version.LUCENE_36, new StringReader(value.toString()));
      tokenStream = new LowerCaseFilter(tokenStream);
      tokenStream = new PorterStemFilter(tokenStream);
 
      StringBuilder sb = new StringBuilder();
      OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);
      CharTermAttribute charTermAttr = tokenStream.getAttribute(CharTermAttribute.class);
      try{
            while (tokenStream.incrementToken()) {
                word.set(charTermAttr.toString());
                context.write(word, one);
            }
      }
      catch (IOException e){
            System.out.println(e.getMessage());
      }

    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      if(sum>4) {
        context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: stemwordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "stemming word count");
    job.setJarByClass(StemmedWordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
