package net.nuttle.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Simple implementation of Mapper for testing text files.
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
  private static final IntWritable ONE = new IntWritable(1);
  private Text word = new Text();
  
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    while (itr.hasMoreTokens()) {
      String s = (String) itr.nextToken();
      word.set(s);
      context.write(word, ONE);
    }
  }
}
