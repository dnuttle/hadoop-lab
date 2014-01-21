package net.nuttle.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MR1WordCountMapper extends MapReduceBase 
implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
private static final IntWritable ONE = new IntWritable(1);
private Text word = new Text();

public void map(LongWritable key, Text val, OutputCollector<Text, IntWritable> output, Reporter reporter) 
  throws IOException {
  String line = val.toString();
  StringTokenizer tokenizer = new StringTokenizer(line);
  while (tokenizer.hasMoreTokens()) {
    word.set(tokenizer.nextToken());
    output.collect(word, ONE);
  }
}

}

