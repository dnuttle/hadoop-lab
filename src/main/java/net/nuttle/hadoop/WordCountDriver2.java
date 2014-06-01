package net.nuttle.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver2 extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new WordCountDriver2(), args);
    System.exit(result);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    Job job = new Job(conf, "My Job");
    
    job.setJarByClass(WordCountDriver2.class);
    
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job,  new Path(args[0]));
    
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
  
  public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text();
    private IntWritable one = new IntWritable(1);
    @Override
    public void map(LongWritable key, Text value, Context context) 
      throws InterruptedException, IOException {
      StringTokenizer tokenizer = new StringTokenizer(value.toString());
      while(tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable sum = new IntWritable();
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws InterruptedException, IOException {
      int total = 0;
      for(IntWritable value: values) {
        total += value.get();
      }
      sum.set(total);
      context.write(key, sum);
    }
  }
}
