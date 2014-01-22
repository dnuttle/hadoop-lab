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

/**
 * Separate lab in creating an MR driver
 * @author dan
 *
 */
public class WordCountDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new WordCountDriver(), args);
    System.exit(res);
  }
  public int run(String[] args) throws Exception {
    int res = 0;
    Configuration conf = getConf();
    Job job = new Job(conf, "Test Job");
    job.setJarByClass(WordCountDriver.class);
    job.setMapperClass(WCMapper.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    TextInputFormat.addInputPath(job, new Path(args[0]));
    TextOutputFormat.setOutputPath(job, new Path(args[1]));
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
  
  public static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text();
    private IntWritable one = new IntWritable(1);
    @Override
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
      StringTokenizer tokenizer = new StringTokenizer(value.toString());
      while(tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
  }
}
