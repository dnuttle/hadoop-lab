package net.nuttle.hadoop;

import java.io.DataOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import net.nuttle.hadoop.WordCountMapper;
import net.nuttle.hadoop.WordCountReducer;

import static org.junit.Assert.assertTrue;

/**
 * This is yet another way of running a MR job in isolation.  From Hadoop in Action.
 * This uses the local filesystem 
 */
public class LocalMapReduceTest {
  
  @Test 
  public void run() throws Exception {
    Path inputPath = new Path("/tmp/mrtest/input");
    Path outputPath = new Path("/tmp/mrtest/output");
    Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "local");
    conf.set("fs.default.name", "file:///");
    conf.set("key.value.separator.in.input.line", "|");
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    if (fs.exists(inputPath)) {
      fs.delete(inputPath, true);
    }
    fs.mkdirs(inputPath);
    String input = "foo bar\none two";
    DataOutputStream file = fs.create(new Path(inputPath, "part-" + 0));
    file.writeBytes(input);
    file.close();
    file = fs.create(new Path(inputPath, "part-1"));
    file.writeBytes("abc def\nghi jkl");
    file.close();
    Job job = runJob(conf, inputPath, outputPath, KeyValueTextInputFormat.class);
    assertTrue(job.isSuccessful());
    //List<String> lines = IOUtils.readLines(fs.open(new Path(outputPath, "part-r-00000")));
    
  }
  
  public Job runJob(Configuration conf, Path inputPath, Path outputPath, 
    Class<? extends InputFormat> inputFormat) 
    throws Exception {
    Job job = new Job(conf);
    job.setJarByClass(WordCountMapper.class);
    job.setInputFormatClass(inputFormat);
    //job.setMapOutputKeyClass(Text.class);
    
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //job.setOutputFormatClass(TextOutputFormat.class);
    

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.waitForCompletion(false);
    return job;
  }

}
