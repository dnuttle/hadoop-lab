package net.nuttle.hadoop;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;

import net.nuttle.hadoop.MR1WordCountMapper;
import net.nuttle.hadoop.MR1WordCountReducer;
import net.nuttle.hadoop.WordCountMapper;
import net.nuttle.hadoop.WordCountReducer;
//import org.apache.hadoop.mapred.Mapper;


/**
 * Some failed attempts to run an MR job in ClusterMapReduceTestCase
 */
public class ClusterMRTest extends ClusterMapReduceTestCase {
  
  private static final Logger LOG = Logger.getLogger(ClusterMRTest.class);

  /**
   * This doesn't work either.  Verbose output shows that mapper class is not found.
   * MR2 mapper and reducer
   * @throws Exception
   */
  public void notWorking2() throws Exception {
    JobConf conf =  createJobConf();
    Job job = new Job((Configuration) conf);
    FileSystem fs = this.getFileSystem();
    FileSystem f2 = FileSystem.get(conf);
    assertTrue(fs.equals(f2));
    Path inDir = new Path("/users/test/jobconf/input");
    Path outDir = new Path("/users/test/jobconf/output");
    OutputStream os = fs.create(new Path(inDir, "part-0"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("b a one two");
    wr.close();
    assertTrue(fs.exists(new Path(inDir, "part-0")));
    
    job.setJobName("mrzz");
    job.setJarByClass(WordCountMapper.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);
    
    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job,  inDir);
    org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outDir);
    assertTrue(job.waitForCompletion(true));
    
  }
  
  /**
   * An attempt to run an MR job.  Doesn't work because mapper class not found.
   * Apparently the mapper and reducer must be in a separate jar that is on the classpath.
   * MR1 mapper and reducer
   * @throws IOException
   */
  public void notWorking() throws IOException {
    JobConf conf =  createJobConf();
    conf.setJarByClass(MR1WordCountMapper.class);
    FileSystem fs = this.getFileSystem();
    Path inDir = new Path("/users/test/jobconf/input");
    Path outDir = new Path("/users/test/jobconf/output");
    OutputStream os = fs.create(new Path(inDir, "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("b a\n");
    wr.close();
    assertTrue(fs.exists(new Path(inDir, "text.txt")));
    
    conf.setJobName("mr");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);
    conf.setMapperClass(MR1WordCountMapper.class);
    conf.setReducerClass(MR1WordCountReducer.class);
    
    FileInputFormat.setInputPaths(conf,  inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    
    assertTrue(JobClient.runJob(conf).isSuccessful());
  }

}
