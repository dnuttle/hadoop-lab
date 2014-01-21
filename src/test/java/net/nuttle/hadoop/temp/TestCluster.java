package net.nuttle.hadoop.temp;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import net.nuttle.hadoop.WordCountMapper;
import net.nuttle.hadoop.WordCountReducer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Simple class to confirm that cluster is started and HDFS can be read/written in ClusterMapReduceTestCase.
 * Plus some earlier failed attempts to start up a cluster.
 */
public class TestCluster {
  
  private static final int DFS_REPLICATION_INTERVAL = 1;
  private static final int DATA_NODE_COUNT = 1;
  private static final Path TEST_ROOT_DIR_PATH = new Path(System.getProperty("test.build.data", 
    "/tmp/hadoop-test/test/build/data"));
  private static final int DFS_BLOCK_SIZE = 100;
  private static final int BUFFER_SIZE = 1024;
  private static JobConf jobConf;
  private static Configuration conf;
  
  private MiniDFSCluster dfsCluster;
  
  @Test 
  public void run() throws Exception {
    Path inputPath = new Path("/tmp/mrtest/input");
    Path outputPath = new Path("/tmp/mrtest/output");
    startCluster(true, null);
    //Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "local");
    //conf.set("fs.default.name", "file:///");
    conf.set("key.value.separator.in.input.line", "|");
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(inputPath);
    String input = "foo bar\none two";
    DataOutputStream file = fs.create(new Path(inputPath, "part-0"));
    file.writeBytes(input);
    file.close();
    file = fs.create(new Path(inputPath, "part-1"));
    file.writeBytes("abc def\nghi jkl");
    file.close();
    Job job = runJob(conf, inputPath, outputPath, KeyValueTextInputFormat.class);
    assertTrue(job.isSuccessful());
    DataInputStream in = fs.open(new Path("/tmp/mrtest/output/part-r-00000"));
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String line;
    while ((line = reader.readLine()) != null) {
      System.out.println(line);
    }
    reader.close();
    FileStatus[] statuses = fs.listStatus(outputPath);
    for (FileStatus status : statuses) {
      System.out.println(status.getPath().getName());
    }
    stopCluster();
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
  
  @Test
  public void test2() throws Exception {
    startCluster(true, null);
    FileSystem fs = dfsCluster.getFileSystem();
    DataOutputStream file = fs.create(new Path("/user/test/out.txt"));
    file.writeBytes("ABCDEF\nDEFGHI\nJKLMNOP");
    file.close();
    DataInputStream in = fs.open(new Path("/user/test/out.txt"));
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    String line;
    while ((line = reader.readLine()) != null) {
      System.out.println(line);
    }
    in.close();
    stopCluster();
  }
  
  protected synchronized void startCluster(boolean reformatDFS, Properties props) throws Exception {
    if (dfsCluster == null) {
      jobConf = new JobConf();
      conf = (Configuration) jobConf;
      if (props != null) {
        for (Map.Entry entry : props.entrySet()) {
          conf.set((String) entry.getKey(), (String) entry.getValue());
        }
      }
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(reformatDFS).racks(null).build();
    }
  }

  protected void stopCluster() throws Exception {
    /*
    if (mrCluster != null) {
      mrCluster.shutdown();
      mrCluster = null;
    }
    */
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }  
  
  //@Test
  //Never got this to work, set startCluster method instead
  public void test() throws Exception {
    MiniDFSCluster cluster;
    DistributedFileSystem fs;
    FSNamesystem namesystem;
    Configuration conf2 = new HdfsConfiguration();
    conf2.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE);
    conf2.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);
    conf2.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, DFS_REPLICATION_INTERVAL);
    cluster = new MiniDFSCluster(conf2, DATA_NODE_COUNT, StartupOption.REGULAR);
    cluster.waitActive();
    namesystem = cluster.getNamesystem();
    fs = (DistributedFileSystem) cluster.getFileSystem();
    Path p = new Path(TEST_ROOT_DIR_PATH, "test.txt");
    FSDataOutputStream os = fs.create(p);
    os.writeChars("ABCDEF");
    os.flush();
    os.close();
    
    FSDataInputStream fis = fs.open(p, BUFFER_SIZE);
    InputStreamReader isr = new InputStreamReader(fis);
    BufferedReader br = new BufferedReader(isr);
    String line;
    StringBuilder sb = new StringBuilder();
    while ((line = br.readLine()) != null) {
      sb.append(line);
    }
    assertEquals("ABCDEF", sb.toString());
    System.out.println(sb.toString());
    cluster.shutdown();
    
  }
}
