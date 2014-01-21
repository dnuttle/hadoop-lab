package net.nuttle.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Simple attempt to write to HDFS.
 * But I think I need to have hadoop xml config files in path.
 */
public class HdfsIOTest {

  @Test
  public void test() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path p = new Path("/tmp/tmp.txt");
    System.out.println(fs.isFile(p));
    FSDataOutputStream s = fs.create(p);
    s.writeChars("ABCDEF");
    s.close();
    System.out.println(fs.isFile(p));
  }
}
