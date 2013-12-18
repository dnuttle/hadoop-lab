package net.nuttle.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.junit.Test;

/**
 * A simple test to confirm that hadoop is written to, from a local file.
 */
public class ClusterHdfsIOTest extends ClusterMapReduceTestCase {
  
  private static final int BUFFER_SIZE = 1024;

  @Test
  public void test() {
    try {
      FileSystem fs = this.getFileSystem();
      Path src = new Path("/home/dnuttle/test.txt");
      Path dest = new Path("/users/dnuttle/test.txt");
      fs.copyFromLocalFile(src, dest);
      assertTrue(fs.isFile(new Path("/users/dnuttle/test.txt")));
      int bufferSize = BUFFER_SIZE;
      FSDataInputStream is = fs.open(new Path("/users/dnuttle/test.txt"), bufferSize);
      BufferedReader reader = null;
      StringBuilder sb = null;
      try {
        reader = new BufferedReader(new InputStreamReader(is));
        String line;
        sb = new StringBuilder();
        while ((line = reader.readLine()) != null) {
          sb.append(line);
        }
      } catch (IOException ioe) {
        fail("Unexpected IOException");
      } finally {
        reader.close();
      }
      System.out.println("**********VALUE: " + sb.toString());
      writeTestFile(fs);
    } catch (IOException ioe) {
      fail("Unepxected IOException");
    }
  }
  
  private void writeTestFile(FileSystem fs) throws IOException {
    Path p = new Path("/users/test", "test.txt");
    FSDataOutputStream os = fs.create(p);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os));
    writer.write("ABCDEF");
    writer.flush();
    writer.close();
    
    if (fs.exists(p)) {
      System.out.println("FILE EXISTS");
      FileStatus[] stati = fs.listStatus(p);
      for (FileStatus status : stati) {
        System.out.println("Length: " + status.getLen());
      }
      
    } else {
      System.out.println("FILE DOES NOT EXIST");
    }
    FSDataInputStream fis = fs.open(p, BUFFER_SIZE);
    InputStreamReader isr = new InputStreamReader(fis);
    BufferedReader br = new BufferedReader(isr);
    String line;
    StringBuilder sb = new StringBuilder();
    while ((line = br.readLine()) != null) {
      System.out.println("line: " + line);
      sb.append(line);
    }
    br.close();
    assertEquals("ABCDEF", sb.toString());

  }
}
