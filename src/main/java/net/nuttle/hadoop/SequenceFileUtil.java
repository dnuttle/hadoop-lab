package net.nuttle.hadoop;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * Utility for SequenceFile IO
 */
public class SequenceFileUtil {
  
  private SequenceFileUtil() {}

  /**
   * Writes a Map<String, Integer> to a sequence file
   * @param fs
   * @param conf
   * @param name
   * @param values
   * @throws IOException
   */
  public static void writeSequenceFile(FileSystem fs, Configuration conf, Path name, Map<String, Integer> values) 
      throws IOException {
      @SuppressWarnings("deprecation")
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, name, Text.class, IntWritable.class);
      for (String key : values.keySet()) {
        Text inKey = new Text(key);
        IntWritable inVal = new IntWritable(values.get(key).intValue());
        writer.append(inKey, inVal);
      }
      writer.close();
    }

}
