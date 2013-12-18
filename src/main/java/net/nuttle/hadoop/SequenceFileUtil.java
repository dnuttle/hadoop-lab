/*************************************************************************
 *
 * ADOBE CONFIDENTIAL
 * __________________
 *
 *  Copyright 2012 Adobe Systems Incorporated
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Adobe Systems Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Adobe Systems Incorporated and its
 * suppliers and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe Systems Incorporated.
 **************************************************************************/
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
 *
 */
public class SequenceFileUtil {
  
  private SequenceFileUtil() {}

  public static void writeSequenceFile(FileSystem fs, Configuration conf, Path name, Map<String, Integer> values) 
      throws IOException {
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, name, Text.class, IntWritable.class);
      for (String key : values.keySet()) {
        Text inKey = new Text(key);
        IntWritable inVal = new IntWritable(values.get(key).intValue());
        writer.append(inKey, inVal);
      }
      writer.close();
    }

}
