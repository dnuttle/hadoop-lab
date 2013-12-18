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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import static net.nuttle.hadoop.SequenceFileUtil.writeSequenceFile;

/**
 * Tests SequenceFilesReader.
 * As this is an extension of ClusterMapReduceTestCase, it is a JUnit 3 test, 
 * and the @Test annotation is not needed.  In addition, @Before should not be used.
 * This caused the parent class' setUp method not to run for some reason.
 */
public class TestSequenceFilesReader extends ClusterMapReduceTestCase {
  private static final String TEST_FILE_PATH = "/users/test/seq";
  private static final String TEST_FILE_SUBPATH = "/users/test/seq/2";
  private static final int FOUR = 4;
  private static final Map<String, Integer> VALUES = new HashMap<String, Integer>();
  private static final Logger LOG = Logger.getLogger(TestSequenceFilesReader.class);

  public void setUp() throws Exception {
    super.setUp();
    VALUES.put("ABCDEF", 1);
    VALUES.put("GHIKJL", 2);
  }

  /**
   * Tests SequenceFilesReader.readFile.
   * Confirms that the file read contains the expected keys and values.
   * @throws IOException
   */
  public void testReadFile() throws IOException {
    Log.info("testReadFile");
    try {
      this.getFileSystem();
    } catch (NullPointerException npe) {
      npe.printStackTrace();
    }
    FileSystem fs = this.getFileSystem();
    Path testFile = new Path(TEST_FILE_PATH, "one.file");
    Configuration conf = (Configuration) this.getMRCluster().getJobTrackerConf();
    writeSequenceFile(fs, conf, testFile, VALUES);
    SequenceFilesReader reader = new SequenceFilesReader();
    reader.readFile(conf, testFile, new SequenceFilesReader.Handler<Text, IntWritable>() {
      @Override
      public void handle(Text key, IntWritable value) {
        assertTrue(VALUES.keySet().contains(key.toString()));
        assertEquals(VALUES.get(key.toString()).intValue(), value.get());
      }
      @Override
      public Text newKey() {
        return new Text();
      }
      public IntWritable newValue() {
        return new IntWritable();
      }
    });
  }
  
  /**
   * Tests SequenceFilesReader.read(Configuration, Path, Handler).
   * Writes four files, two of which are "hidden", and confirms that two files are read.
   */
  public void testRead1() throws IOException {
    Log.info("testRead1");
    FileSystem fs = this.getFileSystem();
    Configuration conf = (Configuration) this.getMRCluster().getJobTrackerConf();
    Path testFile = null;
    Path testFilePath = new Path(TEST_FILE_PATH);
    //Write two hidden files and confirm that neither are read
    //One of them is in a subpath
    testFile = new Path(TEST_FILE_PATH, "_hidden.file");
    writeSequenceFile(fs, conf, testFile, VALUES);
    testFile = new Path(TEST_FILE_SUBPATH, ".hidden.file");
    writeSequenceFile(fs, conf, testFile, VALUES);
    final LineCounter counter = new LineCounter();
    SequenceFilesReader reader = new SequenceFilesReader();
    reader.read(conf, testFilePath, new SequenceFilesReader.Handler<Text, IntWritable>() {
      @Override
      public void handle(Text key, IntWritable value) {
        counter.increment();
      }
      @Override
      public Text newKey() {
        return new Text();
      }
      public IntWritable newValue() {
        return new IntWritable();
      }
    });
    assertEquals(0, counter.getCount());
    //Write a non-hidden file and confirm that it is read
    testFile = new Path(TEST_FILE_PATH, "one.file");
    writeSequenceFile(fs, conf, testFile, VALUES);
    counter.reset();
    reader.read(conf, testFilePath, new SequenceFilesReader.Handler<Text, IntWritable>() {
      @Override
      public void handle(Text key, IntWritable value) {
        counter.increment();
      }
      @Override
      public Text newKey() {
        return new Text();
      }
      public IntWritable newValue() {
        return new IntWritable();
      }
    });
    assertEquals(2, counter.getCount());
    //Write a second non-hidden file in a subpath and confirm that both non-hidden files are read
    testFile = new Path(TEST_FILE_SUBPATH, "two.file");
    writeSequenceFile(fs, conf, testFile, VALUES);
    counter.reset();
    reader.read(conf, testFilePath, new SequenceFilesReader.Handler<Text, IntWritable>() {
      @Override
      public void handle(Text key, IntWritable value) {
        counter.increment();
      }
      @Override
      public Text newKey() {
        return new Text();
      }
      public IntWritable newValue() {
        return new IntWritable();
      }
    });
    assertEquals(FOUR, counter.getCount());
  }
  
  /**
   * Tests SequenceFilesReader.read(Configuration, Path, PathFilter, Handler).
   * Writes two files, one of which is "hidden", and confirms that only one file is read
   * with HiddenPathFilter, and both are read with a test AllPathsFilter.
   */
  public void testRead2() throws IOException {
    Log.info("testRead2");
    FileSystem fs = this.getFileSystem();
    Configuration conf = (Configuration) this.getMRCluster().getJobTrackerConf();
    Path testFile = null;
    Path testFilePath = new Path(TEST_FILE_PATH);
    final LineCounter counter = new LineCounter();
    //Write one hidden and one non-hidden file
    testFile = new Path(TEST_FILE_PATH, "_hidden.file");
    writeSequenceFile(fs, conf, testFile, VALUES);
    testFile = new Path(TEST_FILE_PATH, "one.file");
    writeSequenceFile(fs, conf, testFile, VALUES);
    //Use a HiddenPathFilter and confirm that only one file is read
    SequenceFilesReader reader = new SequenceFilesReader();
    reader.read(conf, testFilePath, new HiddenPathFilter(), new SequenceFilesReader.Handler<Text, IntWritable>() {
      @Override
      public void handle(Text key, IntWritable value) {
        counter.increment();
      }
      @Override
      public Text newKey() {
        return new Text();
      }
      public IntWritable newValue() {
        return new IntWritable();
      }
    });
    assertEquals(2, counter.getCount());
    //Use AllPathsFilter and confirm that both files are read
    counter.reset();
    reader.read(conf, testFilePath, new AllPathsFilter(), new SequenceFilesReader.Handler<Text, IntWritable>() {
      @Override
      public void handle(Text key, IntWritable value) {
        counter.increment();
      }
      @Override
      public Text newKey() {
        return new Text();
      }
      public IntWritable newValue() {
        return new IntWritable();
      }
    });
    assertEquals(FOUR, counter.getCount());
  }

  
  /**
   * Simple counter class that can be instantiated as final and reset,
   * so that it can be used in anonymous inner classes.
   */
  private static class LineCounter {
    private int count;
    public void increment() {
      count++;
    }
    public int getCount() {
      return count;
    }
    public void reset() {
      count = 0;
    }
  }
  /**
   * Simple sample of PathFilter that returns all files.
   */
  private static class AllPathsFilter implements PathFilter {
    public boolean accept(Path path) {
      return true;
    }
  }
}
