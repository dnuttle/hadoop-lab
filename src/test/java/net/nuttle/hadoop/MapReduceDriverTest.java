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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

/**
 * This class performs local tests of Mappers and Reducers using MapReduceDriver, MapDriver or ReduceDriver.  
 * I.e., they are run locally, not in a running
 * instance of Hadoop.  As a result, there is no HDFS IO.  Instead, inputs are fed in, and expected outputs 
 * are defined.
 */
public class MapReduceDriverTest {

  /**
   * This tests both a mapper and a reducer, without any HDFS IO.
   * The mapper has inputs passed into it, and the reducer has expected outputs defined.
   * @throws IOException
   */
  @Test
  public void testMapReduceDriver() throws IOException {
    Object inKey = new Object();
    Text inValue = new Text();
    inValue.set("this is a string");
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mrd = 
      new MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable>();
    mrd.withMapper(new WordCountMapper()).withInput(inKey, inValue);
    mrd.withReducer(new WordCountReducer());
    mrd.addOutput(new Text("this"), new IntWritable(1));
    mrd.addOutput(new Text("is"), new IntWritable(1));
    mrd.addOutput(new Text("a"), new IntWritable(1));
    mrd.addOutput(new Text("string"), new IntWritable(1));
    mrd.runTest(false);
  }

  /**
   * This tests a mapper in isolation.  The expected outputs are added, and the test 
   * confirms that these are returned by the mapper.
   * @throws IOException
   */
  @Test
  public void testMapDriver() throws IOException {
    Object inKey = new Object();
    Text inValue = new Text();
    inValue.set("this is a string");
    MapDriver<Object, Text, Text, IntWritable> mr = new MapDriver<Object, Text, Text, IntWritable>();
    mr.withMapper(new WordCountMapper()).withInput(inKey, inValue);
    mr.addOutput(new Text("this"), new IntWritable(1));
    mr.addOutput(new Text("is"), new IntWritable(1));
    mr.addOutput(new Text("a"), new IntWritable(1));
    mr.addOutput(new Text("string"), new IntWritable(1));
    mr.runTest(false);
  }
  
  /**
   * This tests a Reducer in isolation.
   * @throws IOException
   */
  @Test
  public void testReduceDriver() throws IOException {
    ReduceDriver<Text, IntWritable, Text, IntWritable> rd = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
    rd.withReducer(new WordCountReducer());
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    rd.setInputKey(new Text("ABC"));
    rd.addInputValues(values);
    rd.withOutput(new Text("ABC"), new IntWritable(2));
    rd.runTest();
  }
}
