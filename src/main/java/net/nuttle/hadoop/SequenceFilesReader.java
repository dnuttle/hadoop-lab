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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Adobe Systems Inc
 */
public final class SequenceFilesReader {

  private static final Logger LOG = LoggerFactory.getLogger(SequenceFilesReader.class);

  public <K extends Writable, V extends Writable> void read(
    Configuration configuration, Path dir, Handler<K, V> handler) throws IOException {
    read(configuration, dir, new HiddenPathFilter(), handler);
  }

  public <K extends Writable, V extends Writable> void read(
    Configuration configuration, Path dir, PathFilter pathFilter,
    Handler<K, V> handler) throws IOException {

    final FileSystem fileSystem = FileSystem.get(configuration);
    if (!fileSystem.exists(dir)) {
      return;
    }

    FileStatus[] statuses = fileSystem.listStatus(dir, pathFilter);
    for (FileStatus status : statuses) {
      if (status.isDirectory()) {
        read(configuration, status.getPath(), pathFilter, handler);
        continue;
      }

      readFile(configuration, status.getPath(), handler);
    }
  }

  public <K extends Writable, V extends Writable> void readFile(
    Configuration configuration, Path file, Handler<K, V> handler) throws IOException {

    final FileSystem fileSystem = FileSystem.get(configuration);
    if (!fileSystem.exists(file)) {
      return;
    }
    FileStatus fileStatus = fileSystem.getFileStatus(file);

    LOG.debug("Reading from file {}", fileStatus.getPath());
    if (fileStatus.getLen() == 0) {
      LOG.info("Empty file {}", fileStatus.getPath());
      return;
    }

    SequenceFile.Reader reader = new SequenceFile.Reader(configuration,
      SequenceFile.Reader.file(fileStatus.getPath()));

    try {
      K key = handler.newKey();
      V value = handler.newValue();
      while (reader.next(key, value)) {
        handler.handle(key, value);
      }
    } finally {
      reader.close();
    }
  }

  /**
   * @param <K>
   * @param <V>
   * 
   * @author Adobe Systems Inc
   */
  public interface Handler<K extends Writable, V extends Writable> {

    K newKey();

    V newValue();

    void handle(K key, V value);
  }

}
