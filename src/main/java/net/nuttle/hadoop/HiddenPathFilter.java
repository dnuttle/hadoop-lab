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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @author Adobe Systems Inc
 */
public final class HiddenPathFilter implements PathFilter {

  private static final String[] HIDDEN_FILE_PREFIX = { "_", "." };

  @Override
  public boolean accept(Path path) {
    String name = path.getName();
    for (String prefix : HIDDEN_FILE_PREFIX) {
      if (StringUtils.startsWith(name, prefix)) {
        return false;
      }
    }
    return true;
  }

}
