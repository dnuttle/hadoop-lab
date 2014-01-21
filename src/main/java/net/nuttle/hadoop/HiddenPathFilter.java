package net.nuttle.hadoop;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 
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
