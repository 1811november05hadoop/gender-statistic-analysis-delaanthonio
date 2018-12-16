package com.revature.io;

import java.util.Set;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class PrettyMapWritable extends MapWritable {

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder("Map: ");
    Set<Writable> keySet = keySet();

    for (Object key : keySet) {
      result
          .append("{")
          .append(key)
          .append(" = ")
          .append(get(key))
          .append("}");
    }
    return result.toString();
  }
}
