package com.revature.io;

import java.util.Set;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.WritableComparable;

public class PrettySortedMapWritable extends SortedMapWritable {

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder("Map: ");
    Set<WritableComparable> keySet = keySet();

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
