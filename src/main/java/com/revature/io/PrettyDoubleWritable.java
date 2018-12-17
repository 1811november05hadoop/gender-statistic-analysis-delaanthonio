package com.revature.io;

import org.apache.hadoop.io.DoubleWritable;

public class PrettyDoubleWritable extends DoubleWritable {


  public PrettyDoubleWritable() {
    super();
  }

  public PrettyDoubleWritable(double value) {
    super(value);
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    return Math.abs(get() - ((DoubleWritable) o).get()) < 0.000000001;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return String.format("%s%3.1f", get() > 0 ? "+" : "", get());
  }
}
