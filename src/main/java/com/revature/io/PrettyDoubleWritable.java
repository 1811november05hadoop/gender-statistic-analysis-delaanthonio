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
  public String toString() {
    return String.format("%3.1f", get());
  }
}
