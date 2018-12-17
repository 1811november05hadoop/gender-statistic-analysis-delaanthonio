package com.revature;

import com.revature.dao.GenderStatsData;
import com.revature.io.PrettyDoubleWritable;
import com.revature.mapreduce.FemaleGraduateMapper;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class WhenMappingFemaleEmploymentRate {

  private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;

  private LongWritable mapperInputKey;
  private Text mapperInputValue;

  private Text mapperOutputKey;
  private DoubleWritable mapperOutputValue;

  @Before
  public void setUp() {
    FemaleGraduateMapper mapper = new FemaleGraduateMapper();
    mapDriver = new MapDriver<>();
    mapDriver.setMapper(mapper);
  }

  @Test
  public void shouldMapCorrectly() throws IOException {
    String row = new StringBuilder()
        .append("\"Arab World\",")
        .append("\"ARB\",\"")
        .append(
            "Gross graduation ratio, tertiary, female (%)\",\"")
        .append(GenderStatsData.FEMALE_GRADUATION_RATE_CODE)
        .append("\",\".02\",\".10\"")
        .toString();

    mapperInputKey = new LongWritable(4);
    mapperInputValue = new Text(row);
    mapperOutputKey = new Text("Arab World");
    mapperOutputValue = new PrettyDoubleWritable(0.1);

    mapDriver
        .withInput(mapperInputKey, mapperInputValue)
        .withOutput(mapperOutputKey, mapperOutputValue)
        .runTest();
  }
}
