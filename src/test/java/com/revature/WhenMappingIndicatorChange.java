package com.revature;

import com.revature.conf.Setting;
import com.revature.dao.GenderStatsData;
import com.revature.io.PrettyDoubleWritable;
import com.revature.mapreduce.IndicatorChangeMapper;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class WhenMappingIndicatorChange {

  private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;

  private LongWritable mapperInputKey;
  private Text mapperInputValue;

  private Text mapperOutputKey;
  private DoubleWritable mapperOutputValue;

  @Before
  public void setUp() {
    IndicatorChangeMapper mapper = new IndicatorChangeMapper();
    mapDriver = new MapDriver<>();
    mapDriver.setMapper(mapper);
  }

  @Test
  public void shouldMapFemaleSecondaryEducation() throws IOException {
    Configuration conf = mapDriver.getConfiguration();
    conf.set(Setting.INDICATOR_CODE, GenderStatsData.FEMALE_UPPER_SECONDARY_EDUCATION_RATE_CODE);
    conf.setInt(Setting.INDICATOR_YEAR_START, 1960);
    conf.setInt(Setting.INDICATOR_YEAR_END, 1961);
    String row = "\"Arab World\","
        + "\"ARB\",\""
        + "Educational attainment, at least completed upper secondary, population 25+, female (%) (cumulative)\",\""
        + GenderStatsData.FEMALE_UPPER_SECONDARY_EDUCATION_RATE_CODE
        + "\",\".2\",\"1.0\"";
    mapperInputKey = new LongWritable(4);
    mapperInputValue = new Text(row);
    mapperOutputKey = new Text("Arab World");
    mapperOutputValue = new PrettyDoubleWritable(0.8);

    mapDriver
        .withInput(mapperInputKey, mapperInputValue)
        .withOutput(mapperOutputKey, mapperOutputValue)
        .runTest();
  }
}
