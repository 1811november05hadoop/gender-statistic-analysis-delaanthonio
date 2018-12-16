package com.revature.mapreduce;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.revature.dao.GenderStatsData;
import com.revature.io.PrettyDoubleWritable;
import com.revature.io.PrettyMapWritable;
import com.revature.io.PrettySortedMapWritable;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FemaleGraduateMapper extends
    Mapper<LongWritable, Text, Text, DoubleWritable> {

  public static final Text COUNTRY_NAME_KEY = new Text("CountryName");
  public static final Text COUNTRY_CODE_KEY = new Text("CountryCode");
  public static final Text INDICATOR_NAME_KEY = new Text("IndicatorName");
  public static final Text INDICATOR_CODE_KEY = new Text("IndicatorCode");
  private static final double MINIMUM_GRADUATION_RATE = 30;

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    if (key.get() == 0L) {
      return;
    }
    Iterator<String> row =
        Splitter.onPattern("\",\"")
            .trimResults(CharMatcher.anyOf("\","))
            .split(value.toString())
            .iterator();

    Text countryName = new Text(row.next());
    Text countryCode = new Text(row.next());
    Text indicatorName = new Text(row.next());
    Text indicatorCode = new Text(row.next());

    final MapWritable resultKey = new PrettyMapWritable();
    resultKey.put(COUNTRY_NAME_KEY, countryName);
    resultKey.put(COUNTRY_CODE_KEY, countryCode);
    resultKey.put(INDICATOR_NAME_KEY, indicatorName);
    resultKey.put(INDICATOR_CODE_KEY, indicatorCode);

    final SortedMapWritable resultValue = new PrettySortedMapWritable();

    for (int i = 1960; i <= 2016 && row.hasNext(); i++) {
      String metric = row.next();
      if (!metric.equals("") && !metric.equals(",")) {
        resultValue.put(new IntWritable(i), new PrettyDoubleWritable(Double.parseDouble(metric)));
      }
    }

    if (resultValue.isEmpty()) {
      return;
    }

    if (resultKey.get(DualYearMapper.INDICATOR_CODE_KEY).toString()
        .equals(GenderStatsData.FEMALE_GRADUATION_RATE_CODE)) {
      IntWritable mostRecentYear = (IntWritable) resultValue.lastKey();
      DoubleWritable graduationRate = (DoubleWritable) resultValue.get(mostRecentYear);
      if (graduationRate.get() < MINIMUM_GRADUATION_RATE) {
        context.write(countryName, graduationRate);
      }
    }
  }
}
