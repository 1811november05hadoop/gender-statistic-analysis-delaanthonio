package com.revature.mapreduce;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.revature.conf.Setting;
import com.revature.dao.GenderStatsData.Key;
import com.revature.io.PrettyDoubleWritable;
import com.revature.io.PrettyMapWritable;
import com.revature.io.PrettySortedMapWritable;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndicatorChangeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  public static final int DEFAULT_YEAR_BEGIN = 2000;
  public static final int DEFAULT_YEAR_END = 2016;

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
    resultKey.put(Key.COUNTRY_NAME, countryName);
    resultKey.put(Key.COUNTRY_CODE, countryCode);
    resultKey.put(Key.INDICATOR_NAME, indicatorName);
    resultKey.put(Key.INDICATOR_CODE, indicatorCode);

    final SortedMapWritable resultValue = new PrettySortedMapWritable();

    Configuration conf = context.getConfiguration();
    String confIndicatorCode = conf.get(Setting.INDICATOR_CODE);

    for (int i = 1960; i <= 2016 && row.hasNext(); i++) {
      String metric = row.next();
      if (!metric.equals("") && !metric.equals(",")) {
        resultValue.put(new IntWritable(i), new DoubleWritable(Double.parseDouble(metric)));
      }
    }

    if (resultValue.isEmpty()) {
      return;
    }

    if (resultKey.get(Key.INDICATOR_CODE).toString()
        .equals(confIndicatorCode)) {
      IntWritable yearStart =
          new IntWritable(conf.getInt(Setting.INDICATOR_YEAR_START, DEFAULT_YEAR_BEGIN));
      IntWritable yearEnd =
          new IntWritable(conf.getInt(Setting.INDICATOR_YEAR_END, DEFAULT_YEAR_END));
      DoubleWritable indicatorBeginRate = (DoubleWritable) resultValue.get(yearStart);
      DoubleWritable indicatorEndRate =
          (DoubleWritable) resultValue.get(yearEnd);
      if (indicatorBeginRate == null || indicatorEndRate == null) {
        return;
      }
      DoubleWritable indicatorDifference = new PrettyDoubleWritable(
          indicatorEndRate.get() - indicatorBeginRate.get());
      context.write(countryName, indicatorDifference);
    }
  }
}
