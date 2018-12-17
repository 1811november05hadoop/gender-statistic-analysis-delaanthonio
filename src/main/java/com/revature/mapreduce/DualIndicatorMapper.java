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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class DualIndicatorMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

  private static final Logger LOGGER = Logger.getLogger(DualIndicatorMapper.class);

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    String primaryIndicatorCode = conf.get(Setting.INDICATOR_CODE);
    String secondaryIndicatorCode = conf.get(Setting.INDICATOR_CODE_SECONDARY);
    LOGGER.debug("Indicator code: " + primaryIndicatorCode);
    LOGGER.debug("Secondary Indicator code: " + secondaryIndicatorCode);
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    if (key.get() == 0L) {
      return;
    }
    Iterator<String> row =
        Splitter.onPattern("\",\"")
            .trimResults(CharMatcher.anyOf("\", "))
            .split(value.toString())
            .iterator();

    Text countryName = new Text(row.next());
    Text countryCode = new Text(row.next());
    Text indicatorName = new Text(row.next());
    Text indicatorCode = new Text(row.next());

    Configuration conf = context.getConfiguration();
    String primaryIndicatorCode = conf.get(Setting.INDICATOR_CODE);
    String secondaryIndicatorCode = conf.get(Setting.INDICATOR_CODE_SECONDARY);

    if (!indicatorCode.toString().contains(primaryIndicatorCode)
        && !indicatorCode.toString().contains(secondaryIndicatorCode)) {
      return;
    }

    final SortedMapWritable columnValues = new PrettySortedMapWritable();

    for (int i = 1960; i <= 2016 && row.hasNext(); i++) {
      String metric = row.next();
      if (!metric.equals("") && !metric.equals(",")) {
        columnValues
            .put(new IntWritable(i), new PrettyDoubleWritable(Double.parseDouble(metric)));
      }
    }

    if (columnValues.isEmpty()) {
      return;
    }

    MapWritable outputMap = new PrettyMapWritable();
    outputMap.put(Key.INDICATOR_CODE, indicatorCode);
    outputMap.put(Key.COLUMN_YEARS, columnValues);

    context.write(countryName, outputMap);
  }
}
