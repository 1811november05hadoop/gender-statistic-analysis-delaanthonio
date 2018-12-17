package com.revature.mapreduce;

import com.revature.conf.Setting;
import com.revature.dao.GenderStatsData.Key;
import com.revature.io.PrettyDoubleWritable;
import com.revature.io.PrettySortedMapWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class IndicatorDifferenceReducer extends
    Reducer<Text, MapWritable, Text, SortedMapWritable> {

  private static final Logger LOGGER = Logger.getLogger(IndicatorChangeMapper.class);
  private static final int YEAR_START = 2010;
  private static final int YEAR_END = 2015;

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
  protected void reduce(Text country, Iterable<MapWritable> values, Context context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    String primaryIndicatorCode = conf.get(Setting.INDICATOR_CODE);
    String secondaryIndicatorCode = conf.get(Setting.INDICATOR_CODE_SECONDARY);

    SortedMapWritable indicatorMapPrimary = null;
    SortedMapWritable indicatorMapSecondary = null;
    SortedMapWritable indicatorDifferenceMap = new PrettySortedMapWritable();
    for (MapWritable mapWritable : values) {
      String indicatorCode = mapWritable.get(Key.INDICATOR_CODE).toString();
      LOGGER.debug("Received MapWritable: " + mapWritable.toString());
      if (primaryIndicatorCode.equals(indicatorCode)) {
        indicatorMapPrimary = (SortedMapWritable) mapWritable.get(Key.COLUMN_YEARS);
      } else if (secondaryIndicatorCode.equals(indicatorCode)) {
        indicatorMapSecondary = (SortedMapWritable) mapWritable.get(Key.COLUMN_YEARS);
      }
    }

    if (indicatorMapPrimary == null || indicatorMapSecondary == null) {
      if (indicatorMapPrimary == null) {
        LOGGER.error("Null primary indicator map");
      }

      if (indicatorMapSecondary == null) {
        LOGGER.error("Null secondary indicator map");
      }
      return;
    }

    LOGGER.debug("Both indicators set successfully");
    LOGGER.debug("primary indicator: " + indicatorMapPrimary);
    LOGGER.debug("secondary indicator: " + indicatorMapSecondary);

    for (int i = YEAR_START; i <= YEAR_END; i++) {
      IntWritable year = new IntWritable(i);
      DoubleWritable primaryVal = (DoubleWritable) indicatorMapPrimary.get(year);
      DoubleWritable secondaryVal = (DoubleWritable) indicatorMapSecondary.get(year);
      if (primaryVal != null && secondaryVal != null) {
        DoubleWritable indicatorDifference =
            new PrettyDoubleWritable(primaryVal.get() - secondaryVal.get());
        indicatorDifferenceMap.put(year, indicatorDifference);
      }
    }
    if (!indicatorDifferenceMap.isEmpty()) {
      context.write(new Text(country), indicatorDifferenceMap);
    }
  }
}
