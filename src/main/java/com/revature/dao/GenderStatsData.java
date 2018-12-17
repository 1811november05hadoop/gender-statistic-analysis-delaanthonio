package com.revature.dao;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;

public class GenderStatsData {

  public static final String FEMALE_GRADUATION_RATE_CODE = "SE.TER.CMPL.FE.ZS";
  public static final String FEMALE_EMPLOYMENT_RATE_CODE = "SL.EMP.TOTL.SP.FE.ZS";
  public static final String FEMALE_UPPER_SECONDARY_EDUCATION_RATE_CODE = "SE.SEC.CUAT.UP.FE.ZS";
  public static final String MALE_EMPLOYMENT_RATE_CODE = "SL.EMP.TOTL.SP.MA.ZS";
  private final List<String> data;

  public GenderStatsData(Text value) {
    this(value.toString());
  }

  public GenderStatsData(String row) {
    Iterable<String> strings = Splitter.onPattern("\",\"").trimResults(CharMatcher.is('"'))
        .split(row);
    data = new ArrayList<>();
    for (String string : strings) {
      data.add(string);
    }
  }
  

  public static class Key {

    public static final Text COUNTRY_NAME = new Text("CountryName");
    public static final Text COUNTRY_CODE = new Text("CountryCode");
    public static final Text INDICATOR_NAME = new Text("IndicatorName");
    public static final Text INDICATOR_CODE = new Text("IndicatorCode");
    public static final Text COLUMN_YEARS = new Text("ColumnYears");
  }


}
