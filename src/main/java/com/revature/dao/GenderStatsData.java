package com.revature.dao;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class GenderStatsData {

  public static final String FEMALE_GRADUATION_RATE_CODE = "SE.TER.CMPL.FE.ZS";
  public static final String FEMALE_EMPLOYMENT_RATE_CODE = "SL.EMP.MPYR.FE.ZS";
  public static final String FEMALE_UPPER_SECONDARY_EDUCATION_RATE_CODE = "SE.SEC.CUAT.UP.FE.ZS";
  public static final String MALE_EMPLOYMENT_RATE_CODE = "SL.EMP.MPYR.MA.ZS";

  public static final int FIRST_YEAR = 1960;
  public static final int LAST_YEAR = 2015;
  private static final Logger LOGGER = Logger.getLogger(GenderStatsData.class);
  private static final int COUNTRY_CODE_INDEX = 1;
  private static final int INDICATOR_NAME_INDEX = 2;
  private static final int INDICATOR_CODE_INDEX = 3;
  private static final int FIRST_YEAR_INDEX = 4;
  private final List<String> data;

  public GenderStatsData(Text value) {
    this(value.toString());
  }

  public GenderStatsData(String row) {
    // LOGGER.debug("Constructing GenderStatsData with " + row);

    Iterable<String> strings = Splitter.onPattern("\",\"").trimResults(CharMatcher.is('"'))
        .split(row);
    data = new ArrayList<>();
    for (String string : strings) {
      data.add(string);
    }
  }

  public String getData() {
    return String.join(",", data);
  }


  public String getIndicatorCode() {
    return data.get(INDICATOR_CODE_INDEX);
  }


}
