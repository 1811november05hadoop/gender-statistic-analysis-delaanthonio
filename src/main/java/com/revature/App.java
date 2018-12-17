package com.revature;

import com.revature.conf.Setting;
import com.revature.dao.GenderStatsData;
import com.revature.io.PrettyMapWritable;
import com.revature.mapreduce.DualIndicatorMapper;
import com.revature.mapreduce.FemaleGraduateMapper;
import com.revature.mapreduce.IndicatorChangeMapper;
import com.revature.mapreduce.IndicatorDifferenceReducer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * Hello world!
 */
public class App {

  private static final int FEMALE_GRADUATION_JOB = 1;
  private static final int FEMALE_EDUCATION_JOB = 2;
  private static final int MALE_EMPLOYMENT_CHANGE = 3;
  private static final int FEMALE_EMPLOYMENT_CHANGE = 4;
  private static final int MALE_FEMALE_EMPLOYMENT_DIFFERENCE = 5;

  private static final Logger LOGGER = Logger.getLogger(App.class);

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      LOGGER.error("Usage: WordCount <input dir> <output dir>");
      System.exit(-1);
    }

    String now =
        DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss")
            .withZone(ZoneOffset.UTC)
            .format(Instant.now());

    int jobNumber = 1;
    if (args.length > 2) {
      jobNumber = Integer.parseInt(args[2]);
    }
    LOGGER.debug("Job number: " + jobNumber);

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(App.class);

    Class<? extends Mapper> mapperClass = null;
    Class<? extends Reducer> reducerClass = null;
    Class<? extends Writable> outputKeyClass = Text.class;
    Class<? extends Writable> outputValueClass = DoubleWritable.class;
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    String jobName = "Unknown";

    conf = job.getConfiguration();
    switch (jobNumber) {
      case FEMALE_GRADUATION_JOB:
        mapperClass = FemaleGraduateMapper.class;
        outputKeyClass = Text.class;
        outputValueClass = DoubleWritable.class;
        conf.set(Setting.INDICATOR_CODE, GenderStatsData.FEMALE_GRADUATION_RATE_CODE);
        jobName = "Female Graduation";
        break;
      case FEMALE_EDUCATION_JOB:
        mapperClass = IndicatorChangeMapper.class;
        conf.set(Setting.INDICATOR_CODE,
            GenderStatsData.FEMALE_UPPER_SECONDARY_EDUCATION_RATE_CODE);
        jobName = "Female Secondary Education";
        break;
      case MALE_EMPLOYMENT_CHANGE:
        mapperClass = IndicatorChangeMapper.class;
        conf.set(Setting.INDICATOR_CODE, GenderStatsData.MALE_EMPLOYMENT_RATE_CODE);
        jobName = "Male Employment";
        break;
      case FEMALE_EMPLOYMENT_CHANGE:
        mapperClass = IndicatorChangeMapper.class;
        conf.set(Setting.INDICATOR_CODE, GenderStatsData.FEMALE_EMPLOYMENT_RATE_CODE);
        jobName = "Female Employment";
        break;
      case MALE_FEMALE_EMPLOYMENT_DIFFERENCE:
        mapperClass = DualIndicatorMapper.class;
        reducerClass = IndicatorDifferenceReducer.class;
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PrettyMapWritable.class);
        job.setOutputValueClass(SortedMapWritable.class);
        conf.set(Setting.INDICATOR_CODE, GenderStatsData.MALE_EMPLOYMENT_RATE_CODE);
        conf.set(Setting.INDICATOR_CODE_SECONDARY, GenderStatsData.FEMALE_EMPLOYMENT_RATE_CODE);
        LOGGER.debug("Primary Code: " + conf.get(Setting.INDICATOR_CODE));
        LOGGER.debug("Secondary Code: " + conf.get(Setting.INDICATOR_CODE_SECONDARY));
        jobName = "Male-Female Employment";
        break;
      default:
        LOGGER.error("Unknown job");
        System.exit(1);
    }

    Path input = new Path(args[0]);
    Path output = new Path(args[1] + jobName + "/" + now);
    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    job.setMapperClass(mapperClass);
    job.setOutputKeyClass(outputKeyClass);
    job.setOutputValueClass(outputValueClass);

    if (reducerClass == null) {
      job.setNumReduceTasks(0);
      LOGGER.debug("Running Map only job");
    } else {
      job.setReducerClass(reducerClass);
      job.setNumReduceTasks(1);
    }

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
