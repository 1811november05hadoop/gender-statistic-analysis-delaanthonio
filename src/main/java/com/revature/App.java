package com.revature;

import com.revature.dao.GenderStatsData;
import com.revature.mapreduce.FemaleGraduateMapper;
import com.revature.mapreduce.IndicatorChangeMapper;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

    Configuration conf = new Configuration();
    conf.set("IndicatorCode", GenderStatsData.FEMALE_GRADUATION_RATE_CODE);
    LOGGER.debug("Job indicator code: " + conf.get("IndicatorCode"));

    int jobNumber = 1;
    if (args.length > 2) {
      jobNumber = Integer.parseInt(args[2]);
    }

    Job job = Job.getInstance(conf);
    job.setJarByClass(App.class);

    Class<? extends Mapper> mapperClass = null;
    Class<? extends Reducer> reducerClass = null;
    Class<? extends Writable> outputKeyClass = Text.class;
    Class<? extends Writable> outputValueClass = DoubleWritable.class;
    String jobName = "Unknown";

    switch (jobNumber) {
      case FEMALE_GRADUATION_JOB:
        mapperClass = FemaleGraduateMapper.class;
        outputKeyClass = Text.class;
        outputValueClass = DoubleWritable.class;
        jobName = "Female Graduation";
        break;
      case FEMALE_EDUCATION_JOB:
        mapperClass = IndicatorChangeMapper.class;
        conf.set("IndicatorCode", GenderStatsData.FEMALE_UPPER_SECONDARY_EDUCATION_RATE_CODE);
        jobName = "Female Secondary Education";
        break;
      case MALE_EMPLOYMENT_CHANGE:
        mapperClass = IndicatorChangeMapper.class;
        conf.set("IndicatorCode", GenderStatsData.MALE_EMPLOYMENT_RATE_CODE);
        jobName = "Male Employment";
        break;
      case FEMALE_EMPLOYMENT_CHANGE:
        mapperClass = IndicatorChangeMapper.class;
        conf.set("IndicatorCode", GenderStatsData.FEMALE_EMPLOYMENT_RATE_CODE);
        jobName = "Female Employment";
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
    } else {
      job.setReducerClass(reducerClass);
    }

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
