package com.java26groupwork.finalassignment.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Minimal word-count MapReduce job that can run in Hadoop local mode. */
public final class LocalWordCountJob {

  private LocalWordCountJob() {}

  public static Configuration localConfiguration() {
    Configuration configuration = new Configuration(false);
    configuration.set("fs.defaultFS", "file:///");
    configuration.set("mapreduce.framework.name", "local");
    configuration.set("fs.file.impl.disable.cache", "true");
    return configuration;
  }

  public static Job createJob(Configuration configuration, Path inputPath, Path outputPath)
      throws IOException {
    Job job = Job.getInstance(configuration, "local-wordcount");
    job.setJarByClass(LocalWordCountJob.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);
    return job;
  }

  public static boolean run(Configuration configuration, Path inputPath, Path outputPath)
      throws Exception {
    return createJob(configuration, inputPath, outputPath).waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: LocalWordCountJob <input> <output>");
      System.exit(1);
    }

    boolean success = run(localConfiguration(), new Path(args[0]), new Path(args[1]));
    System.exit(success ? 0 : 1);
  }

  public static final class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer tokenizer = new StringTokenizer(value.toString());
      while (tokenizer.hasMoreTokens()) {
        context.write(new Text(tokenizer.nextToken().toLowerCase()), ONE);
      }
    }
  }

  public static final class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }
}
