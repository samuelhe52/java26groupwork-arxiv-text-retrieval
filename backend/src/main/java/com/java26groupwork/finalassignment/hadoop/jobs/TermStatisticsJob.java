package com.java26groupwork.finalassignment.hadoop.jobs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public final class TermStatisticsJob {

    public static final String TERM_FREQUENCY_DIRECTORY_NAME = "tf";
    public static final String DOCUMENT_FREQUENCY_DIRECTORY_NAME = "df";

    private static final String TERM_FREQUENCY_OUTPUT_NAME = "termFrequency";
    private static final String DOCUMENT_FREQUENCY_OUTPUT_NAME = "documentFrequency";
    private static final String TERM_FREQUENCY_BASE_OUTPUT_PATH = TERM_FREQUENCY_DIRECTORY_NAME + "/part";
    private static final String DOCUMENT_FREQUENCY_BASE_OUTPUT_PATH = DOCUMENT_FREQUENCY_DIRECTORY_NAME + "/part";

    private TermStatisticsJob() {}

    public static Job createJob(Configuration configuration, Path inputPath, Path outputPath)
            throws IOException {
        Job job = Job.getInstance(configuration, "arxiv-term-statistics");
        job.setJarByClass(TermStatisticsJob.class);
        job.setMapperClass(TermStatisticsMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(
                job, TERM_FREQUENCY_OUTPUT_NAME, TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(
                job, DOCUMENT_FREQUENCY_OUTPUT_NAME, TextOutputFormat.class, Text.class, IntWritable.class);
        return job;
    }

    public static final class TermStatisticsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final Text outputKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            ArxivJsonlSupport.ParsedDocument document = ArxivJsonlSupport.parsedDocument(value);
            if (document.documentId().isBlank()) {
                return;
            }

            Map<String, Integer> termCounts = new HashMap<>();
            for (String token : document.tokens()) {
                termCounts.merge(token, 1, Integer::sum);
            }

            for (Map.Entry<String, Integer> entry : termCounts.entrySet()) {
                outputKey.set("TF\t" + document.documentId() + "\t" + entry.getKey());
                context.write(outputKey, new IntWritable(entry.getValue()));
                outputKey.set("DF\t" + entry.getKey());
                context.write(outputKey, ONE);
            }
        }
    }

    public static final class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private MultipleOutputs<Text, IntWritable> outputs;

        @Override
        protected void setup(Context context) {
            outputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            String[] parts = key.toString().split("\t", 3);
            if (parts.length < 2) {
                return;
            }

            IntWritable outputValue = new IntWritable(sum);
            if ("TF".equals(parts[0]) && parts.length >= 3) {
                outputs.write(
                        TERM_FREQUENCY_OUTPUT_NAME,
                        new Text(parts[1] + "\t" + parts[2]),
                        outputValue,
                        TERM_FREQUENCY_BASE_OUTPUT_PATH);
                return;
            }
            if ("DF".equals(parts[0])) {
                outputs.write(
                        DOCUMENT_FREQUENCY_OUTPUT_NAME,
                        new Text(parts[1]),
                        outputValue,
                        DOCUMENT_FREQUENCY_BASE_OUTPUT_PATH);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputs.close();
        }
    }

    public static final class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

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
