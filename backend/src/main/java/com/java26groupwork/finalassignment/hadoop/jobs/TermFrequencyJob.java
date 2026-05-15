package com.java26groupwork.finalassignment.hadoop.jobs;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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

public final class TermFrequencyJob {

    private TermFrequencyJob() {}

    public static Job createJob(Configuration configuration, Path inputPath, Path outputPath)
            throws IOException {
        Job job = Job.getInstance(configuration, "arxiv-term-frequency");
        job.setJarByClass(TermFrequencyJob.class);
        job.setMapperClass(TermFrequencyMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

    public static final class TermFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text outputKey = new Text();
        private final IntWritable outputValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            ArxivJsonlSupport.ParsedDocument document = ArxivJsonlSupport.parsedDocument(value);
            if (document.documentId().isBlank()) {
                return;
            }
            Map<String, Integer> termCounts = new HashMap<>();
            List<String> tokens = document.tokens();
            for (String token : tokens) {
                termCounts.merge(token, 1, Integer::sum);
            }
            for (Map.Entry<String, Integer> entry : termCounts.entrySet()) {
                outputKey.set(document.documentId() + "\t" + entry.getKey());
                outputValue.set(entry.getValue());
                context.write(outputKey, outputValue);
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
