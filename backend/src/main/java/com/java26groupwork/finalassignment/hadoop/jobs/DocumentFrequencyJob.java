package com.java26groupwork.finalassignment.hadoop.jobs;

import java.io.IOException;
import java.util.Set;
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

public final class DocumentFrequencyJob {

    private DocumentFrequencyJob() {}

    public static Job createJob(Configuration configuration, Path inputPath, Path outputPath)
            throws IOException {
        Job job = Job.getInstance(configuration, "arxiv-document-frequency");
        job.setJarByClass(DocumentFrequencyJob.class);
        job.setMapperClass(DocumentFrequencyMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

    public static final class DocumentFrequencyMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final Text outputKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            ArxivJsonlSupport.ParsedDocument document = ArxivJsonlSupport.parsedDocument(value);
            Set<String> uniqueTokens = ArxivJsonlSupport.uniqueTokens(document);
            for (String token : uniqueTokens) {
                outputKey.set(token);
                context.write(outputKey, ONE);
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
