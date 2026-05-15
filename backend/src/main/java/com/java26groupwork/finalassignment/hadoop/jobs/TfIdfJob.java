package com.java26groupwork.finalassignment.hadoop.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public final class TfIdfJob {

    public static final String DOCUMENT_COUNT_KEY = "app.corpus.document-count";

    private TfIdfJob() {}

    public static Job createJob(
            Configuration configuration, Path termFrequencyPath, Path documentFrequencyPath, Path outputPath)
            throws IOException {
        Job job = Job.getInstance(configuration, "arxiv-tf-idf");
        job.setJarByClass(TfIdfJob.class);
        job.setReducerClass(TfIdfReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, termFrequencyPath, TextInputFormat.class, TermFrequencyInputMapper.class);
        MultipleInputs.addInputPath(
                job, documentFrequencyPath, TextInputFormat.class, DocumentFrequencyInputMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

    public static final class TermFrequencyInputMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length < 3) {
                return;
            }
            context.write(new Text(parts[1]), new Text("TF\t" + parts[0] + "\t" + parts[2]));
        }
    }

    public static final class DocumentFrequencyInputMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length < 2) {
                return;
            }
            context.write(new Text(parts[0]), new Text("DF\t" + parts[1]));
        }
    }

    public static final class TfIdfReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text term, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int documentCount = context.getConfiguration().getInt(DOCUMENT_COUNT_KEY, 1);
            int documentFrequency = 0;
            List<TermFrequencyRow> rows = new ArrayList<>();

            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                if (parts.length < 2) {
                    continue;
                }
                if ("DF".equals(parts[0])) {
                    documentFrequency = Integer.parseInt(parts[1]);
                } else if ("TF".equals(parts[0]) && parts.length >= 3) {
                    rows.add(new TermFrequencyRow(parts[1], Integer.parseInt(parts[2])));
                }
            }

            if (documentFrequency <= 0) {
                return;
            }

            double idf = Math.log((documentCount + 1.0d) / (documentFrequency + 1.0d)) + 1.0d;
            for (TermFrequencyRow row : rows) {
                double score = (1.0d + Math.log(row.getTermFrequency())) * idf;
                context.write(
                        new Text(term.toString() + "\t" + row.getDocumentId() + "\t" + row.getTermFrequency()),
                        new Text(String.format(Locale.ROOT, "%.6f", score)));
            }
        }
    }

    private static final class TermFrequencyRow {
        private final String documentId;
        private final int termFrequency;

        private TermFrequencyRow(String documentId, int termFrequency) {
            this.documentId = documentId;
            this.termFrequency = termFrequency;
        }

        private String getDocumentId() {
            return documentId;
        }

        private int getTermFrequency() {
            return termFrequency;
        }
    }
}
