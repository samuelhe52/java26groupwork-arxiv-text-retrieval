package com.java26groupwork.finalassignment.hadoop.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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

public final class InvertedIndexJob {

    public static final String DOCUMENT_COUNT_KEY = "app.corpus.document-count";
    public static final String MAX_DOCUMENT_FREQUENCY_RATIO_KEY = "app.corpus.max-document-frequency-ratio";

    private InvertedIndexJob() {}

    public static Job createJob(
            Configuration configuration, Path tfIdfPath, Path documentFrequencyPath, Path outputPath)
            throws IOException {
        Job job = Job.getInstance(configuration, "arxiv-inverted-index");
        job.setJarByClass(InvertedIndexJob.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, tfIdfPath, TextInputFormat.class, InvertedIndexMapper.class);
        MultipleInputs.addInputPath(
                job, documentFrequencyPath, TextInputFormat.class, DocumentFrequencyInputMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

    public static final class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length < 4) {
                return;
            }
            context.write(new Text(parts[0]), new Text("POSTING\t" + parts[1] + "\t" + parts[2] + "\t" + parts[3]));
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

    public static final class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text term, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int documentCount = context.getConfiguration().getInt(DOCUMENT_COUNT_KEY, 1);
            double maxDocumentFrequencyRatio = context.getConfiguration()
                    .getDouble(MAX_DOCUMENT_FREQUENCY_RATIO_KEY, 1.0d);
            int documentFrequency = 0;
            List<PostingRow> postings = new ArrayList<>();
            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                if (parts.length < 2) {
                    continue;
                }
                if ("DF".equals(parts[0])) {
                    documentFrequency = Integer.parseInt(parts[1]);
                    continue;
                }
                if (!"POSTING".equals(parts[0]) || parts.length < 4) {
                    continue;
                }
                postings.add(new PostingRow(parts[1], Integer.parseInt(parts[2]), Double.parseDouble(parts[3])));
            }

            if (documentFrequency <= 0) {
                return;
            }

            double maxDocumentFrequency = documentCount * maxDocumentFrequencyRatio;
            if (documentFrequency > maxDocumentFrequency) {
                return;
            }

            postings.sort(Comparator.comparingDouble(PostingRow::getScore).reversed());
            for (PostingRow row : postings) {
                context.write(
                        term,
                        new Text(row.getDocumentId() + "\t" + row.getTermFrequency() + "\t" + row.getScore()));
            }
        }
    }

    private static final class PostingRow {
        private final String documentId;
        private final int termFrequency;
        private final double score;

        private PostingRow(String documentId, int termFrequency, double score) {
            this.documentId = documentId;
            this.termFrequency = termFrequency;
            this.score = score;
        }

        private String getDocumentId() {
            return documentId;
        }

        private int getTermFrequency() {
            return termFrequency;
        }

        private double getScore() {
            return score;
        }
    }
}
