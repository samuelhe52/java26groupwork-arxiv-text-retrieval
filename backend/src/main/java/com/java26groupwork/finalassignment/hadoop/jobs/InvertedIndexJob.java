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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public final class InvertedIndexJob {

    private InvertedIndexJob() {}

    public static Job createJob(Configuration configuration, Path inputPath, Path outputPath)
            throws IOException {
        Job job = Job.getInstance(configuration, "arxiv-inverted-index");
        job.setJarByClass(InvertedIndexJob.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
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
            context.write(new Text(parts[0]), new Text(parts[1] + "\t" + parts[2] + "\t" + parts[3]));
        }
    }

    public static final class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text term, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<PostingRow> postings = new ArrayList<>();
            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                if (parts.length < 3) {
                    continue;
                }
                postings.add(new PostingRow(parts[0], Integer.parseInt(parts[1]), Double.parseDouble(parts[2])));
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
