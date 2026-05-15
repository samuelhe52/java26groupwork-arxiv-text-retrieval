package com.java26groupwork.finalassignment.hadoop.jobs;

import java.io.IOException;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.StringJoiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public final class DocumentKeywordsJob {

    public static final String KEYWORD_LIMIT_KEY = "app.corpus.keyword-limit";

    private DocumentKeywordsJob() {}

    public static Job createJob(Configuration configuration, Path inputPath, Path outputPath)
            throws IOException {
        Job job = Job.getInstance(configuration, "arxiv-document-keywords");
        job.setJarByClass(DocumentKeywordsJob.class);
        job.setMapperClass(DocumentKeywordsMapper.class);
        job.setReducerClass(DocumentKeywordsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

    public static final class DocumentKeywordsMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length < 3) {
                return;
            }
            context.write(new Text(parts[1]), new Text(parts[0] + "\t" + parts[2]));
        }
    }

    public static final class DocumentKeywordsReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text documentId, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int keywordLimit = context.getConfiguration().getInt(KEYWORD_LIMIT_KEY, 8);
            PriorityQueue<KeywordRow> keywords =
                    new PriorityQueue<>(Math.max(1, keywordLimit), DocumentKeywordsJob::compareKeywordRows);
            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                if (parts.length < 2) {
                    continue;
                }
                collectTopKeyword(
                        keywords,
                        new KeywordRow(parts[0], Double.parseDouble(parts[1])),
                        keywordLimit);
            }
            var orderedKeywords = keywords.stream()
                    .sorted((left, right) -> compareKeywordRows(right, left))
                    .toList();
            StringJoiner joiner = new StringJoiner(",");
            for (KeywordRow row : orderedKeywords) {
                joiner.add(row.getTerm() + "|" + String.format(Locale.ROOT, "%.6f", row.getScore()));
            }
            context.write(documentId, new Text(joiner.toString()));
        }
    }

    private static int compareKeywordRows(KeywordRow left, KeywordRow right) {
        int scoreComparison = Double.compare(left.getScore(), right.getScore());
        if (scoreComparison != 0) {
            return scoreComparison;
        }
        return right.getTerm().compareTo(left.getTerm());
    }

    private static void collectTopKeyword(
            PriorityQueue<KeywordRow> keywords,
            KeywordRow candidate,
            int keywordLimit) {
        if (keywordLimit <= 0) {
            return;
        }
        if (keywords.size() < keywordLimit) {
            keywords.offer(candidate);
            return;
        }
        KeywordRow smallest = keywords.peek();
        if (smallest != null && compareKeywordRows(candidate, smallest) > 0) {
            keywords.poll();
            keywords.offer(candidate);
        }
    }

    private static final class KeywordRow {
        private final String term;
        private final double score;

        private KeywordRow(String term, double score) {
            this.term = term;
            this.score = score;
        }

        private String getTerm() {
            return term;
        }

        private double getScore() {
            return score;
        }
    }
}
