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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public final class ScoredTermsJob {

    public static final String DOCUMENT_COUNT_KEY = "app.corpus.document-count";
    public static final String MAX_DOCUMENT_FREQUENCY_RATIO_KEY = "app.corpus.max-document-frequency-ratio";

    public static final String TF_IDF_DIRECTORY_NAME = "tfidf";
    public static final String INVERTED_INDEX_DIRECTORY_NAME = "index";

    private static final String TF_IDF_OUTPUT_NAME = "tfIdf";
    private static final String INVERTED_INDEX_OUTPUT_NAME = "invertedIndex";
    private static final String TF_IDF_BASE_OUTPUT_PATH = TF_IDF_DIRECTORY_NAME + "/part";
    private static final String INVERTED_INDEX_BASE_OUTPUT_PATH = INVERTED_INDEX_DIRECTORY_NAME + "/part";

    private ScoredTermsJob() {}

    public static Job createJob(
            Configuration configuration, Path termFrequencyPath, Path documentFrequencyPath, Path outputPath)
            throws IOException {
        Job job = Job.getInstance(configuration, "arxiv-scored-terms");
        job.setJarByClass(ScoredTermsJob.class);
        job.setReducerClass(ScoredTermsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, termFrequencyPath, TextInputFormat.class, TermFrequencyInputMapper.class);
        MultipleInputs.addInputPath(
                job, documentFrequencyPath, TextInputFormat.class, DocumentFrequencyInputMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, TF_IDF_OUTPUT_NAME, TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(
                job, INVERTED_INDEX_OUTPUT_NAME, TextOutputFormat.class, Text.class, Text.class);
        return job;
    }

    public static final class TermFrequencyInputMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t", 3);
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
            String[] parts = value.toString().split("\t", 2);
            if (parts.length < 2) {
                return;
            }
            context.write(new Text(parts[0]), new Text("DF\t" + parts[1]));
        }
    }

    public static final class ScoredTermsReducer extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs<Text, Text> outputs;

        @Override
        protected void setup(Context context) {
            outputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text term, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int documentCount = context.getConfiguration().getInt(DOCUMENT_COUNT_KEY, 1);
            double maxDocumentFrequencyRatio = context.getConfiguration()
                    .getDouble(MAX_DOCUMENT_FREQUENCY_RATIO_KEY, 1.0d);
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

            String termText = term.toString();
            double idf = Math.log((documentCount + 1.0d) / (documentFrequency + 1.0d)) + 1.0d;
            double maxDocumentFrequency = documentCount * maxDocumentFrequencyRatio;
            boolean includeInIndex = documentFrequency <= maxDocumentFrequency;

            for (TermFrequencyRow row : rows) {
                double score = (1.0d + Math.log(row.termFrequency())) * idf;
                String formattedScore = String.format(Locale.ROOT, "%.6f", score);
                outputs.write(
                        TF_IDF_OUTPUT_NAME,
                        new Text(termText + "\t" + row.documentId()),
                        new Text(formattedScore),
                        TF_IDF_BASE_OUTPUT_PATH);
                if (includeInIndex) {
                    outputs.write(
                            INVERTED_INDEX_OUTPUT_NAME,
                            new Text(termText),
                            new Text(row.documentId() + "\t" + formattedScore),
                            INVERTED_INDEX_BASE_OUTPUT_PATH);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputs.close();
        }
    }

    private record TermFrequencyRow(String documentId, int termFrequency) {}
}
