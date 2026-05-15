package com.java26groupwork.finalassignment.hadoop;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.java26groupwork.finalassignment.corpus.CorpusProperties;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.springframework.stereotype.Service;

import com.java26groupwork.finalassignment.hadoop.jobs.DocumentFrequencyJob;
import com.java26groupwork.finalassignment.hadoop.jobs.DocumentKeywordsJob;
import com.java26groupwork.finalassignment.hadoop.jobs.InvertedIndexJob;
import com.java26groupwork.finalassignment.hadoop.jobs.TermFrequencyJob;
import com.java26groupwork.finalassignment.hadoop.jobs.TfIdfJob;

@Service
public class HadoopProcessingService {

    private static final PathFilter PART_FILE_FILTER =
            path -> path.getName().startsWith("part-");

    private final HadoopProperties properties;
    private final org.apache.hadoop.conf.Configuration hadoopConfiguration;
    private final ObjectMapper objectMapper;

    public HadoopProcessingService(
            HadoopProperties properties,
            org.apache.hadoop.conf.Configuration hadoopConfiguration,
            ObjectMapper objectMapper) {
        this.properties = properties;
        this.hadoopConfiguration = hadoopConfiguration;
        this.objectMapper = objectMapper;
    }

    public String describe() {
        return properties.getMode() == HadoopProperties.Mode.LOCAL
                ? "local-mapreduce-pipeline"
                : "hdfs-yarn-mapreduce-pipeline";
    }

    public HadoopConnectionDescription describeConnection() {
        return new HadoopConnectionDescription(
                properties.getMode().name().toLowerCase(),
                hadoopConfiguration.get("fs.defaultFS"),
                hadoopConfiguration.get("dfs.nameservices"),
                hadoopConfiguration.get("yarn.resourcemanager.cluster-id"),
                properties.getInputPath(),
                properties.getOutputPath(),
                properties.getLocalBasePath(),
                properties.getConfigDir());
    }

    public ProcessingArtifacts processDataset(Path sourceDatasetDir, CorpusProperties corpusProperties) {
        long startNanos = System.nanoTime();
        Instant startedAt = Instant.now();
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration(hadoopConfiguration);

        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            WorkPaths workPaths = createWorkPaths(sourceDatasetDir);
            deleteIfExists(fileSystem, workPaths.root());
            fileSystem.mkdirs(workPaths.root());
            fileSystem.mkdirs(workPaths.input());

            StageResult stageResult = stageDataset(sourceDatasetDir, fileSystem, workPaths.input());
            if (stageResult.documentCount() == 0) {
                return new ProcessingArtifacts(
                        sourceDatasetDir.toAbsolutePath().normalize(),
                        workPaths.root(),
                        workPaths.input(),
                        workPaths.termFrequency(),
                        workPaths.documentFrequency(),
                        workPaths.tfIdf(),
                        workPaths.documentKeywords(),
                        workPaths.invertedIndex(),
                        startedAt,
                        elapsedMillis(startNanos),
                        0,
                        List.copyOf(stageResult.warnings()));
            }

            runJob(TermFrequencyJob.createJob(new org.apache.hadoop.conf.Configuration(configuration),
                    workPaths.input(), workPaths.termFrequency()));
            runJob(DocumentFrequencyJob.createJob(new org.apache.hadoop.conf.Configuration(configuration),
                    workPaths.input(), workPaths.documentFrequency()));

            org.apache.hadoop.conf.Configuration tfIdfConfiguration =
                    new org.apache.hadoop.conf.Configuration(configuration);
            tfIdfConfiguration.setInt(TfIdfJob.DOCUMENT_COUNT_KEY, stageResult.documentCount());
            runJob(TfIdfJob.createJob(
                    tfIdfConfiguration, workPaths.termFrequency(), workPaths.documentFrequency(), workPaths.tfIdf()));

            org.apache.hadoop.conf.Configuration keywordsConfiguration =
                    new org.apache.hadoop.conf.Configuration(configuration);
            keywordsConfiguration.setInt(
                    DocumentKeywordsJob.KEYWORD_LIMIT_KEY, corpusProperties.getDocumentKeywordCount());
            runJob(DocumentKeywordsJob.createJob(
                    keywordsConfiguration, workPaths.tfIdf(), workPaths.documentKeywords()));

            org.apache.hadoop.conf.Configuration invertedIndexConfiguration =
                    new org.apache.hadoop.conf.Configuration(configuration);
            invertedIndexConfiguration.setInt(InvertedIndexJob.DOCUMENT_COUNT_KEY, stageResult.documentCount());
            invertedIndexConfiguration.setDouble(
                    InvertedIndexJob.MAX_DOCUMENT_FREQUENCY_RATIO_KEY,
                    corpusProperties.getIndexMaxDocumentFrequencyRatio());
            runJob(InvertedIndexJob.createJob(
                    invertedIndexConfiguration,
                    workPaths.tfIdf(),
                    workPaths.documentFrequency(),
                    workPaths.invertedIndex()));

            return new ProcessingArtifacts(
                    sourceDatasetDir.toAbsolutePath().normalize(),
                    workPaths.root(),
                    workPaths.input(),
                    workPaths.termFrequency(),
                    workPaths.documentFrequency(),
                    workPaths.tfIdf(),
                    workPaths.documentKeywords(),
                    workPaths.invertedIndex(),
                    startedAt,
                    elapsedMillis(startNanos),
                    stageResult.documentCount(),
                    List.copyOf(stageResult.warnings()));
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to execute Hadoop processing pipeline.", exception);
        }
    }

    public FileSystem fileSystem() throws IOException {
        return FileSystem.get(new org.apache.hadoop.conf.Configuration(hadoopConfiguration));
    }

    public List<org.apache.hadoop.fs.Path> outputPartFiles(org.apache.hadoop.fs.Path directory) throws IOException {
        FileStatus[] statuses = fileSystem().listStatus(directory, PART_FILE_FILTER);
        ArrayList<org.apache.hadoop.fs.Path> partFiles = new ArrayList<>(statuses.length);
        for (FileStatus status : statuses) {
            if (status.isFile()) {
                partFiles.add(status.getPath());
            }
        }
        partFiles.sort(java.util.Comparator.comparing(org.apache.hadoop.fs.Path::toString));
        return List.copyOf(partFiles);
    }

    private WorkPaths createWorkPaths(Path sourceDatasetDir) {
        String runId = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(java.time.LocalDateTime.now());
        if (properties.getMode() == HadoopProperties.Mode.LOCAL) {
            Path localRoot = Path.of(properties.getLocalBasePath())
                    .resolve("analyses")
                    .resolve(sourceDatasetDir.getFileName() + "-" + runId)
                    .toAbsolutePath()
                    .normalize();
            org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path(localRoot.toUri().toString());
            return new WorkPaths(
                    root,
                    new org.apache.hadoop.fs.Path(root, "input"),
                    new org.apache.hadoop.fs.Path(root, "tf"),
                    new org.apache.hadoop.fs.Path(root, "df"),
                    new org.apache.hadoop.fs.Path(root, "tfidf"),
                    new org.apache.hadoop.fs.Path(root, "keywords"),
                    new org.apache.hadoop.fs.Path(root, "index"));
        }

        org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path(
                new org.apache.hadoop.fs.Path(properties.getOutputPath()),
                "analysis-" + runId);
        return new WorkPaths(
                root,
                new org.apache.hadoop.fs.Path(root, "input"),
                new org.apache.hadoop.fs.Path(root, "tf"),
                new org.apache.hadoop.fs.Path(root, "df"),
                new org.apache.hadoop.fs.Path(root, "tfidf"),
                new org.apache.hadoop.fs.Path(root, "keywords"),
                new org.apache.hadoop.fs.Path(root, "index"));
    }

    private StageResult stageDataset(
            Path sourceDatasetDir, FileSystem fileSystem, org.apache.hadoop.fs.Path inputDirectory) throws IOException {
        Path yearsDir = sourceDatasetDir.resolve("years");
        if (Files.isDirectory(yearsDir)) {
            return stageLocalDataset(yearsDir, fileSystem, inputDirectory);
        }
        if (properties.getMode() == HadoopProperties.Mode.CLUSTER) {
            return stageClusterDataset(fileSystem, inputDirectory);
        }
        throw new IllegalArgumentException("Dataset years directory does not exist: " + yearsDir);
    }

    private StageResult stageLocalDataset(
            Path yearsDir, FileSystem fileSystem, org.apache.hadoop.fs.Path inputDirectory) throws IOException {
        List<String> warnings = new ArrayList<>();
        Set<String> seenDocumentIds = new LinkedHashSet<>();
        int documentCount = 0;

        try (StagingWriters writers = new StagingWriters(fileSystem, inputDirectory, objectMapper)) {
            List<Path> shards;
            try (var stream = Files.list(yearsDir)) {
                shards = stream
                        .filter(path -> path.getFileName().toString().endsWith(".jsonl"))
                        .sorted()
                        .toList();
            }

            for (Path shard : shards) {
                try (BufferedReader reader = Files.newBufferedReader(shard, StandardCharsets.UTF_8)) {
                    documentCount += stageRecords(reader, writers, warnings, seenDocumentIds);
                }
            }
        }

        return new StageResult(documentCount, limitWarnings(warnings));
    }

    private StageResult stageClusterDataset(FileSystem fileSystem, org.apache.hadoop.fs.Path inputDirectory)
            throws IOException {
        String configuredInputPath = properties.getInputPath();
        if (configuredInputPath == null || configuredInputPath.isBlank()) {
            throw new IllegalArgumentException("app.hadoop.input-path must be configured for cluster mode.");
        }

        org.apache.hadoop.fs.Path sourceInputPath = new org.apache.hadoop.fs.Path(configuredInputPath);
        if (!fileSystem.exists(sourceInputPath)) {
            throw new IllegalArgumentException("Configured Hadoop input path does not exist: " + sourceInputPath);
        }

        List<String> warnings = new ArrayList<>();
        Set<String> seenDocumentIds = new LinkedHashSet<>();
        int documentCount = 0;

        try (StagingWriters writers = new StagingWriters(fileSystem, inputDirectory, objectMapper)) {
            FileStatus[] statuses = fileSystem.listStatus(sourceInputPath);
            ArrayList<org.apache.hadoop.fs.Path> shards = new ArrayList<>();
            for (FileStatus status : statuses) {
                if (status.isFile() && status.getPath().getName().endsWith(".jsonl")) {
                    shards.add(status.getPath());
                }
            }
            shards.sort(java.util.Comparator.comparing(org.apache.hadoop.fs.Path::toString));

            for (org.apache.hadoop.fs.Path shard : shards) {
                try (BufferedReader reader = new BufferedReader(
                        new java.io.InputStreamReader(fileSystem.open(shard), StandardCharsets.UTF_8))) {
                    documentCount += stageRecords(reader, writers, warnings, seenDocumentIds);
                }
            }
        }

        return new StageResult(documentCount, limitWarnings(warnings));
    }

    private int stageRecords(
            BufferedReader reader,
            StagingWriters writers,
            List<String> warnings,
            Set<String> seenDocumentIds) throws IOException {
        int documentCount = 0;
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isBlank()) {
                continue;
            }
            JsonNode node = objectMapper.readTree(line);
            String documentId = node.path("id").asText("").trim();
            if (documentId.isBlank()) {
                warnings.add("Skipped a record with a blank id while staging Hadoop input.");
                continue;
            }
            if (!seenDocumentIds.add(documentId)) {
                warnings.add("Skipped duplicate document id during staging: " + documentId);
                continue;
            }
            int year = node.path("year").asInt(0);
            writers.write(year, node);
            documentCount++;
        }
        return documentCount;
    }

    private void runJob(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        if (!job.waitForCompletion(true)) {
            throw new IllegalStateException("Hadoop job failed: " + job.getJobName());
        }
    }

    private void deleteIfExists(FileSystem fileSystem, org.apache.hadoop.fs.Path path) throws IOException {
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

    private List<String> limitWarnings(List<String> warnings) {
        if (warnings.size() <= 25) {
            return List.copyOf(warnings);
        }
        ArrayList<String> limited = new ArrayList<>(warnings.subList(0, 25));
        limited.add("Additional warnings omitted: " + (warnings.size() - 25));
        return List.copyOf(limited);
    }

    private static long elapsedMillis(long startNanos) {
        return Math.round((System.nanoTime() - startNanos) / 1_000_000.0d);
    }

    public record ProcessingArtifacts(
            Path sourceDatasetDir,
            org.apache.hadoop.fs.Path workRoot,
            org.apache.hadoop.fs.Path stagedInputDir,
            org.apache.hadoop.fs.Path termFrequencyDir,
            org.apache.hadoop.fs.Path documentFrequencyDir,
            org.apache.hadoop.fs.Path tfIdfDir,
            org.apache.hadoop.fs.Path documentKeywordsDir,
            org.apache.hadoop.fs.Path invertedIndexDir,
            Instant builtAt,
            long buildMillis,
            int documentCount,
            List<String> warnings) {}

    private record WorkPaths(
            org.apache.hadoop.fs.Path root,
            org.apache.hadoop.fs.Path input,
            org.apache.hadoop.fs.Path termFrequency,
            org.apache.hadoop.fs.Path documentFrequency,
            org.apache.hadoop.fs.Path tfIdf,
            org.apache.hadoop.fs.Path documentKeywords,
            org.apache.hadoop.fs.Path invertedIndex) {}

    private record StageResult(int documentCount, List<String> warnings) {}

    private static final class StagingWriters implements AutoCloseable {
        private final FileSystem fileSystem;
        private final org.apache.hadoop.fs.Path inputDirectory;
        private final ObjectMapper objectMapper;
        private final java.util.Map<Integer, BufferedWriter> writers = new java.util.HashMap<>();

        private StagingWriters(
                FileSystem fileSystem, org.apache.hadoop.fs.Path inputDirectory, ObjectMapper objectMapper) {
            this.fileSystem = fileSystem;
            this.inputDirectory = inputDirectory;
            this.objectMapper = objectMapper;
        }

        private void write(int year, JsonNode node) throws IOException {
            BufferedWriter writer = writers.computeIfAbsent(year, this::openWriter);
            writer.write(objectMapper.writeValueAsString(node));
            writer.newLine();
        }

        private BufferedWriter openWriter(int year) {
            org.apache.hadoop.fs.Path shardPath =
                    new org.apache.hadoop.fs.Path(inputDirectory, String.format("year_%04d.jsonl", Math.max(0, year)));
            try {
                return new BufferedWriter(new OutputStreamWriter(fileSystem.create(shardPath, true), StandardCharsets.UTF_8));
            } catch (IOException exception) {
                throw new UncheckedIOException("Failed to create staged Hadoop input shard: " + shardPath, exception);
            }
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            for (BufferedWriter writer : writers.values()) {
                try {
                    writer.close();
                } catch (IOException exception) {
                    if (failure == null) {
                        failure = exception;
                    } else {
                        failure.addSuppressed(exception);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }
}
