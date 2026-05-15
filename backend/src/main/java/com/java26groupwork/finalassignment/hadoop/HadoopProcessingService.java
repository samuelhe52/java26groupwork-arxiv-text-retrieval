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
import org.apache.hadoop.mapreduce.Job;
import org.springframework.stereotype.Service;

import com.java26groupwork.finalassignment.hadoop.jobs.DocumentFrequencyJob;
import com.java26groupwork.finalassignment.hadoop.jobs.DocumentKeywordsJob;
import com.java26groupwork.finalassignment.hadoop.jobs.InvertedIndexJob;
import com.java26groupwork.finalassignment.hadoop.jobs.TermFrequencyJob;
import com.java26groupwork.finalassignment.hadoop.jobs.TfIdfJob;

@Service
public class HadoopProcessingService {

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
                ? "local-direct-indexer"
                : "hdfs-yarn-mapreduce-pipeline";
    }

    public boolean isLocalMode() {
        return properties.getMode() == HadoopProperties.Mode.LOCAL;
    }

    public HadoopConnectionDescription describeConnection() {
        return new HadoopConnectionDescription(
                properties.getMode().name().toLowerCase(),
                properties.getMode() == HadoopProperties.Mode.LOCAL
                        ? "local-direct"
                        : hadoopConfiguration.get("fs.defaultFS"),
                properties.getMode() == HadoopProperties.Mode.LOCAL
                        ? null
                        : hadoopConfiguration.get("dfs.nameservices"),
                properties.getMode() == HadoopProperties.Mode.LOCAL
                        ? null
                        : hadoopConfiguration.get("yarn.resourcemanager.cluster-id"),
                properties.getInputPath(),
                properties.getOutputPath(),
                properties.getConfigDir(),
                properties.getReducerTasks());
    }

    public ProcessingArtifacts processDataset(Path sourceDatasetDir, CorpusProperties corpusProperties) {
        if (isLocalMode()) {
            throw new IllegalStateException("Cluster Hadoop processing is not used in local mode.");
        }
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

            int reducerTasks = reducerTasksForCluster(stageResult);

            Job termFrequencyJob = TermFrequencyJob.createJob(
                    new org.apache.hadoop.conf.Configuration(configuration),
                    workPaths.input(),
                    workPaths.termFrequency());
            applyReducerTasks(termFrequencyJob, reducerTasks);
            runJob(termFrequencyJob);

            Job documentFrequencyJob = DocumentFrequencyJob.createJob(
                    new org.apache.hadoop.conf.Configuration(configuration),
                    workPaths.input(),
                    workPaths.documentFrequency());
            applyReducerTasks(documentFrequencyJob, reducerTasks);
            runJob(documentFrequencyJob);

            org.apache.hadoop.conf.Configuration tfIdfConfiguration =
                    new org.apache.hadoop.conf.Configuration(configuration);
            tfIdfConfiguration.setInt(TfIdfJob.DOCUMENT_COUNT_KEY, stageResult.documentCount());
            Job tfIdfJob = TfIdfJob.createJob(
                    tfIdfConfiguration,
                    workPaths.termFrequency(),
                    workPaths.documentFrequency(),
                    workPaths.tfIdf());
            applyReducerTasks(tfIdfJob, reducerTasks);
            runJob(tfIdfJob);

            org.apache.hadoop.conf.Configuration keywordsConfiguration =
                    new org.apache.hadoop.conf.Configuration(configuration);
            keywordsConfiguration.setInt(
                    DocumentKeywordsJob.KEYWORD_LIMIT_KEY, corpusProperties.getDocumentKeywordCount());
            Job documentKeywordsJob = DocumentKeywordsJob.createJob(
                    keywordsConfiguration,
                    workPaths.tfIdf(),
                    workPaths.documentKeywords());
            applyReducerTasks(documentKeywordsJob, reducerTasks);
            runJob(documentKeywordsJob);

            org.apache.hadoop.conf.Configuration invertedIndexConfiguration =
                    new org.apache.hadoop.conf.Configuration(configuration);
            invertedIndexConfiguration.setInt(InvertedIndexJob.DOCUMENT_COUNT_KEY, stageResult.documentCount());
            invertedIndexConfiguration.setDouble(
                    InvertedIndexJob.MAX_DOCUMENT_FREQUENCY_RATIO_KEY,
                    corpusProperties.getIndexMaxDocumentFrequencyRatio());
            Job invertedIndexJob = InvertedIndexJob.createJob(
                    invertedIndexConfiguration,
                    workPaths.tfIdf(),
                    workPaths.documentFrequency(),
                    workPaths.invertedIndex());
            applyReducerTasks(invertedIndexJob, reducerTasks);
            runJob(invertedIndexJob);

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

    public Path configuredDatasetDir() {
        String configuredInputPath = properties.getInputPath();
        if (configuredInputPath == null || configuredInputPath.isBlank()) {
            return null;
        }
        Path configuredPath = Path.of(configuredInputPath);
        if (!configuredPath.isAbsolute()) {
            configuredPath = Path.of("").toAbsolutePath().resolve(configuredPath);
        }
        configuredPath = configuredPath.normalize();
        if (isLocalMode()) {
            return normalizeLocalDatasetDir(configuredPath);
        }
        return configuredPath;
    }

    private Path normalizeLocalDatasetDir(Path configuredPath) {
        Path fileName = configuredPath.getFileName();
        if (fileName == null || !"years".equals(fileName.toString())) {
            return configuredPath;
        }

        Path parent = configuredPath.getParent();
        if (parent == null) {
            return configuredPath;
        }

        return Files.isDirectory(parent) ? parent : configuredPath;
    }

    private WorkPaths createWorkPaths(Path sourceDatasetDir) {
        String runId = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(java.time.LocalDateTime.now());
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
        if (Files.isDirectory(sourceDatasetDir) && containsJsonlShards(sourceDatasetDir)) {
            return stageLocalDataset(sourceDatasetDir, fileSystem, inputDirectory);
        }
        if (properties.getMode() == HadoopProperties.Mode.CLUSTER) {
            return stageClusterDataset(fileSystem, inputDirectory);
        }
        throw new IllegalArgumentException("Dataset years directory does not exist: " + yearsDir);
    }

    private boolean containsJsonlShards(Path directory) throws IOException {
        try (var stream = Files.list(directory)) {
            return stream.anyMatch(path -> path.getFileName().toString().endsWith(".jsonl"));
        }
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

            return new StageResult(documentCount, shards.size(), limitWarnings(warnings));
        }
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

            return new StageResult(documentCount, shards.size(), limitWarnings(warnings));
        }
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

    private void applyReducerTasks(Job job, int reducerTasks) {
        if (reducerTasks > 0) {
            job.setNumReduceTasks(reducerTasks);
        }
    }

    private int reducerTasksForCluster(StageResult stageResult) {
        if (properties.getMode() != HadoopProperties.Mode.CLUSTER) {
            return 1;
        }
        int configuredReducers = Math.max(1, properties.getReducerTasks());
        int shardBound = Math.max(1, stageResult.inputShardCount());
        return Math.min(configuredReducers, shardBound);
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

    private record StageResult(int documentCount, int inputShardCount, List<String> warnings) {}

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
