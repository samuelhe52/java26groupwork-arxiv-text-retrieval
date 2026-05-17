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

import com.java26groupwork.finalassignment.hadoop.jobs.DocumentKeywordsJob;
import com.java26groupwork.finalassignment.hadoop.jobs.ScoredTermsJob;
import com.java26groupwork.finalassignment.hadoop.jobs.TermStatisticsJob;

@Service
public class HadoopProcessingService {

    private static final int MAX_CLUSTER_NODE_BUDGET = 4;

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
                properties.getReducerTasks(),
                properties.getJobJar());
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
            WorkPaths workPaths = createWorkPaths();
            deleteIfExists(fileSystem, workPaths.root());
            fileSystem.mkdirs(workPaths.root());
            fileSystem.mkdirs(workPaths.input());

            StageResult stageResult = stageDataset(sourceDatasetDir, fileSystem, workPaths.input());
            if (stageResult.documentCount() == 0) {
                return new ProcessingArtifacts(
                        sourceDatasetDir.toAbsolutePath().normalize(),
                        workPaths.root(),
                        stageResult.inputPath(),
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

            ClusterExecutionPlan executionPlan = clusterExecutionPlan(properties.getReducerTasks());

            Job termStatisticsJob = TermStatisticsJob.createJob(
                    new org.apache.hadoop.conf.Configuration(configuration),
                    stageResult.inputPath(),
                    workPaths.termStatisticsRoot());
            applyJobJar(termStatisticsJob);
            applyReducerTasks(termStatisticsJob, executionPlan.termStatisticsReducers());
            runJob(termStatisticsJob);
            ensureDirectoriesExist(fileSystem, workPaths.termFrequency(), workPaths.documentFrequency());

            org.apache.hadoop.conf.Configuration scoredTermsConfiguration =
                    new org.apache.hadoop.conf.Configuration(configuration);
            scoredTermsConfiguration.setInt(ScoredTermsJob.DOCUMENT_COUNT_KEY, stageResult.documentCount());
            scoredTermsConfiguration.setDouble(
                    ScoredTermsJob.MAX_DOCUMENT_FREQUENCY_RATIO_KEY,
                    corpusProperties.getIndexMaxDocumentFrequencyRatio());
            Job scoredTermsJob = ScoredTermsJob.createJob(
                    scoredTermsConfiguration,
                    workPaths.termFrequency(),
                    workPaths.documentFrequency(),
                    workPaths.scoredTermsRoot());
            applyJobJar(scoredTermsJob);
            applyReducerTasks(scoredTermsJob, executionPlan.scoredTermsReducers());
            runJob(scoredTermsJob);
            ensureDirectoriesExist(fileSystem, workPaths.tfIdf(), workPaths.invertedIndex());

            org.apache.hadoop.conf.Configuration keywordsConfiguration =
                    new org.apache.hadoop.conf.Configuration(configuration);
            keywordsConfiguration.setInt(
                    DocumentKeywordsJob.KEYWORD_LIMIT_KEY, corpusProperties.getDocumentKeywordCount());
            Job documentKeywordsJob = DocumentKeywordsJob.createJob(
                    keywordsConfiguration,
                    workPaths.tfIdf(),
                    workPaths.documentKeywords());
            applyJobJar(documentKeywordsJob);
            applyReducerTasks(documentKeywordsJob, executionPlan.documentKeywordsReducers());
            runJob(documentKeywordsJob);

            return new ProcessingArtifacts(
                    sourceDatasetDir.toAbsolutePath().normalize(),
                    workPaths.root(),
                    stageResult.inputPath(),
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

    private WorkPaths createWorkPaths() {
        String runId = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(java.time.LocalDateTime.now());
        org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path(
                new org.apache.hadoop.fs.Path(properties.getOutputPath()),
                "analysis-" + runId);
        org.apache.hadoop.fs.Path termStatisticsRoot = new org.apache.hadoop.fs.Path(root, "term-statistics");
        org.apache.hadoop.fs.Path scoredTermsRoot = new org.apache.hadoop.fs.Path(root, "scored-terms");
        return new WorkPaths(
                root,
                new org.apache.hadoop.fs.Path(root, "input"),
                termStatisticsRoot,
                new org.apache.hadoop.fs.Path(termStatisticsRoot, TermStatisticsJob.TERM_FREQUENCY_DIRECTORY_NAME),
                new org.apache.hadoop.fs.Path(termStatisticsRoot, TermStatisticsJob.DOCUMENT_FREQUENCY_DIRECTORY_NAME),
                scoredTermsRoot,
                new org.apache.hadoop.fs.Path(scoredTermsRoot, ScoredTermsJob.TF_IDF_DIRECTORY_NAME),
                new org.apache.hadoop.fs.Path(root, "keywords"),
                new org.apache.hadoop.fs.Path(scoredTermsRoot, ScoredTermsJob.INVERTED_INDEX_DIRECTORY_NAME));
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

        try (StagingWriters writers = new StagingWriters(fileSystem, inputDirectory)) {
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

            return new StageResult(inputDirectory, documentCount, shards.size(), limitWarnings(warnings));
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

        PreparedClusterInput preparedClusterInput = resolvePreparedClusterInput(fileSystem, sourceInputPath);
        if (preparedClusterInput != null) {
            return new StageResult(
                    preparedClusterInput.inputPath(),
                    preparedClusterInput.documentCount(),
                    preparedClusterInput.inputShardCount(),
                    List.of());
        }

        List<String> warnings = new ArrayList<>();
        Set<String> seenDocumentIds = new LinkedHashSet<>();
        int documentCount = 0;
        List<org.apache.hadoop.fs.Path> shards = listJsonlInputs(fileSystem, sourceInputPath);
        if (shards.isEmpty()) {
            throw new IllegalArgumentException(
                    "Configured Hadoop input path must be a .jsonl file, a shard directory, or a dataset root with years/ shards: "
                            + sourceInputPath);
        }

        try (StagingWriters writers = new StagingWriters(fileSystem, inputDirectory)) {
            for (org.apache.hadoop.fs.Path shard : shards) {
                try (BufferedReader reader = new BufferedReader(
                        new java.io.InputStreamReader(fileSystem.open(shard), StandardCharsets.UTF_8))) {
                    documentCount += stageRecords(reader, writers, warnings, seenDocumentIds);
                }
            }

            return new StageResult(inputDirectory, documentCount, shards.size(), limitWarnings(warnings));
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
            writers.write(year, line);
            documentCount++;
        }
        return documentCount;
    }

    private PreparedClusterInput resolvePreparedClusterInput(
            FileSystem fileSystem, org.apache.hadoop.fs.Path sourceInputPath) throws IOException {
        List<org.apache.hadoop.fs.Path> directShards = listJsonlInputs(fileSystem, sourceInputPath);
        if (!directShards.isEmpty() && canReusePreparedClusterInput(fileSystem, sourceInputPath)) {
            return new PreparedClusterInput(
                    sourceInputPath,
                    resolveDocumentCount(fileSystem, sourceInputPath, directShards),
                    directShards.size());
        }

        org.apache.hadoop.fs.Path yearsPath = new org.apache.hadoop.fs.Path(sourceInputPath, "years");
        List<org.apache.hadoop.fs.Path> yearShards = listJsonlInputs(fileSystem, yearsPath);
        if (!yearShards.isEmpty()) {
            return new PreparedClusterInput(
                    yearsPath,
                    resolveDocumentCount(fileSystem, yearsPath, yearShards),
                    yearShards.size());
        }
        return null;
    }

    private boolean canReusePreparedClusterInput(FileSystem fileSystem, org.apache.hadoop.fs.Path inputPath)
            throws IOException {
        String fileName = inputPath.getName();
        if ("years".equals(fileName)) {
            return true;
        }
        return readManifestDocumentCount(fileSystem, inputPath) != null;
    }

    private int resolveDocumentCount(
            FileSystem fileSystem,
            org.apache.hadoop.fs.Path inputPath,
            List<org.apache.hadoop.fs.Path> shards) throws IOException {
        Integer manifestDocumentCount = readManifestDocumentCount(fileSystem, inputPath);
        if (manifestDocumentCount != null && manifestDocumentCount >= 0) {
            return manifestDocumentCount;
        }
        return countNonBlankLines(fileSystem, shards);
    }

    private Integer readManifestDocumentCount(FileSystem fileSystem, org.apache.hadoop.fs.Path inputPath)
            throws IOException {
        FileStatus status = fileSystem.getFileStatus(inputPath);
        ArrayList<org.apache.hadoop.fs.Path> manifestCandidates = new ArrayList<>(2);
        if (status.isDirectory()) {
            manifestCandidates.add(new org.apache.hadoop.fs.Path(inputPath, "manifest.json"));
        }
        org.apache.hadoop.fs.Path parent = inputPath.getParent();
        if (parent != null) {
            manifestCandidates.add(new org.apache.hadoop.fs.Path(parent, "manifest.json"));
        }

        for (org.apache.hadoop.fs.Path manifestPath : manifestCandidates) {
            if (!fileSystem.exists(manifestPath)) {
                continue;
            }
            try (var reader = new java.io.InputStreamReader(fileSystem.open(manifestPath), StandardCharsets.UTF_8)) {
                JsonNode totals = objectMapper.readTree(reader).path("totals");
                JsonNode records = totals.path("records");
                if (records.canConvertToInt()) {
                    return records.asInt();
                }
            }
        }
        return null;
    }

    private int countNonBlankLines(FileSystem fileSystem, List<org.apache.hadoop.fs.Path> shards) throws IOException {
        int documentCount = 0;
        for (org.apache.hadoop.fs.Path shard : shards) {
            try (BufferedReader reader = new BufferedReader(
                    new java.io.InputStreamReader(fileSystem.open(shard), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isBlank()) {
                        documentCount++;
                    }
                }
            }
        }
        return documentCount;
    }

    private List<org.apache.hadoop.fs.Path> listJsonlInputs(
            FileSystem fileSystem, org.apache.hadoop.fs.Path inputPath) throws IOException {
        if (!fileSystem.exists(inputPath)) {
            return List.of();
        }
        FileStatus status = fileSystem.getFileStatus(inputPath);
        if (status.isFile()) {
            return status.getPath().getName().endsWith(".jsonl")
                    ? List.of(status.getPath())
                    : List.of();
        }

        FileStatus[] statuses = fileSystem.listStatus(inputPath);
        ArrayList<org.apache.hadoop.fs.Path> shards = new ArrayList<>();
        for (FileStatus childStatus : statuses) {
            if (childStatus.isFile() && childStatus.getPath().getName().endsWith(".jsonl")) {
                shards.add(childStatus.getPath());
            }
        }
        shards.sort(java.util.Comparator.comparing(org.apache.hadoop.fs.Path::toString));
        return List.copyOf(shards);
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

    private void applyJobJar(Job job) {
        String configuredJobJar = properties.getJobJar();
        if (configuredJobJar == null || configuredJobJar.isBlank()) {
            return;
        }
        job.setJar(configuredJobJar);
    }

    // The merged pipeline now runs three sequential jobs, so each stage can use the
    // full cluster reducer budget up to the four-node cap.
    static ClusterExecutionPlan clusterExecutionPlan(int configuredReducerTasks) {
        int reducerBudget = Math.max(1, Math.min(MAX_CLUSTER_NODE_BUDGET, configuredReducerTasks));
        return new ClusterExecutionPlan(reducerBudget, reducerBudget, reducerBudget, reducerBudget);
    }

    private void deleteIfExists(FileSystem fileSystem, org.apache.hadoop.fs.Path path) throws IOException {
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

    private void ensureDirectoriesExist(FileSystem fileSystem, org.apache.hadoop.fs.Path... paths) throws IOException {
        for (org.apache.hadoop.fs.Path path : paths) {
            fileSystem.mkdirs(path);
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
            org.apache.hadoop.fs.Path termStatisticsRoot,
            org.apache.hadoop.fs.Path termFrequency,
            org.apache.hadoop.fs.Path documentFrequency,
            org.apache.hadoop.fs.Path scoredTermsRoot,
            org.apache.hadoop.fs.Path tfIdf,
            org.apache.hadoop.fs.Path documentKeywords,
            org.apache.hadoop.fs.Path invertedIndex) {}

    private record StageResult(
            org.apache.hadoop.fs.Path inputPath,
            int documentCount,
            int inputShardCount,
            List<String> warnings) {}

    private record PreparedClusterInput(
            org.apache.hadoop.fs.Path inputPath,
            int documentCount,
            int inputShardCount) {}

    static record ClusterExecutionPlan(
            int reducerBudget,
            int termStatisticsReducers,
            int scoredTermsReducers,
            int documentKeywordsReducers) {}

    private static final class StagingWriters implements AutoCloseable {
        private final FileSystem fileSystem;
        private final org.apache.hadoop.fs.Path inputDirectory;
        private final java.util.Map<Integer, BufferedWriter> writers = new java.util.HashMap<>();

        private StagingWriters(FileSystem fileSystem, org.apache.hadoop.fs.Path inputDirectory) {
            this.fileSystem = fileSystem;
            this.inputDirectory = inputDirectory;
        }

        private void write(int year, String jsonLine) throws IOException {
            BufferedWriter writer = writers.computeIfAbsent(year, this::openWriter);
            writer.write(jsonLine);
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
