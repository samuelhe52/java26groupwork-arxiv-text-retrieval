package com.java26groupwork.finalassignment.hadoop;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.java26groupwork.finalassignment.corpus.CorpusProperties;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.java26groupwork.finalassignment.hadoop.jobs.DocumentKeywordsJob;
import com.java26groupwork.finalassignment.hadoop.jobs.ScoredTermsJob;
import com.java26groupwork.finalassignment.hadoop.jobs.TermStatisticsJob;

@Service
public class HadoopProcessingService {

    private static final Logger log = LoggerFactory.getLogger(HadoopProcessingService.class);
    private static final int MAX_CLUSTER_NODE_BUDGET = 4;
    private static final ProcessingProgressReporter NOOP_PROGRESS_REPORTER = progress -> {};

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
                properties.getJobJar(),
                properties.getReplicationFactor());
    }

    public ProcessingArtifacts processDataset(Path sourceDatasetDir, CorpusProperties corpusProperties) {
        return processDataset(sourceDatasetDir, corpusProperties, NOOP_PROGRESS_REPORTER);
    }

    public ProcessingArtifacts processDataset(
            Path sourceDatasetDir,
            CorpusProperties corpusProperties,
            ProcessingProgressReporter progressReporter) {
        if (isLocalMode()) {
            throw new IllegalStateException("Cluster Hadoop processing is not used in local mode.");
        }
        long startNanos = System.nanoTime();
        Instant startedAt = Instant.now();
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration(hadoopConfiguration);
        configuration.setInt("dfs.replication", Math.max(1, properties.getReplicationFactor()));
        configuration.setInt(
                "mapreduce.client.submit.file.replication",
                Math.max(1, properties.getReplicationFactor()));

        try {
            reportProgress(progressReporter, "connect", "Connecting to HDFS/YARN.", 1, 6, 5, startNanos);
            log.info(
                    "Starting Hadoop processing for dataset={} mode={} fs={} outputRoot={} configuredInput={} reducerTasks={} replicationFactor={} jobJar={}",
                    sourceDatasetDir.toAbsolutePath().normalize(),
                    properties.getMode(),
                    configuration.get("fs.defaultFS"),
                    properties.getOutputPath(),
                    properties.getInputPath(),
                    properties.getReducerTasks(),
                    properties.getReplicationFactor(),
                    properties.getJobJar());
            FileSystem fileSystem = FileSystem.get(configuration);
            WorkPaths workPaths = createWorkPaths();
            log.info("Preparing Hadoop work directory: {}", workPaths.root());
            deleteIfExists(fileSystem, workPaths.root());
            fileSystem.mkdirs(workPaths.root());
            fileSystem.mkdirs(workPaths.input());

            reportProgress(progressReporter, "stage-input", "Preparing Hadoop input shards.", 2, 6, 10, startNanos);
            StageResult stageResult = stageDataset(sourceDatasetDir, fileSystem, workPaths.input());
            log.info(
                    "Hadoop input ready: inputPath={} shardCount={} manifestDocumentCount={}",
                    stageResult.inputPath(),
                    stageResult.inputShardCount(),
                    stageResult.documentCount());
            if (stageResult.documentCount() == 0) {
                reportProgress(progressReporter, "complete", "Dataset contains zero records; no Hadoop jobs needed.", 6, 6, 100, startNanos);
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
            log.info(
                    "Hadoop execution plan: termStatisticsReducers={} scoredTermsReducers={} documentKeywordsReducers={}",
                    executionPlan.termStatisticsReducers(),
                    executionPlan.scoredTermsReducers(),
                    executionPlan.documentKeywordsReducers());

            reportProgress(progressReporter, "term-statistics", "Submitting term statistics job.", 3, 6, 18, startNanos);
            Job termStatisticsJob = TermStatisticsJob.createJob(
                    new org.apache.hadoop.conf.Configuration(configuration),
                    stageResult.inputPath(),
                    workPaths.termStatisticsRoot());
            applyJobJar(termStatisticsJob);
            applyReducerTasks(termStatisticsJob, executionPlan.termStatisticsReducers());
            runJob(termStatisticsJob, progressReporter, startNanos, 3, 6, 18, 45);
            ensureDirectoriesExist(fileSystem, workPaths.termFrequency(), workPaths.documentFrequency());
            int documentCount = resolveProcessedDocumentCount(termStatisticsJob, stageResult);
            log.info("Term statistics completed: processedDocumentCount={}", documentCount);
            if (documentCount == 0) {
                reportProgress(progressReporter, "complete", "No tokenized documents were produced; index will be empty.", 6, 6, 100, startNanos);
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

            org.apache.hadoop.conf.Configuration scoredTermsConfiguration =
                    new org.apache.hadoop.conf.Configuration(configuration);
            scoredTermsConfiguration.setInt(ScoredTermsJob.DOCUMENT_COUNT_KEY, documentCount);
            scoredTermsConfiguration.setDouble(
                    ScoredTermsJob.MAX_DOCUMENT_FREQUENCY_RATIO_KEY,
                    corpusProperties.getIndexMaxDocumentFrequencyRatio());
            reportProgress(progressReporter, "scored-terms", "Submitting TF-IDF and inverted-index job.", 4, 6, 48, startNanos);
            Job scoredTermsJob = ScoredTermsJob.createJob(
                    scoredTermsConfiguration,
                    workPaths.termFrequency(),
                    workPaths.documentFrequency(),
                    workPaths.scoredTermsRoot());
            applyJobJar(scoredTermsJob);
            applyReducerTasks(scoredTermsJob, executionPlan.scoredTermsReducers());
            runJob(scoredTermsJob, progressReporter, startNanos, 4, 6, 48, 72);
            ensureDirectoriesExist(fileSystem, workPaths.tfIdf(), workPaths.invertedIndex());

            org.apache.hadoop.conf.Configuration keywordsConfiguration =
                    new org.apache.hadoop.conf.Configuration(configuration);
            keywordsConfiguration.setInt(
                    DocumentKeywordsJob.KEYWORD_LIMIT_KEY, corpusProperties.getDocumentKeywordCount());
            reportProgress(progressReporter, "document-keywords", "Submitting per-document keyword job.", 5, 6, 74, startNanos);
            Job documentKeywordsJob = DocumentKeywordsJob.createJob(
                    keywordsConfiguration,
                    workPaths.tfIdf(),
                    workPaths.documentKeywords());
            applyJobJar(documentKeywordsJob);
            applyReducerTasks(documentKeywordsJob, executionPlan.documentKeywordsReducers());
            runJob(documentKeywordsJob, progressReporter, startNanos, 5, 6, 74, 92);
            reportProgress(progressReporter, "snapshot", "Reading Hadoop outputs into the backend search snapshot.", 6, 6, 94, startNanos);

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
                    documentCount,
                    List.copyOf(stageResult.warnings()));
        } catch (Exception exception) {
            log.error("Hadoop processing pipeline failed.", exception);
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
        if (Files.isDirectory(sourceDatasetDir) && containsJsonlShards(sourceDatasetDir)) {
            return stageLocalDataset(sourceDatasetDir, fileSystem, inputDirectory);
        }
        Path yearsDir = sourceDatasetDir.resolve("years");
        if (Files.isDirectory(yearsDir)) {
            return stageLocalDataset(yearsDir, fileSystem, inputDirectory);
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
            Path shardDirectory, FileSystem fileSystem, org.apache.hadoop.fs.Path inputDirectory) throws IOException {
        List<String> warnings = new ArrayList<>();
        List<Path> shards;
        try (var stream = Files.list(shardDirectory)) {
            shards = stream
                    .filter(path -> path.getFileName().toString().endsWith(".jsonl"))
                    .sorted()
                    .toList();
        }
        for (Path shard : shards) {
            org.apache.hadoop.fs.Path destination =
                    new org.apache.hadoop.fs.Path(inputDirectory, shard.getFileName().toString());
            log.info("Copying local dataset shard to HDFS: source={} destination={}", shard, destination);
            fileSystem.copyFromLocalFile(false, true, new org.apache.hadoop.fs.Path(shard.toUri()), destination);
        }
        return new StageResult(
                inputDirectory,
                resolveLocalManifestDocumentCountOrUnknown(shardDirectory),
                shards.size(),
                limitWarnings(warnings));
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
            log.info(
                    "Using prepared HDFS input directly: inputPath={} shardCount={} manifestDocumentCount={}",
                    preparedClusterInput.inputPath(),
                    preparedClusterInput.inputShardCount(),
                    preparedClusterInput.documentCount());
            return new StageResult(
                    preparedClusterInput.inputPath(),
                    preparedClusterInput.documentCount(),
                    preparedClusterInput.inputShardCount(),
                    List.of());
        }

        List<org.apache.hadoop.fs.Path> shards = listJsonlInputs(fileSystem, sourceInputPath);
        if (shards.isEmpty()) {
            throw new IllegalArgumentException(
                    "Configured Hadoop input path must be a .jsonl file, a shard directory, or a dataset root with years/ shards: "
                            + sourceInputPath);
        }
        for (org.apache.hadoop.fs.Path shard : shards) {
            org.apache.hadoop.fs.Path destination =
                    new org.apache.hadoop.fs.Path(inputDirectory, shard.getName());
            log.info("Copying HDFS dataset shard: source={} destination={}", shard, destination);
            FileUtil.copy(fileSystem, shard, fileSystem, destination, false, hadoopConfiguration);
        }
        return new StageResult(inputDirectory, -1, shards.size(), List.of());
    }

    private PreparedClusterInput resolvePreparedClusterInput(
            FileSystem fileSystem, org.apache.hadoop.fs.Path sourceInputPath) throws IOException {
        List<org.apache.hadoop.fs.Path> directShards = listJsonlInputs(fileSystem, sourceInputPath);
        if (!directShards.isEmpty()) {
            return new PreparedClusterInput(
                    sourceInputPath,
                    resolveManifestDocumentCountOrUnknown(fileSystem, sourceInputPath),
                    directShards.size());
        }

        org.apache.hadoop.fs.Path yearsPath = new org.apache.hadoop.fs.Path(sourceInputPath, "years");
        List<org.apache.hadoop.fs.Path> yearShards = listJsonlInputs(fileSystem, yearsPath);
        if (!yearShards.isEmpty()) {
            return new PreparedClusterInput(
                    yearsPath,
                    resolveManifestDocumentCountOrUnknown(fileSystem, yearsPath),
                    yearShards.size());
        }
        return null;
    }

    private int resolveManifestDocumentCountOrUnknown(FileSystem fileSystem, org.apache.hadoop.fs.Path inputPath)
            throws IOException {
        Integer manifestDocumentCount = readManifestDocumentCount(fileSystem, inputPath);
        if (manifestDocumentCount != null && manifestDocumentCount >= 0) {
            return manifestDocumentCount;
        }
        return -1;
    }

    private int resolveLocalManifestDocumentCountOrUnknown(Path shardDirectory) throws IOException {
        Path manifestPath = shardDirectory.resolve("manifest.json");
        if (!Files.exists(manifestPath)) {
            Path parent = shardDirectory.getParent();
            manifestPath = parent == null ? manifestPath : parent.resolve("manifest.json");
        }
        if (!Files.exists(manifestPath)) {
            return -1;
        }
        try (BufferedReader reader = Files.newBufferedReader(manifestPath, StandardCharsets.UTF_8)) {
            JsonNode totals = objectMapper.readTree(reader).path("totals");
            JsonNode records = totals.path("records");
            return records.canConvertToInt() ? records.asInt() : -1;
        }
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

    private void runJob(
            Job job,
            ProcessingProgressReporter progressReporter,
            long pipelineStartNanos,
            int currentStep,
            int totalSteps,
            int startPercent,
            int endPercent)
            throws IOException, ClassNotFoundException, InterruptedException {
        String inputPaths = job.getConfiguration().get("mapreduce.input.fileinputformat.inputdir", "n/a");
        String outputPath = job.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir", "n/a");
        log.info(
                "Submitting Hadoop job: name={} reducers={} jar={} input={} output={}",
                job.getJobName(),
                job.getNumReduceTasks(),
                job.getJar(),
                inputPaths,
                outputPath);
        reportProgress(
                progressReporter,
                jobProgressStage(job),
                "Submitting " + job.getJobName() + ".",
                currentStep,
                totalSteps,
                startPercent,
                pipelineStartNanos);

        job.submit();
        log.info(
                "Hadoop job submitted: name={} jobId={} trackingUrl={}",
                job.getJobName(),
                job.getJobID(),
                job.getTrackingURL());

        while (!job.isComplete()) {
            float mapProgress = job.mapProgress();
            float reduceProgress = job.reduceProgress();
            int percent = interpolateProgress(startPercent, endPercent, mapProgress, reduceProgress);
            String message = "%s running: map %.0f%%, reduce %.0f%%"
                    .formatted(job.getJobName(), mapProgress * 100.0f, reduceProgress * 100.0f);
            reportProgress(
                    progressReporter,
                    jobProgressStage(job),
                    message,
                    currentStep,
                    totalSteps,
                    percent,
                    pipelineStartNanos);
            log.info(
                    "Hadoop job progress: name={} jobId={} state={} map={} reduce={} trackingUrl={}",
                    job.getJobName(),
                    job.getJobID(),
                    job.getJobState(),
                    String.format(java.util.Locale.ROOT, "%.1f%%", mapProgress * 100.0f),
                    String.format(java.util.Locale.ROOT, "%.1f%%", reduceProgress * 100.0f),
                    job.getTrackingURL());
            Thread.sleep(5_000L);
        }

        boolean successful = job.isSuccessful();
        log.info(
                "Hadoop job finished: name={} jobId={} successful={} state={} map={} reduce={}",
                job.getJobName(),
                job.getJobID(),
                successful,
                job.getJobState(),
                String.format(java.util.Locale.ROOT, "%.1f%%", job.mapProgress() * 100.0f),
                String.format(java.util.Locale.ROOT, "%.1f%%", job.reduceProgress() * 100.0f));
        reportProgress(
                progressReporter,
                jobProgressStage(job),
                job.getJobName() + (successful ? " completed." : " failed."),
                currentStep,
                totalSteps,
                successful ? endPercent : startPercent,
                pipelineStartNanos);

        if (!successful) {
            throw new IllegalStateException("Hadoop job failed: " + job.getJobName());
        }
    }

    private String jobProgressStage(Job job) {
        return job.getJobName().replace("arxiv-", "");
    }

    private int interpolateProgress(int startPercent, int endPercent, float mapProgress, float reduceProgress) {
        float jobProgress = (mapProgress * 0.55f) + (reduceProgress * 0.45f);
        return Math.max(startPercent, Math.min(endPercent, Math.round(startPercent + ((endPercent - startPercent) * jobProgress))));
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

    private int resolveProcessedDocumentCount(Job termStatisticsJob, StageResult stageResult) throws IOException {
        if (stageResult.documentCount() >= 0) {
            return stageResult.documentCount();
        }
        long counterValue = termStatisticsJob.getCounters()
                .findCounter(TermStatisticsJob.Counters.DOCUMENTS)
                .getValue();
        if (counterValue > Integer.MAX_VALUE) {
            throw new IllegalStateException("Hadoop input contains too many documents: " + counterValue);
        }
        return (int) counterValue;
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

    private void reportProgress(
            ProcessingProgressReporter progressReporter,
            String stage,
            String message,
            int currentStep,
            int totalSteps,
            int percent,
            long startNanos) {
        progressReporter.report(new ProcessingProgress(
                stage,
                message,
                currentStep,
                totalSteps,
                Math.max(0, Math.min(100, percent)),
                elapsedMillis(startNanos),
                Instant.now()));
    }

    @FunctionalInterface
    public interface ProcessingProgressReporter {
        void report(ProcessingProgress progress);
    }

    public record ProcessingProgress(
            String stage,
            String message,
            int currentStep,
            int totalSteps,
            int percent,
            long elapsedMillis,
            Instant updatedAt) {}

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

}
