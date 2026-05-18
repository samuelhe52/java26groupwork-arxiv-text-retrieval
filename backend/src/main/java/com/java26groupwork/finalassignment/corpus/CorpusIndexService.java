package com.java26groupwork.finalassignment.corpus;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.java26groupwork.finalassignment.hadoop.HadoopProcessingService;
import jakarta.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
public class CorpusIndexService {

    private static final Logger log = LoggerFactory.getLogger(CorpusIndexService.class);
    private static final List<String> WRAPPED_RECORD_FIELDS =
            List.of("data", "records", "papers", "items", "documents");
    private static final int MAX_LOCAL_ANALYSIS_WORKERS = 4;

    private final CorpusProperties properties;
    private final ObjectMapper objectMapper;
    private final HadoopProcessingService hadoopProcessingService;
    private final ExecutorService reloadExecutor;
    private volatile Path activeDatasetDir;
    private volatile CorpusIndexSnapshot snapshot;
    private volatile boolean reloadInProgress;
    private volatile Instant reloadRequestedAt;
    private volatile String lastReloadError;
    private volatile CorpusResponses.CorpusBuildProgress reloadProgress;

    public CorpusIndexService(
            CorpusProperties properties,
            ObjectMapper objectMapper,
            HadoopProcessingService hadoopProcessingService) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.hadoopProcessingService = hadoopProcessingService;
        this.reloadExecutor = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, "corpus-index-reload");
            thread.setDaemon(true);
            return thread;
        });
        this.activeDatasetDir = resolveConfiguredDatasetDir();
        this.snapshot = CorpusIndexSnapshot.empty(
                activeDatasetDir == null ? "" : activeDatasetDir.toString(),
                "not-analyzed",
                List.of());
    }

    public synchronized CorpusResponses.CorpusBuildSummary reload() {
        Path datasetDir = requireActiveDatasetDir();
        try {
            startProgress("starting", "Preparing corpus analysis.", 1, 6, 3);
            log.info("Starting corpus analysis: dataset={} mode={}", datasetDir, hadoopProcessingService.describe());
            this.snapshot = hadoopProcessingService.isLocalMode()
                    ? buildLocalSnapshot(datasetDir)
                    : buildSnapshot(hadoopProcessingService.processDataset(
                            datasetDir,
                            properties,
                            this::updateReloadProgress));
            this.lastReloadError = null;
            log.info(
                    "Corpus analysis completed: dataset={} status={} documents={} buildMillis={}",
                    datasetDir,
                    snapshot.buildSummary.getStatus(),
                    snapshot.recordCount(),
                    snapshot.buildSummary.getBuildMillis());
            return snapshot.buildSummary;
        } finally {
            this.reloadInProgress = false;
            this.reloadRequestedAt = null;
            this.reloadProgress = null;
        }
    }

    public synchronized CorpusResponses.CorpusBuildSummary requestReload() {
        if (reloadInProgress) {
            return buildSummary();
        }
        requireActiveDatasetDir();

        reloadInProgress = true;
        reloadRequestedAt = Instant.now();
        lastReloadError = null;
        startProgress("queued", "Analysis request accepted; waiting for the background worker.", 0, 6, 1);
        log.info("Corpus analysis requested: dataset={}", activeDatasetDir);

        reloadExecutor.execute(() -> {
            try {
                reload();
            } catch (RuntimeException exception) {
                lastReloadError = exception.getMessage() == null
                        ? exception.getClass().getSimpleName()
                        : exception.getMessage();
                log.error("Corpus analysis failed: dataset={} error={}", activeDatasetDir, lastReloadError, exception);
                reloadInProgress = false;
                reloadProgress = null;
            }
        });
        return buildSummary();
    }

    public synchronized CorpusResponses.CorpusUploadResponse importUploadedCorpus(List<MultipartFile> files) {
        if (reloadInProgress) {
            throw new IllegalArgumentException("An analysis job is already running. Wait for it to finish first.");
        }
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("Choose at least one .json or .jsonl file.");
        }

        List<MultipartFile> nonEmptyFiles = files.stream()
                .filter(file -> file != null && !file.isEmpty())
                .toList();
        if (nonEmptyFiles.isEmpty()) {
            throw new IllegalArgumentException("All selected files were empty.");
        }

        Path uploadDatasetDir = createUploadDatasetDir();
        List<String> uploadedFiles = new ArrayList<>(nonEmptyFiles.size());
        List<String> warnings = new ArrayList<>();
        AtomicLong importedRecordCount = new AtomicLong();

        try {
            Files.createDirectories(uploadDatasetDir);
            try (UploadWriter writer = new UploadWriter(uploadDatasetDir.resolve("upload.jsonl"), objectMapper)) {
                for (MultipartFile file : nonEmptyFiles) {
                    String filename = file.getOriginalFilename() == null ? "upload" : file.getOriginalFilename().trim();
                    uploadedFiles.add(filename.isBlank() ? "upload" : filename);
                    importFile(file, writer, warnings, importedRecordCount);
                }
            }
            writeUploadManifest(uploadDatasetDir, importedRecordCount.get(), 1);
        } catch (IOException exception) {
            throw new UncheckedIOException(describeUploadImportFailure(exception), exception);
        }

        if (importedRecordCount.get() == 0) {
            throw new IllegalArgumentException("No valid JSON records were found in the uploaded files.");
        }

        activeDatasetDir = uploadDatasetDir;
        reloadInProgress = false;
        reloadRequestedAt = null;
        lastReloadError = null;
        snapshot = CorpusIndexSnapshot.empty(
                uploadDatasetDir.toString(),
                "staged",
                limitWarnings(warnings));
        CorpusResponses.CorpusBuildSummary build = buildSummary();
        return new CorpusResponses.CorpusUploadResponse(
                build.getStatus(),
                uploadDatasetDir.toString(),
                nonEmptyFiles.size(),
                importedRecordCount.get(),
                List.copyOf(uploadedFiles),
                limitWarnings(warnings),
                build);
    }

    @PreDestroy
    public void destroy() {
        reloadExecutor.shutdownNow();
    }

    public boolean isReady() {
        return snapshot.ready;
    }

    public long documentCount() {
        return snapshot.recordCount();
    }

    public CorpusResponses.CorpusBuildSummary buildSummary() {
        CorpusIndexSnapshot current = snapshot;
        if (reloadInProgress) {
            String message = current.ready
                    ? "Analysis is running; serving the previous index until the new build finishes."
                    : "Analysis is running for the selected dataset.";
            return buildTransientSummary(current, "reloading", message);
        }
        if (lastReloadError != null && !lastReloadError.isBlank()) {
            return buildTransientSummary(current, "reload-failed", "Last analysis failed: " + lastReloadError);
        }
        return current.buildSummary;
    }

    public CorpusResponses.CorpusOverviewResponse overview() {
        CorpusIndexSnapshot current = snapshot;
        CorpusResponses.CorpusBuildSummary build = buildSummary();
        return new CorpusResponses.CorpusOverviewResponse(
                current.ready,
                hadoopProcessingService.describe(),
                current.datasetName,
                current.datasetDir,
                build.getStatus(),
                current.recordCount(),
                current.minYear,
                current.maxYear,
                current.topCategories,
                current.topTerms,
                current.yearSummaries,
                build);
    }

    public CorpusResponses.CorpusSearchResponse search(
            String query, Integer year, String category, Integer requestedLimit) {
        CorpusIndexSnapshot current = snapshot;
        long start = System.nanoTime();
        int limit = clampLimit(requestedLimit);
        List<String> queryTerms = CorpusTokenizer.tokenize(query);
        List<String> distinctQueryTerms = distinctTerms(queryTerms);
        List<String> warnings = new ArrayList<>(current.warnings);

        if (!current.ready) {
            warnings.add("Upload a dataset and submit analysis before searching.");
            return new CorpusResponses.CorpusSearchResponse(
                    false,
                    query == null ? "" : query,
                    queryTerms,
                    limit,
                    0L,
                    elapsedMillis(start),
                    List.copyOf(warnings),
                    List.of());
        }

        if (queryTerms.isEmpty()) {
            warnings.add("Enter at least one searchable term after stopword filtering.");
            return new CorpusResponses.CorpusSearchResponse(
                    true,
                    query == null ? "" : query,
                    queryTerms,
                    limit,
                    0L,
                    elapsedMillis(start),
                    List.copyOf(warnings),
                    List.of());
        }

        Map<Integer, Double> scores = new HashMap<>();
        Map<Integer, Set<String>> matches = new HashMap<>();
        for (String term : distinctQueryTerms) {
            CorpusIndexSnapshot.PostingList postingList = current.postings.get(term);
            if (postingList == null) {
                continue;
            }
            int[] docOrdinals = postingList.documentOrdinals();
            float[] tfIdfScores = postingList.tfIdfScores();
            for (int index = 0; index < docOrdinals.length; index++) {
                int docOrdinal = docOrdinals[index];
                StoredDocument document = current.documents.get(docOrdinal);
                if (!document.matchesYear(year) || !document.matchesCategory(category)) {
                    continue;
                }
                scores.merge(docOrdinal, (double) tfIdfScores[index], Double::sum);
                matches.computeIfAbsent(docOrdinal, ignored -> new HashSet<>()).add(term);
            }
        }

        List<SearchHit> topHits = selectTopHits(scores, limit);
        List<CorpusResponses.CorpusSearchResult> results = topHits.stream()
                .map(hit -> toSearchResult(
                        current,
                        current.documents.get(hit.documentOrdinal()),
                        hit.score(),
                        matches.getOrDefault(hit.documentOrdinal(), Set.of())))
                .toList();

        return new CorpusResponses.CorpusSearchResponse(
                true,
                query == null ? "" : query,
                queryTerms,
                limit,
                scores.size(),
                elapsedMillis(start),
                List.copyOf(warnings),
                results);
    }

    public CorpusResponses.DocumentDetailResponse documentById(String id) {
        CorpusIndexSnapshot current = snapshot;
        if (!current.ready) {
            return new CorpusResponses.DocumentDetailResponse(false, current.status, null, List.of());
        }
        Integer ordinal = current.documentOrdinalsById.get(id);
        if (ordinal == null) {
            return new CorpusResponses.DocumentDetailResponse(false, "not-found", null, List.of());
        }
        return new CorpusResponses.DocumentDetailResponse(
                true,
                "ok",
                current.documents.get(ordinal),
                current.documentKeywords.get(ordinal));
    }

    private CorpusIndexSnapshot buildSnapshot(HadoopProcessingService.ProcessingArtifacts artifacts) {
        if (artifacts.documentCount() <= 0) {
            return CorpusIndexSnapshot.empty(activeDatasetDir.toString(), "empty-dataset", artifacts.warnings());
        }

        long startNanos = System.nanoTime();
        List<String> warnings = new ArrayList<>(artifacts.warnings());

        try {
            FileSystem fileSystem = hadoopProcessingService.fileSystem();
            List<StoredDocument> documents = new ArrayList<>();
            Map<String, Integer> documentOrdinalsById = new HashMap<>();
            Map<String, Integer> primaryCategoryCounts = new HashMap<>();
            Map<String, Integer> documentYears = new HashMap<>();

            int[] minYear = {Integer.MAX_VALUE};
            int[] maxYear = {Integer.MIN_VALUE};

            forEachFileLine(fileSystem, artifacts.stagedInputDir(), false, line -> {
                JsonNode node = objectMapper.readTree(line);
                String documentId = CorpusTokenizer.normalize(node.path("id").asText(""));
                if (documentId.isBlank()) {
                    warnings.add("Skipped a staged record with blank id while building the search snapshot.");
                    return;
                }
                if (documentOrdinalsById.containsKey(documentId)) {
                    warnings.add("Skipped a duplicate staged id while building the search snapshot: " + documentId);
                    return;
                }
                StoredDocument document = toDocument(node, documents.size());
                documents.add(document);
                documentOrdinalsById.put(documentId, document.getOrdinal());
                documentYears.put(documentId, document.getYear());
                minYear[0] = Math.min(minYear[0], document.getYear());
                maxYear[0] = Math.max(maxYear[0], document.getYear());
                primaryCategoryCounts.merge(document.getPrimaryCategory(), 1, Integer::sum);
            });

            Map<String, Integer> documentFrequency = new HashMap<>();
            forEachFileLine(fileSystem, artifacts.documentFrequencyDir(), true, line -> {
                String[] parts = line.split("\t");
                if (parts.length >= 2) {
                    documentFrequency.put(parts[0], Integer.parseInt(parts[1]));
                }
            });

            Map<String, List<CorpusResponses.DocumentKeyword>> documentKeywordsById = new HashMap<>();
            forEachFileLine(fileSystem, artifacts.documentKeywordsDir(), true, line -> {
                String[] parts = line.split("\t", 2);
                if (parts.length < 2) {
                    return;
                }
                documentKeywordsById.put(parts[0], parseKeywordList(parts[1]));
            });

            Map<String, Integer> globalTermFrequency = new HashMap<>();
            Map<Integer, Map<String, Integer>> yearlyTermFrequency = new TreeMap<>();
            Map<Integer, Integer> yearlyTokenTotals = new TreeMap<>();
            Map<Integer, Integer> yearlyCounts = new TreeMap<>();
            for (StoredDocument document : documents) {
                yearlyCounts.merge(document.getYear(), 1, Integer::sum);
            }

            forEachFileLine(fileSystem, artifacts.termFrequencyDir(), true, line -> {
                String[] parts = line.split("\t");
                if (parts.length < 3) {
                    return;
                }
                String documentId = parts[0];
                String term = parts[1];
                int termFrequency = Integer.parseInt(parts[2]);
                Integer year = documentYears.get(documentId);
                if (year == null) {
                    return;
                }
                globalTermFrequency.merge(term, termFrequency, Integer::sum);
                yearlyTermFrequency.computeIfAbsent(year, ignored -> new HashMap<>())
                        .merge(term, termFrequency, Integer::sum);
                yearlyTokenTotals.merge(year, termFrequency, Integer::sum);
            });

            Map<String, PostingAccumulator> postingAccumulators = new HashMap<>();
            forEachFileLine(fileSystem, artifacts.invertedIndexDir(), true, line -> {
                String[] parts = line.split("\t");
                if (parts.length < 3) {
                    return;
                }
                String term = parts[0];
                String documentId = parts[1];
                Integer ordinal = documentOrdinalsById.get(documentId);
                if (ordinal == null) {
                    return;
                }
                double score = Double.parseDouble(parts[2]);
                postingAccumulators
                        .computeIfAbsent(term, ignored -> new PostingAccumulator())
                        .add(ordinal, score);
            });

            List<List<CorpusResponses.DocumentKeyword>> documentKeywords = new ArrayList<>(documents.size());
            for (StoredDocument document : documents) {
                documentKeywords.add(documentKeywordsById.getOrDefault(document.getId(), List.of()));
            }

            Map<String, CorpusIndexSnapshot.PostingList> postings = freezePostings(postingAccumulators);
            long indexedPostingCount = postingAccumulators.values().stream()
                    .mapToLong(PostingAccumulator::size)
                    .sum();

            List<CorpusResponses.CorpusYearSummary> yearSummaries = buildYearSummaries(
                    yearlyCounts,
                    yearlyTermFrequency,
                    yearlyTokenTotals,
                    globalTermFrequency,
                    properties.getYearKeywordSince(),
                    properties.getYearKeywordCount(),
                    properties.getYearKeywordMinCount());

            List<CorpusResponses.NamedCount> topCategories = topCounts(primaryCategoryCounts, 12);
            List<CorpusResponses.NamedCount> topTerms = topCounts(globalTermFrequency, 20);
            long buildMillis = artifacts.buildMillis() + elapsedMillis(startNanos);

            CorpusResponses.CorpusBuildSummary buildSummary = new CorpusResponses.CorpusBuildSummary(
                    "ready",
                    activeDatasetDir.toString(),
                    artifacts.builtAt(),
                    buildMillis,
                    documents.size(),
                    globalTermFrequency.size(),
                    postings.size(),
                    indexedPostingCount,
                    limitWarnings(warnings));

            return new CorpusIndexSnapshot(
                    true,
                    "ready",
                    activeDatasetDir.getFileName() == null ? activeDatasetDir.toString() : activeDatasetDir.getFileName().toString(),
                    activeDatasetDir.toString(),
                    limitWarnings(warnings),
                    List.copyOf(documents),
                    Map.copyOf(documentOrdinalsById),
                    Map.copyOf(documentFrequency),
                    Map.copyOf(postings),
                    List.copyOf(documentKeywords),
                    topCategories,
                    topTerms,
                    yearSummaries,
                    buildSummary,
                    artifacts.builtAt(),
                    minYear[0] == Integer.MAX_VALUE ? 0 : minYear[0],
                    maxYear[0] == Integer.MIN_VALUE ? 0 : maxYear[0]);
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to read Hadoop processing outputs.", exception);
        }
    }

    private CorpusIndexSnapshot buildLocalSnapshot(Path datasetDir) {
        long startNanos = System.nanoTime();
        Instant builtAt = Instant.now();
        List<String> warnings = new ArrayList<>();
        List<StoredDocument> documents = new ArrayList<>();
        List<Map<String, Integer>> documentTermFrequencies = new ArrayList<>();
        Map<String, Integer> documentOrdinalsById = new HashMap<>();
        Map<String, Integer> documentFrequency = new HashMap<>();
        Map<String, Integer> primaryCategoryCounts = new HashMap<>();
        Map<String, Integer> globalTermFrequency = new HashMap<>();
        Map<Integer, Map<String, Integer>> yearlyTermFrequency = new TreeMap<>();
        Map<Integer, Integer> yearlyTokenTotals = new TreeMap<>();
        Map<Integer, Integer> yearlyCounts = new TreeMap<>();

        int minYear = Integer.MAX_VALUE;
        int maxYear = Integer.MIN_VALUE;

        try {
            for (Path shard : datasetShardPaths(datasetDir)) {
                try (BufferedReader reader = Files.newBufferedReader(shard, StandardCharsets.UTF_8)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.isBlank()) {
                            continue;
                        }
                        JsonNode node = objectMapper.readTree(line);
                        String documentId = CorpusTokenizer.normalize(node.path("id").asText(""));
                        if (documentId.isBlank()) {
                            warnings.add("Skipped a record with blank id while building the local search snapshot.");
                            continue;
                        }
                        if (documentOrdinalsById.containsKey(documentId)) {
                            warnings.add("Skipped duplicate document id during local analysis: " + documentId);
                            continue;
                        }

                        StoredDocument document = toDocument(node, documents.size());
                        documents.add(document);
                        documentOrdinalsById.put(documentId, document.getOrdinal());
                        primaryCategoryCounts.merge(document.getPrimaryCategory(), 1, Integer::sum);
                        yearlyCounts.merge(document.getYear(), 1, Integer::sum);
                        minYear = Math.min(minYear, document.getYear());
                        maxYear = Math.max(maxYear, document.getYear());

                        Map<String, Integer> termCounts = new HashMap<>();
                        for (String token : CorpusTokenizer.tokenizeNormalizedParts(
                                document.getTitle(),
                                document.getAbstractText())) {
                            termCounts.merge(token, 1, Integer::sum);
                            globalTermFrequency.merge(token, 1, Integer::sum);
                            yearlyTermFrequency.computeIfAbsent(document.getYear(), ignored -> new HashMap<>())
                                    .merge(token, 1, Integer::sum);
                            yearlyTokenTotals.merge(document.getYear(), 1, Integer::sum);
                        }
                        for (String term : termCounts.keySet()) {
                            documentFrequency.merge(term, 1, Integer::sum);
                        }
                        documentTermFrequencies.add(termCounts);
                    }
                }
            }
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to read the local dataset.", exception);
        }

        if (documents.isEmpty()) {
            return CorpusIndexSnapshot.empty(activeDatasetDir.toString(), "empty-dataset", limitWarnings(warnings));
        }

        double maxDocumentFrequency = documents.size() * properties.getIndexMaxDocumentFrequencyRatio();
        LocalScoringResult scoringResult = scoreLocalDocuments(
                documents.size(),
                documentTermFrequencies,
                documentFrequency,
                maxDocumentFrequency,
                properties.getDocumentKeywordCount());
        Map<String, PostingAccumulator> postingAccumulators = scoringResult.postingAccumulators();
        List<List<CorpusResponses.DocumentKeyword>> documentKeywords = scoringResult.documentKeywords();

        Map<String, CorpusIndexSnapshot.PostingList> postings = freezePostings(postingAccumulators);
        long indexedPostingCount = postingAccumulators.values().stream()
                .mapToLong(PostingAccumulator::size)
                .sum();

        List<CorpusResponses.CorpusYearSummary> yearSummaries = buildYearSummaries(
                yearlyCounts,
                yearlyTermFrequency,
                yearlyTokenTotals,
                globalTermFrequency,
                properties.getYearKeywordSince(),
                properties.getYearKeywordCount(),
                properties.getYearKeywordMinCount());

        List<CorpusResponses.NamedCount> topCategories = topCounts(primaryCategoryCounts, 12);
        List<CorpusResponses.NamedCount> topTerms = topCounts(globalTermFrequency, 20);
        long buildMillis = elapsedMillis(startNanos);
        List<String> limitedWarnings = limitWarnings(warnings);

        CorpusResponses.CorpusBuildSummary buildSummary = new CorpusResponses.CorpusBuildSummary(
                "ready",
                activeDatasetDir.toString(),
                builtAt,
                buildMillis,
                documents.size(),
                globalTermFrequency.size(),
                postings.size(),
                indexedPostingCount,
                limitedWarnings);

        return new CorpusIndexSnapshot(
                true,
                "ready",
                activeDatasetDir.getFileName() == null ? activeDatasetDir.toString() : activeDatasetDir.getFileName().toString(),
                activeDatasetDir.toString(),
                limitedWarnings,
                List.copyOf(documents),
                Map.copyOf(documentOrdinalsById),
                Map.copyOf(documentFrequency),
                Map.copyOf(postings),
                List.copyOf(documentKeywords),
                topCategories,
                topTerms,
                yearSummaries,
                buildSummary,
                builtAt,
                minYear == Integer.MAX_VALUE ? 0 : minYear,
                maxYear == Integer.MIN_VALUE ? 0 : maxYear);
    }

    private List<Path> datasetShardPaths(Path datasetDir) throws IOException {
        Path yearsDir = datasetDir.resolve("years");
        Path shardDirectory = Files.isDirectory(yearsDir) ? yearsDir : datasetDir;
        if (!Files.isDirectory(shardDirectory)) {
            throw new IllegalArgumentException("Dataset years directory does not exist: " + yearsDir);
        }
        try (var stream = Files.list(shardDirectory)) {
            return stream.filter(path -> path.getFileName().toString().endsWith(".jsonl"))
                    .sorted()
                    .toList();
        }
    }

    private LocalScoringResult scoreLocalDocuments(
            int documentCount,
            List<Map<String, Integer>> documentTermFrequencies,
            Map<String, Integer> documentFrequency,
            double maxDocumentFrequency,
            int keywordLimit) {
        int workerCount = localAnalysisWorkerCount(documentCount, Runtime.getRuntime().availableProcessors());
        if (workerCount <= 1) {
            return scoreLocalDocumentRange(
                    0,
                    documentCount,
                    documentCount,
                    documentTermFrequencies,
                    documentFrequency,
                    maxDocumentFrequency,
                    keywordLimit);
        }

        ExecutorService executor = Executors.newFixedThreadPool(workerCount, task -> {
            Thread thread = new Thread(task, "local-corpus-analysis");
            thread.setDaemon(true);
            return thread;
        });
        try {
            List<java.util.concurrent.Callable<LocalScoringChunk>> tasks = new ArrayList<>(workerCount);
            int chunkSize = Math.max(1, (documentCount + workerCount - 1) / workerCount);
            for (int startOrdinal = 0; startOrdinal < documentCount; startOrdinal += chunkSize) {
                int rangeStart = startOrdinal;
                int rangeEnd = Math.min(documentCount, rangeStart + chunkSize);
                tasks.add(() -> scoreLocalDocumentChunk(
                        rangeStart,
                        rangeEnd,
                        documentCount,
                        documentTermFrequencies,
                        documentFrequency,
                        maxDocumentFrequency,
                        keywordLimit));
            }

            List<Future<LocalScoringChunk>> futures = executor.invokeAll(tasks);
            List<List<CorpusResponses.DocumentKeyword>> documentKeywords = new ArrayList<>(documentCount);
            Map<String, PostingAccumulator> postingAccumulators = new HashMap<>();
            for (Future<LocalScoringChunk> future : futures) {
                LocalScoringChunk chunk = future.get();
                documentKeywords.addAll(chunk.documentKeywords());
                mergePostingAccumulators(postingAccumulators, chunk.postingAccumulators());
            }
            return new LocalScoringResult(postingAccumulators, documentKeywords);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Local corpus analysis was interrupted.", exception);
        } catch (java.util.concurrent.ExecutionException exception) {
            throw new IllegalStateException("Local corpus analysis failed.", exception.getCause());
        } finally {
            executor.shutdownNow();
        }
    }

    private LocalScoringResult scoreLocalDocumentRange(
            int startOrdinal,
            int endOrdinal,
            int documentCount,
            List<Map<String, Integer>> documentTermFrequencies,
            Map<String, Integer> documentFrequency,
            double maxDocumentFrequency,
            int keywordLimit) {
        LocalScoringChunk chunk = scoreLocalDocumentChunk(
                startOrdinal,
                endOrdinal,
                documentCount,
                documentTermFrequencies,
                documentFrequency,
                maxDocumentFrequency,
                keywordLimit);
        return new LocalScoringResult(chunk.postingAccumulators(), chunk.documentKeywords());
    }

    private LocalScoringChunk scoreLocalDocumentChunk(
            int startOrdinal,
            int endOrdinal,
            int documentCount,
            List<Map<String, Integer>> documentTermFrequencies,
            Map<String, Integer> documentFrequency,
            double maxDocumentFrequency,
            int keywordLimit) {
        Map<String, PostingAccumulator> postingAccumulators = new HashMap<>();
        List<List<CorpusResponses.DocumentKeyword>> documentKeywords =
                new ArrayList<>(Math.max(0, endOrdinal - startOrdinal));

        for (int ordinal = startOrdinal; ordinal < endOrdinal; ordinal++) {
            Map<String, Integer> termCounts = documentTermFrequencies.get(ordinal);
            PriorityQueue<KeywordCandidate> topKeywordCandidates =
                    new PriorityQueue<>(Math.max(1, keywordLimit), CorpusIndexService::compareKeywordCandidates);
            for (Map.Entry<String, Integer> entry : termCounts.entrySet()) {
                String term = entry.getKey();
                int termFrequency = entry.getValue();
                int df = documentFrequency.getOrDefault(term, 0);
                if (df <= 0) {
                    continue;
                }
                double score = (1.0d + Math.log(termFrequency))
                        * (Math.log((documentCount + 1.0d) / (df + 1.0d)) + 1.0d);
                collectTopKeywordCandidate(
                        topKeywordCandidates,
                        new KeywordCandidate(term, score),
                        keywordLimit);
                if (df <= maxDocumentFrequency) {
                    postingAccumulators.computeIfAbsent(term, ignored -> new PostingAccumulator())
                            .add(ordinal, score);
                }
            }
            documentKeywords.add(freezeKeywordCandidates(topKeywordCandidates));
        }

        return new LocalScoringChunk(postingAccumulators, documentKeywords);
    }

    private void mergePostingAccumulators(
            Map<String, PostingAccumulator> target,
            Map<String, PostingAccumulator> source) {
        for (Map.Entry<String, PostingAccumulator> entry : source.entrySet()) {
            target.computeIfAbsent(entry.getKey(), ignored -> new PostingAccumulator())
                    .addAll(entry.getValue());
        }
    }

    static int localAnalysisWorkerCount(int documentCount, int availableProcessors) {
        if (documentCount <= 1) {
            return 1;
        }
        int processorBudget = Math.max(1, availableProcessors);
        return Math.max(1, Math.min(Math.min(MAX_LOCAL_ANALYSIS_WORKERS, processorBudget), documentCount));
    }

    private void forEachFileLine(
            FileSystem fileSystem,
            org.apache.hadoop.fs.Path directory,
            boolean outputPartFilesOnly,
            LineConsumer consumer) throws IOException {
        FileStatus[] statuses = fileSystem.listStatus(directory);
        ArrayList<org.apache.hadoop.fs.Path> files = new ArrayList<>();
        for (FileStatus status : statuses) {
            if (!status.isFile()) {
                continue;
            }
            if (outputPartFilesOnly && !status.getPath().getName().startsWith("part-")) {
                continue;
            }
            files.add(status.getPath());
        }
        files.sort(Comparator.comparing(org.apache.hadoop.fs.Path::toString));

        for (org.apache.hadoop.fs.Path file : files) {
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(fileSystem.open(file), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    consumer.accept(line);
                }
            }
        }
    }

    private List<CorpusResponses.DocumentKeyword> parseKeywordList(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        ArrayList<CorpusResponses.DocumentKeyword> keywords = new ArrayList<>();
        for (String part : raw.split(",")) {
            String[] segments = part.split("\\|");
            if (segments.length < 2) {
                continue;
            }
            keywords.add(new CorpusResponses.DocumentKeyword(
                    segments[0],
                    roundScore(Double.parseDouble(segments[1]))));
        }
        return List.copyOf(keywords);
    }

    private CorpusResponses.CorpusSearchResult toSearchResult(
            CorpusIndexSnapshot snapshot,
            StoredDocument document,
            double score,
            Set<String> matchedTerms) {
        return new CorpusResponses.CorpusSearchResult(
                document.getId(),
                document.getYear(),
                document.getTitle(),
                buildSnippet(document.getAbstractText(), matchedTerms),
                document.getAuthors(),
                document.getPrimaryCategory(),
                document.getCategories(),
                matchedTerms.stream().sorted().toList(),
                snapshot.documentKeywords.get(document.getOrdinal()).stream().limit(5).toList(),
                roundScore(score));
    }

    private int clampLimit(Integer requestedLimit) {
        int limit = requestedLimit == null ? properties.getSearchDefaultLimit() : requestedLimit;
        limit = Math.max(1, limit);
        return Math.min(limit, properties.getSearchMaxLimit());
    }

    private CorpusResponses.CorpusBuildSummary buildTransientSummary(
            CorpusIndexSnapshot current, String status, String extraWarning) {
        ArrayList<String> warnings = new ArrayList<>(current.buildSummary.getWarnings());
        warnings.add(extraWarning);
        String datasetDir = activeDatasetDir == null
                ? current.buildSummary.getDatasetDir()
                : activeDatasetDir.toString();
        return new CorpusResponses.CorpusBuildSummary(
                status,
                datasetDir,
                reloadRequestedAt != null ? reloadRequestedAt : current.buildSummary.getBuiltAt(),
                current.buildSummary.getBuildMillis(),
                current.recordCount(),
                current.buildSummary.getVocabularySize(),
                current.buildSummary.getIndexedTermCount(),
                current.buildSummary.getIndexedPostingCount(),
                limitWarnings(warnings),
                reloadProgress);
    }

    private void startProgress(String stage, String message, int currentStep, int totalSteps, int percent) {
        reloadProgress = new CorpusResponses.CorpusBuildProgress(
                stage,
                message,
                currentStep,
                totalSteps,
                percent,
                0L,
                Instant.now());
    }

    private void updateReloadProgress(HadoopProcessingService.ProcessingProgress progress) {
        reloadProgress = new CorpusResponses.CorpusBuildProgress(
                progress.stage(),
                progress.message(),
                progress.currentStep(),
                progress.totalSteps(),
                progress.percent(),
                progress.elapsedMillis(),
                progress.updatedAt());
    }

    private List<String> distinctTerms(List<String> terms) {
        if (terms.size() < 2) {
            return terms;
        }
        return new ArrayList<>(new LinkedHashSet<>(terms));
    }

    private List<SearchHit> selectTopHits(Map<Integer, Double> scores, int limit) {
        PriorityQueue<SearchHit> topHits = new PriorityQueue<>(
                Comparator.comparingDouble(SearchHit::score)
                        .thenComparingInt(SearchHit::documentOrdinal));
        for (Map.Entry<Integer, Double> entry : scores.entrySet()) {
            SearchHit candidate = new SearchHit(entry.getKey(), entry.getValue());
            if (topHits.size() < limit) {
                topHits.offer(candidate);
                continue;
            }
            SearchHit smallest = topHits.peek();
            if (smallest != null && compareByScore(candidate, smallest) > 0) {
                topHits.poll();
                topHits.offer(candidate);
            }
        }
        ArrayList<SearchHit> orderedHits = new ArrayList<>(topHits);
        orderedHits.sort((left, right) -> compareByScore(right, left));
        return orderedHits;
    }

    private int compareByScore(SearchHit left, SearchHit right) {
        int scoreComparison = Double.compare(left.score(), right.score());
        if (scoreComparison != 0) {
            return scoreComparison;
        }
        return Integer.compare(left.documentOrdinal(), right.documentOrdinal());
    }

    private static int compareKeywordCandidates(KeywordCandidate left, KeywordCandidate right) {
        int scoreComparison = Double.compare(left.score(), right.score());
        if (scoreComparison != 0) {
            return scoreComparison;
        }
        return right.term().compareTo(left.term());
    }

    private void collectTopKeywordCandidate(
            PriorityQueue<KeywordCandidate> topKeywordCandidates,
            KeywordCandidate candidate,
            int keywordLimit) {
        if (keywordLimit <= 0) {
            return;
        }
        if (topKeywordCandidates.size() < keywordLimit) {
            topKeywordCandidates.offer(candidate);
            return;
        }
        KeywordCandidate smallest = topKeywordCandidates.peek();
        if (smallest != null && compareKeywordCandidates(candidate, smallest) > 0) {
            topKeywordCandidates.poll();
            topKeywordCandidates.offer(candidate);
        }
    }

    private List<CorpusResponses.DocumentKeyword> freezeKeywordCandidates(
            PriorityQueue<KeywordCandidate> topKeywordCandidates) {
        if (topKeywordCandidates.isEmpty()) {
            return List.of();
        }
        ArrayList<KeywordCandidate> ordered = new ArrayList<>(topKeywordCandidates);
        ordered.sort((left, right) -> compareKeywordCandidates(right, left));
        return ordered.stream()
                .map(candidate -> new CorpusResponses.DocumentKeyword(
                        candidate.term(),
                        roundScore(candidate.score())))
                .toList();
    }

    private List<CorpusResponses.CorpusYearSummary> buildYearSummaries(
            Map<Integer, Integer> yearlyCounts,
            Map<Integer, Map<String, Integer>> yearlyTermFrequency,
            Map<Integer, Integer> yearlyTokenTotals,
            Map<String, Integer> globalTermFrequency,
            int sinceYear,
            int keywordCount,
            int minCount) {
        int globalTokenTotal = globalTermFrequency.values().stream().mapToInt(Integer::intValue).sum();
        List<CorpusResponses.CorpusYearSummary> summaries = new ArrayList<>();

        for (Map.Entry<Integer, Integer> yearEntry : yearlyCounts.entrySet()) {
            int year = yearEntry.getKey();
            Map<String, Integer> terms = yearlyTermFrequency.getOrDefault(year, Map.of());
            int yearTokenTotal = yearlyTokenTotals.getOrDefault(year, 0);
            List<CorpusResponses.DocumentKeyword> keywords;
            if (year < sinceYear || yearTokenTotal <= 0 || globalTokenTotal <= 0) {
                keywords = List.of();
            } else {
                List<CorpusResponses.DocumentKeyword> primaryKeywords = terms.entrySet().stream()
                        .filter(entry -> entry.getValue() >= minCount)
                        .map(entry -> new KeywordCandidate(
                                entry.getKey(),
                                Math.log(
                                        ((entry.getValue() + 1.0d) / yearTokenTotal)
                                                / ((globalTermFrequency.getOrDefault(entry.getKey(), 0) + 1.0d)
                                                        / globalTokenTotal))))
                        .sorted(Comparator.comparingDouble(KeywordCandidate::score).reversed()
                                .thenComparing(KeywordCandidate::term))
                        .limit(keywordCount)
                        .map(candidate -> new CorpusResponses.DocumentKeyword(
                                candidate.term(),
                                roundScore(candidate.score())))
                        .toList();

                if (!primaryKeywords.isEmpty()) {
                    keywords = primaryKeywords;
                } else {
                    keywords = terms.entrySet().stream()
                            .map(entry -> new KeywordCandidate(
                                    entry.getKey(),
                                    Math.log(
                                            ((entry.getValue() + 1.0d) / yearTokenTotal)
                                                    / ((globalTermFrequency.getOrDefault(entry.getKey(), 0) + 1.0d)
                                                            / globalTokenTotal))))
                            .sorted(Comparator.comparingDouble(KeywordCandidate::score).reversed()
                                    .thenComparing(KeywordCandidate::term))
                            .limit(keywordCount)
                            .map(candidate -> new CorpusResponses.DocumentKeyword(
                                    candidate.term(),
                                    roundScore(candidate.score())))
                            .toList();
                }
            }
            summaries.add(new CorpusResponses.CorpusYearSummary(year, yearEntry.getValue(), keywords));
        }
        return List.copyOf(summaries);
    }

    private List<CorpusResponses.NamedCount> topCounts(Map<String, Integer> counts, int limit) {
        return counts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed()
                        .thenComparing(Map.Entry::getKey))
                .limit(limit)
                .map(entry -> new CorpusResponses.NamedCount(entry.getKey(), entry.getValue()))
                .toList();
    }

    private StoredDocument toDocument(JsonNode node, int ordinal) {
        List<String> categories = new ArrayList<>();
        JsonNode categoriesList = node.get("categories_list");
        if (categoriesList != null && categoriesList.isArray()) {
            categoriesList.forEach(category -> categories.add(CorpusTokenizer.normalize(category.asText())));
        } else {
            String categoriesText = CorpusTokenizer.normalize(node.path("categories").asText(""));
            if (!categoriesText.isBlank()) {
                categories.addAll(List.of(categoriesText.split("\\s+")));
            }
        }

        String primaryCategory = CorpusTokenizer.normalize(node.path("primary_category").asText(""));
        if (primaryCategory.isBlank() && !categories.isEmpty()) {
            primaryCategory = categories.get(0);
        }

        return new StoredDocument(
                ordinal,
                CorpusTokenizer.normalize(node.path("id").asText("")),
                node.path("year").asInt(0),
                node.path("month").asInt(0),
                CorpusTokenizer.normalize(node.path("title").asText("")),
                CorpusTokenizer.normalize(node.path("abstract").asText("")),
                CorpusTokenizer.normalize(node.path("authors").asText("")),
                List.copyOf(categories),
                primaryCategory,
                CorpusTokenizer.normalize(node.path("update_date").asText("")));
    }

    private Map<String, CorpusIndexSnapshot.PostingList> freezePostings(
            Map<String, PostingAccumulator> postingAccumulators) {
        Map<String, CorpusIndexSnapshot.PostingList> postings = new HashMap<>(postingAccumulators.size());
        for (Map.Entry<String, PostingAccumulator> entry : postingAccumulators.entrySet()) {
            postings.put(entry.getKey(), entry.getValue().freeze());
        }
        return postings;
    }

    private String buildSnippet(String abstractText, Collection<String> matchedTerms) {
        String normalized = CorpusTokenizer.normalize(abstractText);
        if (normalized.length() <= 260) {
            return normalized;
        }
        String lower = normalized.toLowerCase(Locale.ROOT);
        int matchIndex = Integer.MAX_VALUE;
        for (String term : matchedTerms) {
            int candidate = lower.indexOf(term.toLowerCase(Locale.ROOT));
            if (candidate >= 0) {
                matchIndex = Math.min(matchIndex, candidate);
            }
        }
        if (matchIndex == Integer.MAX_VALUE) {
            return normalized.substring(0, 257) + "...";
        }
        int start = Math.max(0, matchIndex - 70);
        int end = Math.min(normalized.length(), matchIndex + 170);
        while (start > 0 && normalized.charAt(start) != ' ') {
            start--;
        }
        while (end < normalized.length() && normalized.charAt(end - 1) != ' ') {
            end++;
            if (end >= normalized.length()) {
                end = normalized.length();
                break;
            }
        }
        String snippet = normalized.substring(start, end).trim();
        if (start > 0) {
            snippet = "..." + snippet;
        }
        if (end < normalized.length()) {
            snippet = snippet + "...";
        }
        return snippet;
    }

    private Path requireActiveDatasetDir() {
        if (activeDatasetDir != null) {
            return activeDatasetDir;
        }

        Path configuredDatasetDir = resolveConfiguredDatasetDir();
        if (configuredDatasetDir == null) {
            throw new IllegalArgumentException("Upload a dataset before submitting analysis.");
        }
        activeDatasetDir = configuredDatasetDir;
        return configuredDatasetDir;
    }

    private Path resolvePath(String value) {
        return Path.of(value).normalize().toAbsolutePath();
    }

    private Path resolveConfiguredDatasetDir() {
        return hadoopProcessingService.configuredDatasetDir();
    }

    private Path createUploadDatasetDir() {
        Path uploadBaseDir = resolvePath(properties.getUploadBaseDir());
        try {
            Files.createDirectories(uploadBaseDir);
            return Files.createTempDirectory(uploadBaseDir, "dataset-");
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to prepare upload dataset directory.", exception);
        }
    }

    private void importFile(
            MultipartFile file,
            UploadWriter writer,
            List<String> warnings,
            AtomicLong importedRecordCount) throws IOException {
        String originalFilename = file.getOriginalFilename() == null ? "" : file.getOriginalFilename().trim();
        String lowercaseName = originalFilename.toLowerCase(Locale.ROOT);
        if (lowercaseName.endsWith(".jsonl")) {
            importJsonLines(file, writer, warnings, importedRecordCount);
            return;
        }
        if (lowercaseName.endsWith(".json")) {
            importJson(file, writer, warnings, importedRecordCount);
            return;
        }
        throw new IllegalArgumentException("Unsupported file type: " + originalFilename + ". Use .json or .jsonl.");
    }

    private void importJsonLines(
            MultipartFile file,
            UploadWriter writer,
            List<String> warnings,
            AtomicLong importedRecordCount) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                JsonNode node = readUploadNode(line, file.getOriginalFilename());
                if (writeUploadedRecord(node, writer, warnings)) {
                    importedRecordCount.incrementAndGet();
                }
            }
        }
    }

    private void importJson(
            MultipartFile file,
            UploadWriter writer,
            List<String> warnings,
            AtomicLong importedRecordCount) throws IOException {
        try (JsonParser parser = objectMapper.getFactory().createParser(file.getInputStream())) {
            JsonToken firstToken = parser.nextToken();
            if (firstToken == JsonToken.START_ARRAY) {
                streamJsonArray(parser, writer, warnings, importedRecordCount);
                return;
            }
        }

        String wrappedField = detectWrappedArrayField(file);
        if (wrappedField != null) {
            streamWrappedJsonArray(file, wrappedField, writer, warnings, importedRecordCount);
            return;
        }

        JsonNode root = objectMapper.readTree(file.getInputStream());
        if (writeUploadedRecord(root, writer, warnings)) {
            importedRecordCount.incrementAndGet();
        }
    }

    private String detectWrappedArrayField(MultipartFile file) throws IOException {
        try (JsonParser parser = objectMapper.getFactory().createParser(file.getInputStream())) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return null;
            }
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = parser.currentName();
                JsonToken valueToken = parser.nextToken();
                if (WRAPPED_RECORD_FIELDS.contains(fieldName) && valueToken == JsonToken.START_ARRAY) {
                    return fieldName;
                }
                parser.skipChildren();
            }
            return null;
        }
    }

    private void streamWrappedJsonArray(
            MultipartFile file,
            String wrappedField,
            UploadWriter writer,
            List<String> warnings,
            AtomicLong importedRecordCount) throws IOException {
        try (JsonParser parser = objectMapper.getFactory().createParser(file.getInputStream())) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return;
            }
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = parser.currentName();
                JsonToken valueToken = parser.nextToken();
                if (!wrappedField.equals(fieldName)) {
                    parser.skipChildren();
                    continue;
                }
                if (valueToken == JsonToken.START_ARRAY) {
                    streamJsonArray(parser, writer, warnings, importedRecordCount);
                } else if (valueToken == JsonToken.START_OBJECT) {
                    JsonNode node = objectMapper.readTree(parser);
                    if (writeUploadedRecord(node, writer, warnings)) {
                        importedRecordCount.incrementAndGet();
                    }
                }
                return;
            }
        }
    }

    private void streamJsonArray(
            JsonParser parser,
            UploadWriter writer,
            List<String> warnings,
            AtomicLong importedRecordCount) throws IOException {
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            JsonNode node = objectMapper.readTree(parser);
            if (writeUploadedRecord(node, writer, warnings)) {
                importedRecordCount.incrementAndGet();
            }
        }
    }

    private JsonNode readUploadNode(String line, String filename) {
        try {
            return objectMapper.readTree(line);
        } catch (IOException exception) {
            String safeFilename = (filename == null || filename.isBlank()) ? "upload.jsonl" : filename;
            String parserMessage = extractUploadParserMessage(exception);
            String message = parserMessage == null
                    ? "Failed to parse uploaded JSONL in " + safeFilename + "."
                    : "Uploaded JSON is malformed in " + safeFilename + ": " + parserMessage;
            throw new UncheckedIOException(message, exception);
        }
    }

    private String describeUploadImportFailure(IOException exception) {
        String parserMessage = extractUploadParserMessage(exception);
        if (parserMessage != null) {
            return "Uploaded JSON is malformed: " + parserMessage;
        }
        return "Failed to persist uploaded dataset.";
    }

    private String extractUploadParserMessage(IOException exception) {
        Throwable current = exception;
        while (current != null) {
            if (current instanceof JsonProcessingException) {
                String parserMessage = current.getMessage();
                if (parserMessage == null || parserMessage.isBlank()) {
                    return null;
                }
                return parserMessage.lines().findFirst().orElse(parserMessage).trim();
            }
            current = current.getCause();
        }
        return null;
    }

    private boolean writeUploadedRecord(JsonNode rawNode, UploadWriter writer, List<String> warnings)
            throws IOException {
        if (rawNode == null || !rawNode.isObject()) {
            warnings.add("Skipped a non-object JSON record.");
            return false;
        }

        JsonNode normalizedNode = normalizeUploadedRecord(rawNode);
        if (normalizedNode == null) {
            warnings.add("Skipped a record missing id, title, or abstract.");
            return false;
        }
        writer.write(normalizedNode);
        return true;
    }

    private void writeUploadManifest(Path datasetDir, long recordCount, int shardCount) throws IOException {
        ObjectNode root = objectMapper.createObjectNode();
        ObjectNode totals = root.putObject("totals");
        totals.put("records", recordCount);
        totals.put("shards", shardCount);
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(datasetDir.resolve("manifest.json").toFile(), root);
    }

    private JsonNode normalizeUploadedRecord(JsonNode node) {
        String id = firstText(node, "id", "paper_id", "arxiv_id");
        String title = firstText(node, "title", "paper_title");
        String abstractText = firstText(node, "abstract", "abstractText", "summary");
        if (id.isBlank() || title.isBlank() || abstractText.isBlank()) {
            return null;
        }

        String updateDate = firstText(node, "update_date", "updated", "published", "created");
        int year = firstInt(node, "year");
        if (year <= 0) {
            year = extractYear(updateDate);
        }

        int month = firstInt(node, "month");
        if (month <= 0) {
            month = extractMonth(updateDate);
        }

        List<String> categories = extractCategories(node);
        String primaryCategory = firstText(node, "primary_category", "primaryCategory");
        if (primaryCategory.isBlank() && !categories.isEmpty()) {
            primaryCategory = categories.get(0);
        }

        ObjectNode normalized = objectMapper.createObjectNode();
        normalized.put("id", CorpusTokenizer.normalize(id));
        normalized.put("year", Math.max(year, 0));
        normalized.put("month", Math.max(month, 0));
        normalized.put("authors", CorpusTokenizer.normalize(firstText(node, "authors", "author")));
        normalized.put("title", CorpusTokenizer.normalize(title));
        normalized.put("abstract", CorpusTokenizer.normalize(abstractText));
        normalized.put("categories", String.join(" ", categories));
        ArrayNode categoriesList = normalized.putArray("categories_list");
        categories.forEach(categoriesList::add);
        normalized.put("primary_category", CorpusTokenizer.normalize(primaryCategory));
        normalized.put("update_date", CorpusTokenizer.normalize(updateDate));
        return normalized;
    }

    private String firstText(JsonNode node, String... fieldNames) {
        for (String fieldName : fieldNames) {
            JsonNode value = node.get(fieldName);
            if (value != null && !value.isNull()) {
                String text = CorpusTokenizer.normalize(value.asText(""));
                if (!text.isBlank()) {
                    return text;
                }
            }
        }
        return "";
    }

    private int firstInt(JsonNode node, String... fieldNames) {
        for (String fieldName : fieldNames) {
            JsonNode value = node.get(fieldName);
            if (value != null && value.canConvertToInt()) {
                return value.asInt();
            }
            if (value != null && !value.isNull()) {
                String text = value.asText("");
                if (text.matches("\\d{1,4}")) {
                    return Integer.parseInt(text);
                }
            }
        }
        return 0;
    }

    private List<String> extractCategories(JsonNode node) {
        LinkedHashSet<String> categories = new LinkedHashSet<>();
        JsonNode listNode = node.get("categories_list");
        if (listNode == null || !listNode.isArray()) {
            listNode = node.get("categoriesList");
        }
        if (listNode != null && listNode.isArray()) {
            listNode.forEach(item -> {
                String normalized = CorpusTokenizer.normalize(item.asText(""));
                if (!normalized.isBlank()) {
                    categories.add(normalized);
                }
            });
        }

        String categoriesText = firstText(node, "categories", "category");
        if (!categoriesText.isBlank()) {
            for (String part : categoriesText.split("[,\\s]+")) {
                String normalized = CorpusTokenizer.normalize(part);
                if (!normalized.isBlank()) {
                    categories.add(normalized);
                }
            }
        }
        return List.copyOf(categories);
    }

    private int extractYear(String value) {
        if (value == null || value.length() < 4) {
            return 0;
        }
        String prefix = value.substring(0, 4);
        return prefix.matches("\\d{4}") ? Integer.parseInt(prefix) : 0;
    }

    private int extractMonth(String value) {
        if (value == null || value.length() < 7) {
            return 0;
        }
        String segment = value.substring(5, 7);
        return segment.matches("\\d{2}") ? Integer.parseInt(segment) : 0;
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

    private static double roundScore(double value) {
        return Math.round(value * 1000.0d) / 1000.0d;
    }

    private interface LineConsumer {
        void accept(String line) throws IOException;
    }

    private record SearchHit(int documentOrdinal, double score) {}

    private record KeywordCandidate(String term, double score) {}

    private record LocalScoringResult(
            Map<String, PostingAccumulator> postingAccumulators,
            List<List<CorpusResponses.DocumentKeyword>> documentKeywords) {}

    private record LocalScoringChunk(
            Map<String, PostingAccumulator> postingAccumulators,
            List<List<CorpusResponses.DocumentKeyword>> documentKeywords) {}

    private static final class UploadWriter implements AutoCloseable {
        private final ObjectMapper objectMapper;
        private final BufferedWriter writer;

        private UploadWriter(Path shardPath, ObjectMapper objectMapper) throws IOException {
            this.objectMapper = objectMapper;
            this.writer = Files.newBufferedWriter(
                    shardPath,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
        }

        private void write(JsonNode node) throws IOException {
            writer.write(objectMapper.writeValueAsString(node));
            writer.newLine();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    private static final class PostingAccumulator {
        private int[] documentOrdinals = new int[16];
        private float[] tfIdfScores = new float[16];
        private int size;

        void add(int documentOrdinal, double tfIdfScore) {
            ensureCapacity(size + 1);
            documentOrdinals[size] = documentOrdinal;
            tfIdfScores[size] = (float) tfIdfScore;
            size++;
        }

        void addAll(PostingAccumulator source) {
            ensureCapacity(size + source.size);
            System.arraycopy(source.documentOrdinals, 0, documentOrdinals, size, source.size);
            System.arraycopy(source.tfIdfScores, 0, tfIdfScores, size, source.size);
            size += source.size;
        }

        int size() {
            return size;
        }

        CorpusIndexSnapshot.PostingList freeze() {
            return new CorpusIndexSnapshot.PostingList(
                    java.util.Arrays.copyOf(documentOrdinals, size),
                    java.util.Arrays.copyOf(tfIdfScores, size));
        }

        private void ensureCapacity(int capacity) {
            if (capacity <= documentOrdinals.length) {
                return;
            }
            int newCapacity = Math.max(capacity, documentOrdinals.length * 2);
            documentOrdinals = java.util.Arrays.copyOf(documentOrdinals, newCapacity);
            tfIdfScores = java.util.Arrays.copyOf(tfIdfScores, newCapacity);
        }
    }
}
