package com.java26groupwork.finalassignment.corpus;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class PrebuiltCorpusService {

    private static final Logger log = LoggerFactory.getLogger(PrebuiltCorpusService.class);

    private final PrebuiltDemoProperties properties;
    private final CorpusProperties corpusProperties;
    private final ObjectMapper objectMapper;
    private volatile LoadedSnapshot snapshot;

    public PrebuiltCorpusService(
            PrebuiltDemoProperties properties, CorpusProperties corpusProperties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.corpusProperties = corpusProperties;
        this.objectMapper = objectMapper;
    }

    public CorpusResponses.CorpusOverviewResponse overview() {
        LoadedSnapshot current = requireSnapshot();
        return new CorpusResponses.CorpusOverviewResponse(
                true,
                current.mode,
                current.datasetName,
                current.datasetDir,
                current.buildSummary.getStatus(),
                current.documents.size(),
                current.minYear,
                current.maxYear,
                current.topCategories,
                current.topTerms,
                current.yearSummaries,
                current.buildSummary);
    }

    public CorpusResponses.CorpusSearchResponse search(
            String query, Integer year, String category, Integer requestedLimit) {
        LoadedSnapshot current = requireSnapshot();
        long start = System.nanoTime();
        int limit = clampLimit(requestedLimit);
        List<String> queryTerms = CorpusTokenizer.tokenize(query);
        List<String> distinctQueryTerms = distinctTerms(queryTerms);
        List<String> warnings = new ArrayList<>(current.warnings);

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
            PostingList postingList = current.postings.get(term);
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
                matches.computeIfAbsent(docOrdinal, ignored -> new java.util.HashSet<>()).add(term);
            }
        }

        List<SearchHit> topHits = selectTopHits(scores, limit);
        List<CorpusResponses.CorpusSearchResult> results =
                topHits.stream()
                        .map(
                                hit ->
                                        toSearchResult(
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
        LoadedSnapshot current = requireSnapshot();
        Integer ordinal = current.documentOrdinalsById.get(id);
        if (ordinal == null) {
            return new CorpusResponses.DocumentDetailResponse(false, "not-found", null, List.of());
        }
        return new CorpusResponses.DocumentDetailResponse(
                true, "ok", current.documents.get(ordinal), current.documentKeywords.get(ordinal));
    }

    public Map<String, Object> health() {
        LoadedSnapshot current = requireSnapshot();
        Map<String, Object> hadoop = new HashMap<>();
        hadoop.put("mode", "cluster");
        hadoop.put("fileSystem", "prebuilt-demo");
        hadoop.put("clusterId", "prebuilt-demo");
        hadoop.put("configDir", properties.getPrebuiltSnapshotPath());

        Map<String, Object> corpus = new HashMap<>();
        corpus.put("ready", true);
        corpus.put("documents", current.documents.size());
        corpus.put("build", current.buildSummary);

        return Map.of(
                "status", "ok",
                "backend", "spring-boot",
                "processing", current.mode,
                "corpus", corpus,
                "hadoop", hadoop);
    }

    private LoadedSnapshot requireSnapshot() {
        LoadedSnapshot current = snapshot;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            if (snapshot == null) {
                snapshot = loadSnapshot();
            }
            return snapshot;
        }
    }

    private LoadedSnapshot loadSnapshot() {
        Path snapshotPath = resolveSnapshotPath();
        if (!Files.exists(snapshotPath)) {
            throw new IllegalStateException("Prebuilt snapshot file does not exist: " + snapshotPath);
        }

        log.info("Loading prebuilt demo snapshot from {}", snapshotPath);
        long start = System.nanoTime();

        String mode = "hdfs-yarn-mapreduce-pipeline";
        String datasetName = "";
        String datasetDir = "";
        String status = "ready";
        Instant builtAt = Instant.now();
        long buildMillis = 0L;
        long recordCount = 0L;
        int vocabularySize = 0;
        int indexedTermCount = 0;
        long indexedPostingCount = 0L;
        List<String> warnings = List.of();
        List<CorpusResponses.NamedCount> topCategories = List.of();
        List<CorpusResponses.NamedCount> topTerms = List.of();
        List<CorpusResponses.CorpusYearSummary> yearSummaries = List.of();
        List<StoredDocument> documents = List.of();
        List<List<CorpusResponses.DocumentKeyword>> documentKeywords = List.of();
        Map<String, PostingList> postings = Map.of();

        try (InputStream inputStream = Files.newInputStream(snapshotPath);
             JsonParser parser = objectMapper.getFactory().createParser(inputStream)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new IllegalStateException("Prebuilt snapshot root must be an object.");
            }
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = parser.currentName();
                JsonToken valueToken = parser.nextToken();
                if ("mode".equals(fieldName)) {
                    mode = parser.getValueAsString(mode);
                } else if ("datasetName".equals(fieldName)) {
                    datasetName = parser.getValueAsString(datasetName);
                } else if ("datasetDir".equals(fieldName)) {
                    datasetDir = parser.getValueAsString(datasetDir);
                } else if ("status".equals(fieldName)) {
                    status = parser.getValueAsString(status);
                } else if ("builtAt".equals(fieldName)) {
                    builtAt = Instant.parse(parser.getValueAsString());
                } else if ("buildMillis".equals(fieldName)) {
                    buildMillis = parser.getLongValue();
                } else if ("recordCount".equals(fieldName)) {
                    recordCount = parser.getLongValue();
                } else if ("vocabularySize".equals(fieldName)) {
                    vocabularySize = parser.getIntValue();
                } else if ("indexedTermCount".equals(fieldName)) {
                    indexedTermCount = parser.getIntValue();
                } else if ("indexedPostingCount".equals(fieldName)) {
                    indexedPostingCount = parser.getLongValue();
                } else if ("warnings".equals(fieldName)) {
                    warnings = parseStringArray(parser);
                } else if ("topCategories".equals(fieldName)) {
                    topCategories = parseNamedCounts(parser);
                } else if ("topTerms".equals(fieldName)) {
                    topTerms = parseNamedCounts(parser);
                } else if ("years".equals(fieldName)) {
                    yearSummaries = parseYearSummaries(parser);
                } else if ("documents".equals(fieldName)) {
                    documents = parseDocuments(parser);
                } else if ("documentKeywords".equals(fieldName)) {
                    documentKeywords = parseDocumentKeywords(parser);
                } else if ("postings".equals(fieldName)) {
                    postings = parsePostings(parser, documents);
                } else {
                    parser.skipChildren();
                }
            }
        } catch (IOException exception) {
            throw new UncheckedIOException("Failed to load the prebuilt demo snapshot.", exception);
        }

        if (documents.size() != documentKeywords.size()) {
            throw new IllegalStateException(
                    "Prebuilt snapshot is inconsistent: documents and documentKeywords lengths differ.");
        }

        Map<String, Integer> documentOrdinalsById = new HashMap<>(documents.size());
        int minYear = Integer.MAX_VALUE;
        int maxYear = Integer.MIN_VALUE;
        for (StoredDocument document : documents) {
            documentOrdinalsById.put(document.getId(), document.getOrdinal());
            minYear = Math.min(minYear, document.getYear());
            maxYear = Math.max(maxYear, document.getYear());
        }

        CorpusResponses.CorpusBuildSummary buildSummary =
                new CorpusResponses.CorpusBuildSummary(
                        status,
                        datasetDir,
                        builtAt,
                        buildMillis,
                        recordCount,
                        vocabularySize,
                        indexedTermCount,
                        indexedPostingCount,
                        warnings);

        log.info(
                "Prebuilt demo snapshot loaded: dataset={} documents={} terms={} postings={} tookMillis={}",
                datasetName,
                documents.size(),
                indexedTermCount,
                indexedPostingCount,
                elapsedMillis(start));

        return new LoadedSnapshot(
                mode,
                datasetName,
                datasetDir,
                warnings,
                List.copyOf(documents),
                Map.copyOf(documentOrdinalsById),
                List.copyOf(documentKeywords),
                Map.copyOf(postings),
                topCategories,
                topTerms,
                yearSummaries,
                buildSummary,
                minYear == Integer.MAX_VALUE ? 0 : minYear,
                maxYear == Integer.MIN_VALUE ? 0 : maxYear);
    }

    private Path resolveSnapshotPath() {
        String configuredPath = properties.getPrebuiltSnapshotPath();
        if (configuredPath == null || configuredPath.isBlank()) {
            throw new IllegalStateException("app.demo.prebuilt-snapshot-path is not configured.");
        }
        Path path = Path.of(configuredPath);
        if (!path.isAbsolute()) {
            path = Path.of("").toAbsolutePath().resolve(path);
        }
        return path.normalize();
    }

    private List<String> parseStringArray(JsonParser parser) throws IOException {
        ArrayList<String> values = new ArrayList<>();
        expect(parser, JsonToken.START_ARRAY);
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            values.add(parser.getValueAsString(""));
        }
        return List.copyOf(values);
    }

    private List<CorpusResponses.NamedCount> parseNamedCounts(JsonParser parser) throws IOException {
        ArrayList<CorpusResponses.NamedCount> counts = new ArrayList<>();
        expect(parser, JsonToken.START_ARRAY);
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            expect(parser, JsonToken.START_OBJECT);
            String name = "";
            long count = 0L;
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();
                if ("name".equals(fieldName)) {
                    name = parser.getValueAsString("");
                } else if ("count".equals(fieldName)) {
                    count = parser.getLongValue();
                } else {
                    parser.skipChildren();
                }
            }
            counts.add(new CorpusResponses.NamedCount(name, count));
        }
        return List.copyOf(counts);
    }

    private List<CorpusResponses.CorpusYearSummary> parseYearSummaries(JsonParser parser) throws IOException {
        ArrayList<CorpusResponses.CorpusYearSummary> summaries = new ArrayList<>();
        expect(parser, JsonToken.START_ARRAY);
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            expect(parser, JsonToken.START_OBJECT);
            int year = 0;
            long recordCount = 0L;
            List<CorpusResponses.DocumentKeyword> keywords = List.of();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();
                if ("year".equals(fieldName)) {
                    year = parser.getIntValue();
                } else if ("recordCount".equals(fieldName)) {
                    recordCount = parser.getLongValue();
                } else if ("keywords".equals(fieldName)) {
                    keywords = parseKeywordArray(parser);
                } else {
                    parser.skipChildren();
                }
            }
            summaries.add(new CorpusResponses.CorpusYearSummary(year, recordCount, keywords));
        }
        return List.copyOf(summaries);
    }

    private List<StoredDocument> parseDocuments(JsonParser parser) throws IOException {
        ArrayList<StoredDocument> documents = new ArrayList<>();
        expect(parser, JsonToken.START_ARRAY);
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            expect(parser, JsonToken.START_OBJECT);
            int ordinal = documents.size();
            String id = "";
            int year = 0;
            int month = 0;
            String title = "";
            String abstractText = "";
            String authors = "";
            List<String> categories = List.of();
            String primaryCategory = "";
            String updateDate = "";
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();
                if ("ordinal".equals(fieldName)) {
                    ordinal = parser.getIntValue();
                } else if ("id".equals(fieldName)) {
                    id = parser.getValueAsString("");
                } else if ("year".equals(fieldName)) {
                    year = parser.getIntValue();
                } else if ("month".equals(fieldName)) {
                    month = parser.getIntValue();
                } else if ("title".equals(fieldName)) {
                    title = parser.getValueAsString("");
                } else if ("abstractText".equals(fieldName)) {
                    abstractText = parser.getValueAsString("");
                } else if ("authors".equals(fieldName)) {
                    authors = parser.getValueAsString("");
                } else if ("categories".equals(fieldName)) {
                    categories = parseStringArray(parser);
                } else if ("primaryCategory".equals(fieldName)) {
                    primaryCategory = parser.getValueAsString("");
                } else if ("updateDate".equals(fieldName)) {
                    updateDate = parser.getValueAsString("");
                } else {
                    parser.skipChildren();
                }
            }
            documents.add(
                    new StoredDocument(
                            ordinal,
                            id,
                            year,
                            month,
                            title,
                            abstractText,
                            authors,
                            categories,
                            primaryCategory,
                            updateDate));
        }
        return List.copyOf(documents);
    }

    private List<List<CorpusResponses.DocumentKeyword>> parseDocumentKeywords(JsonParser parser) throws IOException {
        ArrayList<List<CorpusResponses.DocumentKeyword>> keywords = new ArrayList<>();
        expect(parser, JsonToken.START_ARRAY);
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            keywords.add(parseKeywordArray(parser));
        }
        return List.copyOf(keywords);
    }

    private List<CorpusResponses.DocumentKeyword> parseKeywordArray(JsonParser parser) throws IOException {
        ArrayList<CorpusResponses.DocumentKeyword> keywords = new ArrayList<>();
        expect(parser, JsonToken.START_ARRAY);
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            expect(parser, JsonToken.START_OBJECT);
            String term = "";
            double score = 0.0d;
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();
                if ("term".equals(fieldName)) {
                    term = parser.getValueAsString("");
                } else if ("score".equals(fieldName)) {
                    score = parser.getDoubleValue();
                } else {
                    parser.skipChildren();
                }
            }
            keywords.add(new CorpusResponses.DocumentKeyword(term, score));
        }
        return List.copyOf(keywords);
    }

    private Map<String, PostingList> parsePostings(JsonParser parser, List<StoredDocument> documents)
            throws IOException {
        Map<String, Integer> ordinalsById = new HashMap<>(documents.size());
        for (StoredDocument document : documents) {
            ordinalsById.put(document.getId(), document.getOrdinal());
        }

        HashMap<String, PostingList> postings = new HashMap<>();
        expect(parser, JsonToken.START_OBJECT);
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String term = parser.currentName();
            parser.nextToken();
            expect(parser, JsonToken.START_OBJECT);
            List<String> documentIds = List.of();
            ArrayList<Float> scores = new ArrayList<>();
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();
                if ("documentIds".equals(fieldName)) {
                    documentIds = parseStringArray(parser);
                } else if ("tfIdfScores".equals(fieldName)) {
                    expect(parser, JsonToken.START_ARRAY);
                    while (parser.nextToken() != JsonToken.END_ARRAY) {
                        scores.add((float) parser.getDoubleValue());
                    }
                } else {
                    parser.skipChildren();
                }
            }

            int[] documentOrdinals = new int[documentIds.size()];
            float[] tfIdfScores = new float[documentIds.size()];
            for (int index = 0; index < documentIds.size(); index++) {
                Integer ordinal = ordinalsById.get(documentIds.get(index));
                if (ordinal == null) {
                    throw new IllegalStateException(
                            "Prebuilt snapshot references an unknown document id: " + documentIds.get(index));
                }
                documentOrdinals[index] = ordinal;
                tfIdfScores[index] = scores.get(index);
            }
            postings.put(term, new PostingList(documentOrdinals, tfIdfScores));
        }
        return postings;
    }

    private void expect(JsonParser parser, JsonToken expected) throws IOException {
        JsonToken current = parser.currentToken();
        if (current != expected) {
            throw new IOException("Expected token " + expected + " but found " + current + ".");
        }
    }

    private int clampLimit(Integer requestedLimit) {
        int limit = requestedLimit == null ? corpusProperties.getSearchDefaultLimit() : requestedLimit;
        limit = Math.max(1, limit);
        return Math.min(limit, corpusProperties.getSearchMaxLimit());
    }

    private List<String> distinctTerms(List<String> terms) {
        if (terms.size() < 2) {
            return terms;
        }
        return new ArrayList<>(new LinkedHashSet<>(terms));
    }

    private List<SearchHit> selectTopHits(Map<Integer, Double> scores, int limit) {
        PriorityQueue<SearchHit> topHits =
                new PriorityQueue<>(
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

    private CorpusResponses.CorpusSearchResult toSearchResult(
            LoadedSnapshot snapshot, StoredDocument document, double score, Set<String> matchedTerms) {
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

    private static long elapsedMillis(long startNanos) {
        return Math.round((System.nanoTime() - startNanos) / 1_000_000.0d);
    }

    private static double roundScore(double value) {
        return Math.round(value * 1000.0d) / 1000.0d;
    }

    private record SearchHit(int documentOrdinal, double score) {}

    private record PostingList(int[] documentOrdinals, float[] tfIdfScores) {}

    private record LoadedSnapshot(
            String mode,
            String datasetName,
            String datasetDir,
            List<String> warnings,
            List<StoredDocument> documents,
            Map<String, Integer> documentOrdinalsById,
            List<List<CorpusResponses.DocumentKeyword>> documentKeywords,
            Map<String, PostingList> postings,
            List<CorpusResponses.NamedCount> topCategories,
            List<CorpusResponses.NamedCount> topTerms,
            List<CorpusResponses.CorpusYearSummary> yearSummaries,
            CorpusResponses.CorpusBuildSummary buildSummary,
            int minYear,
            int maxYear) {}
}
