package com.java26groupwork.finalassignment.corpus;

import java.time.Instant;
import java.util.List;

public final class CorpusResponses {

    private CorpusResponses() {}

    public static final class NamedCount {
        private final String name;
        private final long count;

        public NamedCount(String name, long count) {
            this.name = name;
            this.count = count;
        }

        public String getName() {
            return name;
        }

        public long getCount() {
            return count;
        }
    }

    public static final class DocumentKeyword {
        private final String term;
        private final double score;

        public DocumentKeyword(String term, double score) {
            this.term = term;
            this.score = score;
        }

        public String getTerm() {
            return term;
        }

        public double getScore() {
            return score;
        }
    }

    public static final class CorpusYearSummary {
        private final int year;
        private final long recordCount;
        private final List<DocumentKeyword> keywords;

        public CorpusYearSummary(int year, long recordCount, List<DocumentKeyword> keywords) {
            this.year = year;
            this.recordCount = recordCount;
            this.keywords = keywords;
        }

        public int getYear() {
            return year;
        }

        public long getRecordCount() {
            return recordCount;
        }

        public List<DocumentKeyword> getKeywords() {
            return keywords;
        }
    }

    public static final class CorpusBuildSummary {
        private final String status;
        private final String datasetDir;
        private final Instant builtAt;
        private final long buildMillis;
        private final long recordCount;
        private final int vocabularySize;
        private final int indexedTermCount;
        private final long indexedPostingCount;
        private final List<String> warnings;

        public CorpusBuildSummary(
                String status,
                String datasetDir,
                Instant builtAt,
                long buildMillis,
                long recordCount,
                int vocabularySize,
                int indexedTermCount,
                long indexedPostingCount,
                List<String> warnings) {
            this.status = status;
            this.datasetDir = datasetDir;
            this.builtAt = builtAt;
            this.buildMillis = buildMillis;
            this.recordCount = recordCount;
            this.vocabularySize = vocabularySize;
            this.indexedTermCount = indexedTermCount;
            this.indexedPostingCount = indexedPostingCount;
            this.warnings = warnings;
        }

        public String getStatus() {
            return status;
        }

        public String getDatasetDir() {
            return datasetDir;
        }

        public Instant getBuiltAt() {
            return builtAt;
        }

        public long getBuildMillis() {
            return buildMillis;
        }

        public long getRecordCount() {
            return recordCount;
        }

        public int getVocabularySize() {
            return vocabularySize;
        }

        public int getIndexedTermCount() {
            return indexedTermCount;
        }

        public long getIndexedPostingCount() {
            return indexedPostingCount;
        }

        public List<String> getWarnings() {
            return warnings;
        }
    }

    public static final class CorpusOverviewResponse {
        private final boolean ready;
        private final String mode;
        private final String datasetName;
        private final String datasetDir;
        private final String status;
        private final long recordCount;
        private final int minYear;
        private final int maxYear;
        private final List<NamedCount> topCategories;
        private final List<NamedCount> topTerms;
        private final List<CorpusYearSummary> years;
        private final CorpusBuildSummary build;

        public CorpusOverviewResponse(
                boolean ready,
                String mode,
                String datasetName,
                String datasetDir,
                String status,
                long recordCount,
                int minYear,
                int maxYear,
                List<NamedCount> topCategories,
                List<NamedCount> topTerms,
                List<CorpusYearSummary> years,
                CorpusBuildSummary build) {
            this.ready = ready;
            this.mode = mode;
            this.datasetName = datasetName;
            this.datasetDir = datasetDir;
            this.status = status;
            this.recordCount = recordCount;
            this.minYear = minYear;
            this.maxYear = maxYear;
            this.topCategories = topCategories;
            this.topTerms = topTerms;
            this.years = years;
            this.build = build;
        }

        public boolean isReady() {
            return ready;
        }

        public String getMode() {
            return mode;
        }

        public String getDatasetName() {
            return datasetName;
        }

        public String getDatasetDir() {
            return datasetDir;
        }

        public String getStatus() {
            return status;
        }

        public long getRecordCount() {
            return recordCount;
        }

        public int getMinYear() {
            return minYear;
        }

        public int getMaxYear() {
            return maxYear;
        }

        public List<NamedCount> getTopCategories() {
            return topCategories;
        }

        public List<NamedCount> getTopTerms() {
            return topTerms;
        }

        public List<CorpusYearSummary> getYears() {
            return years;
        }

        public CorpusBuildSummary getBuild() {
            return build;
        }
    }

    public static final class CorpusSearchResult {
        private final String id;
        private final int year;
        private final String title;
        private final String abstractSnippet;
        private final String authors;
        private final String primaryCategory;
        private final List<String> categories;
        private final List<String> matchedTerms;
        private final List<DocumentKeyword> keywords;
        private final double score;

        public CorpusSearchResult(
                String id,
                int year,
                String title,
                String abstractSnippet,
                String authors,
                String primaryCategory,
                List<String> categories,
                List<String> matchedTerms,
                List<DocumentKeyword> keywords,
                double score) {
            this.id = id;
            this.year = year;
            this.title = title;
            this.abstractSnippet = abstractSnippet;
            this.authors = authors;
            this.primaryCategory = primaryCategory;
            this.categories = categories;
            this.matchedTerms = matchedTerms;
            this.keywords = keywords;
            this.score = score;
        }

        public String getId() {
            return id;
        }

        public int getYear() {
            return year;
        }

        public String getTitle() {
            return title;
        }

        public String getAbstractSnippet() {
            return abstractSnippet;
        }

        public String getAuthors() {
            return authors;
        }

        public String getPrimaryCategory() {
            return primaryCategory;
        }

        public List<String> getCategories() {
            return categories;
        }

        public List<String> getMatchedTerms() {
            return matchedTerms;
        }

        public List<DocumentKeyword> getKeywords() {
            return keywords;
        }

        public double getScore() {
            return score;
        }
    }

    public static final class CorpusSearchResponse {
        private final boolean ready;
        private final String query;
        private final List<String> normalizedTerms;
        private final int limit;
        private final long totalHits;
        private final long tookMillis;
        private final List<String> warnings;
        private final List<CorpusSearchResult> results;

        public CorpusSearchResponse(
                boolean ready,
                String query,
                List<String> normalizedTerms,
                int limit,
                long totalHits,
                long tookMillis,
                List<String> warnings,
                List<CorpusSearchResult> results) {
            this.ready = ready;
            this.query = query;
            this.normalizedTerms = normalizedTerms;
            this.limit = limit;
            this.totalHits = totalHits;
            this.tookMillis = tookMillis;
            this.warnings = warnings;
            this.results = results;
        }

        public boolean isReady() {
            return ready;
        }

        public String getQuery() {
            return query;
        }

        public List<String> getNormalizedTerms() {
            return normalizedTerms;
        }

        public int getLimit() {
            return limit;
        }

        public long getTotalHits() {
            return totalHits;
        }

        public long getTookMillis() {
            return tookMillis;
        }

        public List<String> getWarnings() {
            return warnings;
        }

        public List<CorpusSearchResult> getResults() {
            return results;
        }
    }

    public static final class DocumentDetailResponse {
        private final boolean ready;
        private final String status;
        private final StoredDocument document;
        private final List<DocumentKeyword> keywords;

        public DocumentDetailResponse(
                boolean ready, String status, StoredDocument document, List<DocumentKeyword> keywords) {
            this.ready = ready;
            this.status = status;
            this.document = document;
            this.keywords = keywords;
        }

        public boolean isReady() {
            return ready;
        }

        public String getStatus() {
            return status;
        }

        public StoredDocument getDocument() {
            return document;
        }

        public List<DocumentKeyword> getKeywords() {
            return keywords;
        }
    }

    public static final class CorpusUploadResponse {
        private final String status;
        private final String datasetDir;
        private final int fileCount;
        private final long importedRecordCount;
        private final List<String> uploadedFiles;
        private final List<String> warnings;
        private final CorpusBuildSummary build;

        public CorpusUploadResponse(
                String status,
                String datasetDir,
                int fileCount,
                long importedRecordCount,
                List<String> uploadedFiles,
                List<String> warnings,
                CorpusBuildSummary build) {
            this.status = status;
            this.datasetDir = datasetDir;
            this.fileCount = fileCount;
            this.importedRecordCount = importedRecordCount;
            this.uploadedFiles = uploadedFiles;
            this.warnings = warnings;
            this.build = build;
        }

        public String getStatus() {
            return status;
        }

        public String getDatasetDir() {
            return datasetDir;
        }

        public int getFileCount() {
            return fileCount;
        }

        public long getImportedRecordCount() {
            return importedRecordCount;
        }

        public List<String> getUploadedFiles() {
            return uploadedFiles;
        }

        public List<String> getWarnings() {
            return warnings;
        }

        public CorpusBuildSummary getBuild() {
            return build;
        }
    }
}
