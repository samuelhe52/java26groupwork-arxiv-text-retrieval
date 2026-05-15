package com.java26groupwork.finalassignment.corpus;

import java.time.Instant;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

final class CorpusIndexSnapshot {

    final boolean ready;
    final String status;
    final String datasetName;
    final String datasetDir;
    final List<String> warnings;
    final List<StoredDocument> documents;
    final Map<String, Integer> documentOrdinalsById;
    final Map<String, Integer> documentFrequency;
    final Map<String, PostingList> postings;
    final List<List<CorpusResponses.DocumentKeyword>> documentKeywords;
    final List<CorpusResponses.NamedCount> topCategories;
    final List<CorpusResponses.NamedCount> topTerms;
    final List<CorpusResponses.CorpusYearSummary> yearSummaries;
    final CorpusResponses.CorpusBuildSummary buildSummary;
    final Instant builtAt;
    final int minYear;
    final int maxYear;

    CorpusIndexSnapshot(
            boolean ready,
            String status,
            String datasetName,
            String datasetDir,
            List<String> warnings,
            List<StoredDocument> documents,
            Map<String, Integer> documentOrdinalsById,
            Map<String, Integer> documentFrequency,
            Map<String, PostingList> postings,
            List<List<CorpusResponses.DocumentKeyword>> documentKeywords,
            List<CorpusResponses.NamedCount> topCategories,
            List<CorpusResponses.NamedCount> topTerms,
            List<CorpusResponses.CorpusYearSummary> yearSummaries,
            CorpusResponses.CorpusBuildSummary buildSummary,
            Instant builtAt,
            int minYear,
            int maxYear) {
        this.ready = ready;
        this.status = status;
        this.datasetName = datasetName;
        this.datasetDir = datasetDir;
        this.warnings = warnings;
        this.documents = documents;
        this.documentOrdinalsById = documentOrdinalsById;
        this.documentFrequency = documentFrequency;
        this.postings = postings;
        this.documentKeywords = documentKeywords;
        this.topCategories = topCategories;
        this.topTerms = topTerms;
        this.yearSummaries = yearSummaries;
        this.buildSummary = buildSummary;
        this.builtAt = builtAt;
        this.minYear = minYear;
        this.maxYear = maxYear;
    }

    static CorpusIndexSnapshot empty(String datasetDir, String status, List<String> warnings) {
        CorpusResponses.CorpusBuildSummary buildSummary = new CorpusResponses.CorpusBuildSummary(
                status,
                datasetDir,
                Instant.now(),
                0L,
                0L,
                0,
                0,
                0L,
                warnings);
        return new CorpusIndexSnapshot(
                false,
                status,
                datasetName(datasetDir),
                datasetDir,
                warnings,
                List.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                buildSummary,
                buildSummary.getBuiltAt(),
                0,
                0);
    }

    private static String datasetName(String datasetDir) {
        try {
            Path path = Path.of(datasetDir);
            Path fileName = path.getFileName();
            return fileName == null ? datasetDir : fileName.toString();
        } catch (RuntimeException exception) {
            return datasetDir;
        }
    }

    long recordCount() {
        return documents.size();
    }

    static final class PostingList {
        private final int[] documentOrdinals;
        private final float[] tfIdfScores;

        PostingList(int[] documentOrdinals, float[] tfIdfScores) {
            this.documentOrdinals = documentOrdinals;
            this.tfIdfScores = tfIdfScores;
        }

        int[] documentOrdinals() {
            return documentOrdinals;
        }

        float[] tfIdfScores() {
            return tfIdfScores;
        }
    }
}
