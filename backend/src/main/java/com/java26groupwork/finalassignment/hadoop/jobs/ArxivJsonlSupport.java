package com.java26groupwork.finalassignment.hadoop.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.java26groupwork.finalassignment.corpus.CorpusTokenizer;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.io.Text;

final class ArxivJsonlSupport {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private ArxivJsonlSupport() {}

    static ParsedDocument parsedDocument(Text value) throws IOException {
        JsonNode node = parse(value);
        String documentId = node.path("id").asText("").trim();
        String title = CorpusTokenizer.normalize(node.path("title").asText(""));
        String abstractText = CorpusTokenizer.normalize(node.path("abstract").asText(""));
        return new ParsedDocument(
                documentId,
                CorpusTokenizer.tokenizeNormalizedParts(title, abstractText));
    }

    static Set<String> uniqueTokens(ParsedDocument document) {
        return new HashSet<>(document.tokens());
    }

    private static JsonNode parse(Text value) throws IOException {
        return OBJECT_MAPPER.readTree(value.toString());
    }

    record ParsedDocument(String documentId, List<String> tokens) {}
}
