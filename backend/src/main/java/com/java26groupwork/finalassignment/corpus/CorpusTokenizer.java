package com.java26groupwork.finalassignment.corpus;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class CorpusTokenizer {

    private static final Pattern WORD_PATTERN = Pattern.compile("[A-Za-z][A-Za-z0-9'\\-]+");

    private static final Set<String> STOPWORDS = Set.of(
            "a", "an", "and", "are", "as", "at", "based", "be", "been", "but", "by",
            "can", "could", "dataset", "datasets", "different", "did", "do", "does",
            "doing", "first", "for", "from", "had", "has", "have", "having", "he",
            "her", "his", "how", "i", "if", "in", "into", "is", "it", "its", "may",
            "me", "method", "methods", "might", "model", "models", "must", "my", "new",
            "newly", "no", "not", "of", "on", "one", "or", "our", "out", "over", "paper",
            "present", "problem", "problems", "propose", "proposed", "results", "s",
            "second", "she", "should", "show", "so", "studies", "study", "such", "t",
            "than", "that", "the", "their", "them", "then", "there", "these", "they",
            "this", "those", "to", "too", "two", "under", "up", "using", "very", "was",
            "way", "we", "were", "what", "when", "where", "which", "who", "why", "will",
            "with", "work", "would", "you", "your", "also", "however");

    private CorpusTokenizer() {}

    public static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        StringBuilder normalized = new StringBuilder(value.length());
        boolean previousWhitespace = true;
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (Character.isWhitespace(current)) {
                if (!previousWhitespace) {
                    normalized.append(' ');
                    previousWhitespace = true;
                }
                continue;
            }
            normalized.append(current);
            previousWhitespace = false;
        }
        int length = normalized.length();
        if (length > 0 && normalized.charAt(length - 1) == ' ') {
            normalized.setLength(length - 1);
        }
        return normalized.toString();
    }

    public static List<String> tokenize(String text) {
        return tokenizeNormalized(normalize(text));
    }

    public static List<String> tokenizeNormalized(String normalizedText) {
        if (normalizedText == null || normalizedText.isEmpty()) {
            return List.of();
        }
        java.util.ArrayList<String> tokens = new java.util.ArrayList<>();
        appendTokens(normalizedText, tokens);
        return tokens.isEmpty() ? List.of() : tokens;
    }

    public static List<String> tokenizeNormalizedParts(String... normalizedParts) {
        if (normalizedParts == null || normalizedParts.length == 0) {
            return List.of();
        }
        java.util.ArrayList<String> tokens = new java.util.ArrayList<>();
        for (String normalizedPart : normalizedParts) {
            if (normalizedPart == null || normalizedPart.isEmpty()) {
                continue;
            }
            appendTokens(normalizedPart, tokens);
        }
        return tokens.isEmpty() ? List.of() : tokens;
    }

    private static void appendTokens(String normalizedText, java.util.ArrayList<String> tokens) {
        Matcher matcher = WORD_PATTERN.matcher(normalizedText.toLowerCase(Locale.ROOT));
        while (matcher.find()) {
            String token = matcher.group();
            if (!STOPWORDS.contains(token)) {
                tokens.add(token);
            }
        }
    }
}
