package com.java26groupwork.finalassignment.corpus;

import java.util.List;

public final class StoredDocument {

    private final int ordinal;
    private final String id;
    private final int year;
    private final int month;
    private final String title;
    private final String abstractText;
    private final String authors;
    private final List<String> categories;
    private final String primaryCategory;
    private final String updateDate;

    public StoredDocument(
            int ordinal,
            String id,
            int year,
            int month,
            String title,
            String abstractText,
            String authors,
            List<String> categories,
            String primaryCategory,
            String updateDate) {
        this.ordinal = ordinal;
        this.id = id;
        this.year = year;
        this.month = month;
        this.title = title;
        this.abstractText = abstractText;
        this.authors = authors;
        this.categories = categories;
        this.primaryCategory = primaryCategory;
        this.updateDate = updateDate;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public String getId() {
        return id;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public String getTitle() {
        return title;
    }

    public String getAbstractText() {
        return abstractText;
    }

    public String getAuthors() {
        return authors;
    }

    public List<String> getCategories() {
        return categories;
    }

    public String getPrimaryCategory() {
        return primaryCategory;
    }

    public String getUpdateDate() {
        return updateDate;
    }

    public boolean matchesYear(Integer targetYear) {
        return targetYear == null || year == targetYear;
    }

    public boolean matchesCategory(String targetCategory) {
        if (targetCategory == null || targetCategory.isBlank()) {
            return true;
        }
        for (String category : categories) {
            if (targetCategory.equalsIgnoreCase(category)) {
                return true;
            }
        }
        return false;
    }
}
