package com.java26groupwork.finalassignment.corpus;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.corpus")
public class CorpusProperties {

    private String datasetDir = "../datasets/arxiv-cs-lg-2015-now-primary-cs-only";
    private boolean autoLoad = true;
    private int documentKeywordCount = 8;
    private int yearKeywordSince = 2019;
    private int yearKeywordCount = 10;
    private int yearKeywordMinCount = 40;
    private int searchDefaultLimit = 20;
    private int searchMaxLimit = 50;
    private double indexMaxDocumentFrequencyRatio = 0.20d;
    private String uploadBaseDir = "../datasets/uploads";

    public String getDatasetDir() {
        return datasetDir;
    }

    public void setDatasetDir(String datasetDir) {
        this.datasetDir = datasetDir;
    }

    public boolean isAutoLoad() {
        return autoLoad;
    }

    public void setAutoLoad(boolean autoLoad) {
        this.autoLoad = autoLoad;
    }

    public int getDocumentKeywordCount() {
        return documentKeywordCount;
    }

    public void setDocumentKeywordCount(int documentKeywordCount) {
        this.documentKeywordCount = documentKeywordCount;
    }

    public int getYearKeywordSince() {
        return yearKeywordSince;
    }

    public void setYearKeywordSince(int yearKeywordSince) {
        this.yearKeywordSince = yearKeywordSince;
    }

    public int getYearKeywordCount() {
        return yearKeywordCount;
    }

    public void setYearKeywordCount(int yearKeywordCount) {
        this.yearKeywordCount = yearKeywordCount;
    }

    public int getYearKeywordMinCount() {
        return yearKeywordMinCount;
    }

    public void setYearKeywordMinCount(int yearKeywordMinCount) {
        this.yearKeywordMinCount = yearKeywordMinCount;
    }

    public int getSearchDefaultLimit() {
        return searchDefaultLimit;
    }

    public void setSearchDefaultLimit(int searchDefaultLimit) {
        this.searchDefaultLimit = searchDefaultLimit;
    }

    public int getSearchMaxLimit() {
        return searchMaxLimit;
    }

    public void setSearchMaxLimit(int searchMaxLimit) {
        this.searchMaxLimit = searchMaxLimit;
    }

    public double getIndexMaxDocumentFrequencyRatio() {
        return indexMaxDocumentFrequencyRatio;
    }

    public void setIndexMaxDocumentFrequencyRatio(double indexMaxDocumentFrequencyRatio) {
        this.indexMaxDocumentFrequencyRatio = indexMaxDocumentFrequencyRatio;
    }

    public String getUploadBaseDir() {
        return uploadBaseDir;
    }

    public void setUploadBaseDir(String uploadBaseDir) {
        this.uploadBaseDir = uploadBaseDir;
    }
}
