package com.java26groupwork.finalassignment.api;

import com.java26groupwork.finalassignment.corpus.CorpusIndexService;
import com.java26groupwork.finalassignment.hadoop.HadoopProcessingService;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatusController {

    private final CorpusIndexService corpusIndexService;
    private final HadoopProcessingService hadoopProcessingService;

    public StatusController(
            CorpusIndexService corpusIndexService, HadoopProcessingService hadoopProcessingService) {
        this.corpusIndexService = corpusIndexService;
        this.hadoopProcessingService = hadoopProcessingService;
    }

    @GetMapping("/api/health")
    public Map<String, Object> health() {
        return Map.of(
                "status", "ok",
                "backend", "spring-boot",
                "processing", hadoopProcessingService.describe(),
                "corpus", Map.of(
                        "ready", corpusIndexService.isReady(),
                        "documents", corpusIndexService.documentCount(),
                        "build", corpusIndexService.buildSummary()),
                "hadoop", hadoopProcessingService.describeConnection());
    }
}
