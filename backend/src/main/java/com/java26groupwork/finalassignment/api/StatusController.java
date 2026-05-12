package com.java26groupwork.finalassignment.api;

import com.java26groupwork.finalassignment.hadoop.HadoopProcessingService;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatusController {

    private final HadoopProcessingService hadoopProcessingService;

    public StatusController(HadoopProcessingService hadoopProcessingService) {
        this.hadoopProcessingService = hadoopProcessingService;
    }

    @GetMapping("/api/health")
    public Map<String, String> health() {
        return Map.of(
                "status", "ok",
                "backend", "spring-boot",
                "processing", hadoopProcessingService.describe());
    }
}
