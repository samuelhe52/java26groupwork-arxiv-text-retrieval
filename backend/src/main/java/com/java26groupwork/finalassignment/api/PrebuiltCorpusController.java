package com.java26groupwork.finalassignment.api;

import com.java26groupwork.finalassignment.corpus.CorpusResponses;
import com.java26groupwork.finalassignment.corpus.PrebuiltCorpusService;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/prebuilt")
public class PrebuiltCorpusController {

    private final PrebuiltCorpusService prebuiltCorpusService;

    public PrebuiltCorpusController(PrebuiltCorpusService prebuiltCorpusService) {
        this.prebuiltCorpusService = prebuiltCorpusService;
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return prebuiltCorpusService.health();
    }

    @GetMapping("/overview")
    public CorpusResponses.CorpusOverviewResponse overview() {
        return prebuiltCorpusService.overview();
    }

    @GetMapping("/search")
    public CorpusResponses.CorpusSearchResponse search(
            @RequestParam(name = "q", defaultValue = "") String query,
            @RequestParam(name = "year", required = false) Integer year,
            @RequestParam(name = "category", required = false) String category,
            @RequestParam(name = "limit", required = false) Integer limit) {
        return prebuiltCorpusService.search(query, year, category, limit);
    }

    @GetMapping("/documents/{id}")
    public CorpusResponses.DocumentDetailResponse documentById(@PathVariable("id") String id) {
        return prebuiltCorpusService.documentById(id);
    }
}
