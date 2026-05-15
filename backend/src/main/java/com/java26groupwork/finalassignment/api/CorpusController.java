package com.java26groupwork.finalassignment.api;

import com.java26groupwork.finalassignment.corpus.CorpusIndexService;
import com.java26groupwork.finalassignment.corpus.CorpusResponses;
import java.io.UncheckedIOException;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api")
public class CorpusController {

    private final CorpusIndexService corpusIndexService;

    public CorpusController(CorpusIndexService corpusIndexService) {
        this.corpusIndexService = corpusIndexService;
    }

    @GetMapping("/overview")
    public CorpusResponses.CorpusOverviewResponse overview() {
        return corpusIndexService.overview();
    }

    @GetMapping("/search")
    public CorpusResponses.CorpusSearchResponse search(
            @RequestParam(name = "q", defaultValue = "") String query,
            @RequestParam(name = "year", required = false) Integer year,
            @RequestParam(name = "category", required = false) String category,
            @RequestParam(name = "limit", required = false) Integer limit) {
        return corpusIndexService.search(query, year, category, limit);
    }

    @GetMapping("/documents/{id}")
    public CorpusResponses.DocumentDetailResponse documentById(@PathVariable("id") String id) {
        return corpusIndexService.documentById(id);
    }

    @PostMapping("/corpus/reload")
    public CorpusResponses.CorpusBuildSummary reload() {
        try {
            return corpusIndexService.requestReload();
        } catch (IllegalArgumentException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exception.getMessage(), exception);
        }
    }

    @PostMapping("/corpus/analyze")
    public CorpusResponses.CorpusBuildSummary analyze() {
        try {
            return corpusIndexService.requestReload();
        } catch (IllegalArgumentException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exception.getMessage(), exception);
        }
    }

    @PostMapping("/corpus/upload")
    public CorpusResponses.CorpusUploadResponse upload(
            @RequestParam("files") List<MultipartFile> files) {
        try {
            return corpusIndexService.importUploadedCorpus(files);
        } catch (IllegalArgumentException | UncheckedIOException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exception.getMessage(), exception);
        }
    }
}
