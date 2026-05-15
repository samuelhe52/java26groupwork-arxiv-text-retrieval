package com.java26groupwork.finalassignment.api;

import com.java26groupwork.finalassignment.corpus.CorpusIndexService;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class CorpusControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private CorpusIndexService corpusIndexService;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) throws Exception {
        Path datasetDir = new ClassPathResource("sample-dataset").getFile().toPath();
        registry.add("app.corpus.dataset-dir", datasetDir::toString);
        registry.add("app.corpus.auto-load", () -> "false");
        registry.add("app.corpus.index-max-document-frequency-ratio", () -> "1.0");
    }

    @BeforeEach
    void loadSampleCorpus() {
        corpusIndexService.restoreConfiguredDataset();
    }

    @Test
    void overviewReturnsCorpusShape() throws Exception {
        mockMvc.perform(get("/api/overview"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ready").value(true))
                .andExpect(jsonPath("$.recordCount").value(3))
                .andExpect(jsonPath("$.minYear").value(2024))
                .andExpect(jsonPath("$.maxYear").value(2025))
                .andExpect(jsonPath("$.years.length()").value(2));
    }

    @Test
    void searchReturnsRankedResults() throws Exception {
        mockMvc.perform(get("/api/search").param("q", "graph neural network"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ready").value(true))
                .andExpect(jsonPath("$.totalHits").value(1))
                .andExpect(jsonPath("$.results[0].id").value("2402.00002"))
                .andExpect(jsonPath("$.results[0].matchedTerms.length()").value(3));
    }

    @Test
    void reloadEndpointStartsBackgroundRebuild() throws Exception {
        mockMvc.perform(post("/api/corpus/reload"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("reloading"));
    }

    @Test
    void uploadEndpointAcceptsJsonDatasetFiles() throws Exception {
        String uploadPayload = """
                [
                  {
                    "id": "2601.00001",
                    "title": "Uploaded Retrieval Benchmarks",
                    "abstract": "Uploaded corpora can now be indexed directly from the browser.",
                    "authors": "Dana Example",
                    "categories": "cs.IR cs.LG",
                    "year": 2026,
                    "month": 1,
                    "update_date": "2026-01-03"
                  },
                  {
                    "id": "2601.00002",
                    "title": "JSONL Import for Local Search",
                    "abstract": "This record validates multi-document upload support for corpus search.",
                    "authors": "Evan Example",
                    "categories_list": ["cs.LG"],
                    "primary_category": "cs.LG",
                    "year": 2026,
                    "month": 1,
                    "update_date": "2026-01-07"
                  }
                ]
                """;

        MockMultipartFile file = new MockMultipartFile(
                "files",
                "uploaded.json",
                "application/json",
                uploadPayload.getBytes());

        mockMvc.perform(multipart("/api/corpus/upload").file(file))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("reloading"))
                .andExpect(jsonPath("$.fileCount").value(1))
                .andExpect(jsonPath("$.importedRecordCount").value(2))
                .andExpect(jsonPath("$.uploadedFiles[0]").value("uploaded.json"))
                .andExpect(jsonPath("$.build.status").value("reloading"));
    }
}
