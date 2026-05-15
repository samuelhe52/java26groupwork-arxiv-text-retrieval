package com.java26groupwork.finalassignment.api;

import com.java26groupwork.finalassignment.corpus.CorpusIndexService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
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
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class CorpusControllerTest {

    private static Path localHadoopBaseDir;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private CorpusIndexService corpusIndexService;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) throws Exception {
        localHadoopBaseDir = Files.createTempDirectory("hadoop-local-test-");
        registry.add("app.corpus.index-max-document-frequency-ratio", () -> "1.0");
        registry.add("app.hadoop.local-base-path", () -> localHadoopBaseDir.toString());
    }

    @BeforeEach
    void loadSampleCorpus() throws Exception {
        stageAndAnalyzeSampleCorpus();
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
    void analyzeEndpointRejectsMissingDataset() throws Exception {
        CorpusIndexService freshService = new CorpusIndexService(
                new com.java26groupwork.finalassignment.corpus.CorpusProperties(),
                new com.fasterxml.jackson.databind.ObjectMapper(),
                org.mockito.Mockito.mock(com.java26groupwork.finalassignment.hadoop.HadoopProcessingService.class));

        org.assertj.core.api.Assertions.assertThatThrownBy(freshService::requestReload)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Upload a dataset before submitting analysis.");
    }

    @Test
    void uploadEndpointAcceptsJsonDatasetFiles() throws Exception {
        String uploadPayload = """
                {
                  "papers": [
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
                      "title": "JSON Import for Local Search",
                      "abstract": "This record validates multi-document upload support for corpus search.",
                      "authors": "Evan Example",
                      "categories_list": ["cs.LG"],
                      "primary_category": "cs.LG",
                      "year": 2026,
                      "month": 1,
                      "update_date": "2026-01-07"
                    }
                  ]
                }
                """;

        MockMultipartFile file = new MockMultipartFile(
                "files",
                "uploaded.json",
                "application/json",
                uploadPayload.getBytes());

        mockMvc.perform(multipart("/api/corpus/upload").file(file))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("staged"))
                .andExpect(jsonPath("$.fileCount").value(1))
                .andExpect(jsonPath("$.importedRecordCount").value(2))
                .andExpect(jsonPath("$.uploadedFiles[0]").value("uploaded.json"))
                .andExpect(jsonPath("$.build.status").value("staged"));
    }

    @Test
    void uploadEndpointReturnsBadRequestForMalformedJson() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "files",
                "broken.json",
                "application/json",
                "{\"papers\": [".getBytes());

        mockMvc.perform(multipart("/api/corpus/upload").file(file))
                .andExpect(status().isBadRequest())
                .andExpect(status().reason(org.hamcrest.Matchers.containsString("Unexpected end-of-input")));
    }

    @Test
    void analyzeEndpointBuildsUploadedDatasetAndDeduplicatesIds() throws Exception {
        String uploadPayload = """
                [
                  {
                    "id": "2602.00001",
                    "title": "First Copy",
                    "abstract": "Keyword coverage for federated retrieval pipelines.",
                    "authors": "Dana Example",
                    "categories": "cs.IR cs.LG",
                    "year": 2026,
                    "month": 2,
                    "update_date": "2026-02-03"
                  },
                  {
                    "id": "2602.00001",
                    "title": "Duplicate Copy",
                    "abstract": "This duplicate id should be skipped during analysis.",
                    "authors": "Dana Example",
                    "categories": "cs.IR cs.LG",
                    "year": 2026,
                    "month": 2,
                    "update_date": "2026-02-04"
                  }
                ]
                """;

        MockMultipartFile file = new MockMultipartFile(
                "files",
                "duplicates.json",
                "application/json",
                uploadPayload.getBytes());

        mockMvc.perform(multipart("/api/corpus/upload").file(file))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("staged"));

        mockMvc.perform(post("/api/corpus/analyze"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("reloading"));

        waitForBuildStatus("ready", Duration.ofSeconds(20));

        mockMvc.perform(get("/api/overview"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ready").value(true))
                .andExpect(jsonPath("$.recordCount").value(1))
                .andExpect(jsonPath("$.build.warnings[0]").value(
                        org.hamcrest.Matchers.containsString("Skipped duplicate document id")));

        mockMvc.perform(get("/api/search").param("q", "federated retrieval"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalHits").value(1))
                .andExpect(jsonPath("$.results[0].id").value("2602.00001"));

        mockMvc.perform(get("/api/documents/2602.00001"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ready").value(true))
                .andExpect(jsonPath("$.document.title").value("First Copy"));
    }

    private void waitForBuildStatus(String expectedStatus, Duration timeout) throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            String responseBody = mockMvc.perform(get("/api/overview"))
                    .andExpect(status().isOk())
                    .andReturn()
                    .getResponse()
                    .getContentAsString();
            if (responseBody.contains("\"status\":\"" + expectedStatus + "\"")
                    && responseBody.contains("\"build\":{\"status\":\"" + expectedStatus + "\"")) {
                return;
            }
            Thread.sleep(150L);
        }
        mockMvc.perform(get("/api/overview"))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("\"status\":\"" + expectedStatus + "\"")));
    }

    private void stageAndAnalyzeSampleCorpus() throws Exception {
        Path yearsDir = new ClassPathResource("sample-dataset/years").getFile().toPath();
        List<MockMultipartFile> files;
        try (var stream = Files.list(yearsDir)) {
            files = stream
                    .filter(path -> path.getFileName().toString().endsWith(".jsonl"))
                    .sorted()
                    .map(path -> {
                        try {
                            return new MockMultipartFile(
                                    "files",
                                    path.getFileName().toString(),
                                    "application/json",
                                    Files.readAllBytes(path));
                        } catch (Exception exception) {
                            throw new RuntimeException(exception);
                        }
                    })
                    .toList();
        }

        corpusIndexService.importUploadedCorpus(List.copyOf(files));
        corpusIndexService.reload();
    }
}
