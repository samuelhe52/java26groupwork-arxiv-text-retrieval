package com.java26groupwork.finalassignment.api;

import com.java26groupwork.finalassignment.corpus.CorpusIndexService;
import com.java26groupwork.finalassignment.corpus.CorpusProperties;
import com.java26groupwork.finalassignment.hadoop.HadoopProcessingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.autoconfigure.web.servlet.MultipartProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class CorpusControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private CorpusIndexService corpusIndexService;

    @TempDir
    private Path tempDir;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("app.corpus.index-max-document-frequency-ratio", () -> "1.0");
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
                new CorpusProperties(),
                new ObjectMapper(),
                org.mockito.Mockito.mock(HadoopProcessingService.class));

        org.assertj.core.api.Assertions.assertThatThrownBy(freshService::requestReload)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Upload a dataset before submitting analysis.");
    }

    @Test
    void analyzeUsesConfiguredDatasetWithoutAutoRunningAtStartup() {
        CorpusProperties properties = new CorpusProperties();
        ObjectMapper objectMapper = new ObjectMapper();
        HadoopProcessingService processingService = org.mockito.Mockito.mock(HadoopProcessingService.class);
        Path configuredDatasetDir = Path.of("/configured/corpus/years");

        org.mockito.Mockito.when(processingService.configuredDatasetDir()).thenReturn(configuredDatasetDir);
        org.mockito.Mockito.when(processingService.processDataset(
                        org.mockito.ArgumentMatchers.eq(configuredDatasetDir),
                        org.mockito.ArgumentMatchers.same(properties),
                        org.mockito.ArgumentMatchers.any()))
                .thenReturn(new HadoopProcessingService.ProcessingArtifacts(
                        configuredDatasetDir,
                        new org.apache.hadoop.fs.Path("file:/tmp/work"),
                        new org.apache.hadoop.fs.Path("file:/tmp/input"),
                        new org.apache.hadoop.fs.Path("file:/tmp/tf"),
                        new org.apache.hadoop.fs.Path("file:/tmp/df"),
                        new org.apache.hadoop.fs.Path("file:/tmp/tfidf"),
                        new org.apache.hadoop.fs.Path("file:/tmp/keywords"),
                        new org.apache.hadoop.fs.Path("file:/tmp/index"),
                        Instant.now(),
                        0L,
                        0,
                        List.of()));

        CorpusIndexService freshService = new CorpusIndexService(properties, objectMapper, processingService);
        try {
            org.assertj.core.api.Assertions.assertThat(freshService.buildSummary().getStatus()).isEqualTo("not-analyzed");
            org.assertj.core.api.Assertions.assertThat(freshService.buildSummary().getDatasetDir())
                    .isEqualTo(configuredDatasetDir.toString());
            org.mockito.Mockito.verify(processingService, org.mockito.Mockito.never())
                    .processDataset(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());

            org.assertj.core.api.Assertions.assertThatCode(freshService::requestReload).doesNotThrowAnyException();
            org.mockito.Mockito.verify(processingService, org.mockito.Mockito.timeout(1000))
                    .processDataset(
                            org.mockito.ArgumentMatchers.eq(configuredDatasetDir),
                            org.mockito.ArgumentMatchers.same(properties),
                            org.mockito.ArgumentMatchers.any());
        } finally {
            freshService.destroy();
        }
    }

    @Test
    void uploadWritesFlatCanonicalShardAndManifest() throws Exception {
        CorpusProperties properties = new CorpusProperties();
        properties.setUploadBaseDir(tempDir.toString());
        ObjectMapper objectMapper = new ObjectMapper();
        HadoopProcessingService processingService = org.mockito.Mockito.mock(HadoopProcessingService.class);
        CorpusIndexService freshService = new CorpusIndexService(properties, objectMapper, processingService);
        try {
            MockMultipartFile file = new MockMultipartFile(
                    "files",
                    "fresh.jsonl",
                    "application/json",
                    """
                    {"id":"2602.00001","title":"Uploaded Paper","abstract":"fresh upload pipeline","year":2026}
                    {"id":"2602.00002","title":"Another Paper","abstract":"flat canonical shard","year":2026}
                    """.getBytes(java.nio.charset.StandardCharsets.UTF_8));

            var response = freshService.importUploadedCorpus(List.of(file));
            Path datasetDir = Path.of(response.getDatasetDir());

            org.assertj.core.api.Assertions.assertThat(Files.exists(datasetDir.resolve("upload.jsonl"))).isTrue();
            org.assertj.core.api.Assertions.assertThat(Files.exists(datasetDir.resolve("manifest.json"))).isTrue();
            org.assertj.core.api.Assertions.assertThat(Files.exists(datasetDir.resolve("years"))).isFalse();
            org.assertj.core.api.Assertions.assertThat(
                            objectMapper.readTree(datasetDir.resolve("manifest.json").toFile())
                                    .path("totals")
                                    .path("records")
                                    .asLong())
                    .isEqualTo(2L);
        } finally {
            freshService.destroy();
        }
    }

    @Test
    void localAnalysisPrefersRootCanonicalShardOverYearsDirectory() throws Exception {
        Path datasetDir = tempDir.resolve("dataset");
        Files.createDirectories(datasetDir.resolve("years"));
        Files.writeString(
                datasetDir.resolve("upload.jsonl"),
                """
                {"id":"2604.00001","title":"Root Canonical Paper","abstract":"root merged shard wins","year":2026,"categories":"cs.LG","primary_category":"cs.LG"}
                """,
                java.nio.charset.StandardCharsets.UTF_8);
        Files.writeString(
                datasetDir.resolve("years").resolve("sample_2025.jsonl"),
                """
                {"id":"2501.00001","title":"Year Shard Paper","abstract":"should be ignored when root shard exists","year":2025,"categories":"cs.LG","primary_category":"cs.LG"}
                """,
                java.nio.charset.StandardCharsets.UTF_8);
        Files.writeString(
                datasetDir.resolve("manifest.json"),
                """
                {
                  "totals": {
                    "records": 1,
                    "shards": 1
                  }
                }
                """,
                java.nio.charset.StandardCharsets.UTF_8);

        CorpusProperties properties = new CorpusProperties();
        properties.setIndexMaxDocumentFrequencyRatio(1.0);
        ObjectMapper objectMapper = new ObjectMapper();
        HadoopProcessingService processingService = org.mockito.Mockito.mock(HadoopProcessingService.class);
        org.mockito.Mockito.when(processingService.configuredDatasetDir()).thenReturn(datasetDir);
        org.mockito.Mockito.when(processingService.isLocalMode()).thenReturn(true);

        CorpusIndexService freshService = new CorpusIndexService(properties, objectMapper, processingService);
        try {
            freshService.requestReload();
            waitForServiceBuildStatus(freshService, "ready", Duration.ofSeconds(2));

            org.assertj.core.api.Assertions.assertThat(freshService.documentCount()).isEqualTo(1);
            var search = freshService.search("merged", null, null, 10);
            org.assertj.core.api.Assertions.assertThat(search.getTotalHits()).isEqualTo(1);
            org.assertj.core.api.Assertions.assertThat(search.getResults()).hasSize(1);
            org.assertj.core.api.Assertions.assertThat(search.getResults().get(0).getId()).isEqualTo("2604.00001");
        } finally {
            freshService.destroy();
        }
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
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("Bad Request"))
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("Unexpected end-of-input")));
    }

    @Test
    void uploadEndpointReturnsBadRequestForMalformedJsonLines() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "files",
                "broken.jsonl",
                "application/json",
                "{\"id\":\"2603.00001\",\"title\":\"Broken\",\"abstract\":\"Missing brace\"".getBytes());

        mockMvc.perform(multipart("/api/corpus/upload").file(file))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.error").value("Bad Request"))
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("Unexpected end-of-input")));
    }

    @Test
    void uploadSizeExceededReturnsPayloadTooLargeMessage() {
        MultipartProperties multipartProperties = new MultipartProperties();
        multipartProperties.setMaxRequestSize(org.springframework.util.unit.DataSize.ofGigabytes(1));
        ApiExceptionHandler handler = new ApiExceptionHandler(multipartProperties);

        var response = handler.handleMaxUploadSizeExceeded(new MaxUploadSizeExceededException(1024));

        org.assertj.core.api.Assertions.assertThat(response.getStatusCode()).isEqualTo(org.springframework.http.HttpStatus.PAYLOAD_TOO_LARGE);
        org.assertj.core.api.Assertions.assertThat(response.getBody()).isNotNull();
        org.assertj.core.api.Assertions.assertThat(response.getBody().message())
                .contains("1 GB")
                .contains("Split the dataset");
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

    private void waitForServiceBuildStatus(CorpusIndexService service, String expectedStatus, Duration timeout)
            throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (expectedStatus.equals(service.buildSummary().getStatus())) {
                return;
            }
            Thread.sleep(25L);
        }
        org.assertj.core.api.Assertions.assertThat(service.buildSummary().getStatus()).isEqualTo(expectedStatus);
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
