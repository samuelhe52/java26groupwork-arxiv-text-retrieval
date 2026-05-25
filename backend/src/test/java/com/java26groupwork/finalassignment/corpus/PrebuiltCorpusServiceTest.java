package com.java26groupwork.finalassignment.corpus;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PrebuiltCorpusServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void loadsPrebuiltSnapshotAndServesSearch() throws Exception {
        Path snapshotPath = tempDir.resolve("snapshot.json");
        Files.writeString(
                snapshotPath,
                """
                {
                  "mode": "hdfs-yarn-mapreduce-pipeline",
                  "datasetName": "demo-snapshot",
                  "datasetDir": "/demo/hdfs/prebuilt-cluster-demo",
                  "status": "ready",
                  "builtAt": "2026-05-25T14:15:59Z",
                  "buildMillis": 277564,
                  "recordCount": 2,
                  "vocabularySize": 5,
                  "indexedTermCount": 2,
                  "indexedPostingCount": 3,
                  "warnings": [
                    "Prebuilt demo snapshot served from disk. This view does not submit a live Hadoop job."
                  ],
                  "topCategories": [
                    { "name": "cs.LG", "count": 2 }
                  ],
                  "topTerms": [
                    { "name": "graph", "count": 2 }
                  ],
                  "years": [
                    {
                      "year": 2024,
                      "recordCount": 2,
                      "keywords": [
                        { "term": "graph", "score": 0.742 }
                      ]
                    }
                  ],
                  "documents": [
                    {
                      "ordinal": 0,
                      "id": "2401.00001",
                      "year": 2024,
                      "month": 1,
                      "authors": "Dana Example",
                      "title": "Graph Retrieval Demo",
                      "abstractText": "graph retrieval stays exact in the prebuilt snapshot",
                      "categories": ["cs.LG"],
                      "primaryCategory": "cs.LG",
                      "updateDate": "2024-01-03"
                    },
                    {
                      "ordinal": 1,
                      "id": "2401.00002",
                      "year": 2024,
                      "month": 1,
                      "authors": "Evan Example",
                      "title": "Neural Ranking Demo",
                      "abstractText": "neural ranking pairs with graph search in this corpus",
                      "categories": ["cs.LG"],
                      "primaryCategory": "cs.LG",
                      "updateDate": "2024-01-08"
                    }
                  ],
                  "documentKeywords": [
                    [
                      { "term": "graph", "score": 0.742 }
                    ],
                    [
                      { "term": "neural", "score": 0.651 },
                      { "term": "graph", "score": 0.412 }
                    ]
                  ],
                  "postings": {
                    "graph": {
                      "documentIds": ["2401.00001", "2401.00002"],
                      "tfIdfScores": [0.742, 0.412]
                    },
                    "neural": {
                      "documentIds": ["2401.00002"],
                      "tfIdfScores": [0.651]
                    }
                  }
                }
                """,
                StandardCharsets.UTF_8);

        PrebuiltDemoProperties demoProperties = new PrebuiltDemoProperties();
        demoProperties.setPrebuiltSnapshotPath(snapshotPath.toString());
        CorpusProperties corpusProperties = new CorpusProperties();

        PrebuiltCorpusService service =
                new PrebuiltCorpusService(demoProperties, corpusProperties, new ObjectMapper());

        var overview = service.overview();
        assertThat(overview.isReady()).isTrue();
        assertThat(overview.getRecordCount()).isEqualTo(2);
        assertThat(overview.getBuild().getBuildMillis()).isEqualTo(277564);

        var search = service.search("graph", null, null, 10);
        assertThat(search.isReady()).isTrue();
        assertThat(search.getTotalHits()).isEqualTo(2);
        assertThat(search.getResults()).hasSize(2);
        assertThat(search.getResults().get(0).getId()).isEqualTo("2401.00001");

        var detail = service.documentById("2401.00002");
        assertThat(detail.isReady()).isTrue();
        assertThat(detail.getDocument().getTitle()).isEqualTo("Neural Ranking Demo");
    }
}
