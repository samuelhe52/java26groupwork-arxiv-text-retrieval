package com.java26groupwork.finalassignment.corpus;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CorpusIndexServiceTest {

    @Test
    void localAnalysisWorkerCountCapsAtFourProcessors() {
        assertThat(CorpusIndexService.localAnalysisWorkerCount(100, 8)).isEqualTo(4);
    }

    @Test
    void localAnalysisWorkerCountDoesNotExceedDocumentCount() {
        assertThat(CorpusIndexService.localAnalysisWorkerCount(3, 8)).isEqualTo(3);
    }

    @Test
    void localAnalysisWorkerCountFallsBackToOneForTinyOrSingleCoreWorkloads() {
        assertThat(CorpusIndexService.localAnalysisWorkerCount(1, 8)).isEqualTo(1);
        assertThat(CorpusIndexService.localAnalysisWorkerCount(100, 1)).isEqualTo(1);
    }
}
