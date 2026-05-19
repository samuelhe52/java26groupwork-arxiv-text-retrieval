package com.java26groupwork.finalassignment.corpus;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CorpusIndexServiceTest {

  @Test
  void localAnalysisWorkerCountCapsAtFourProcessors() {
    assertThat(CorpusIndexService.localAnalysisWorkerCount(100, 8)).isEqualTo(6);
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
