package com.java26groupwork.finalassignment.hadoop;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HadoopProcessingServiceTest {

    @Test
    void clusterExecutionPlanCapsReducerBudgetAtFourNodes() {
        HadoopProcessingService.ClusterExecutionPlan plan = HadoopProcessingService.clusterExecutionPlan(8);

        assertThat(plan.reducerBudget()).isEqualTo(4);
        assertThat(plan.termFrequencyReducers()).isEqualTo(2);
        assertThat(plan.documentFrequencyReducers()).isEqualTo(2);
        assertThat(plan.tfIdfReducers()).isEqualTo(4);
        assertThat(plan.documentKeywordsReducers()).isEqualTo(1);
        assertThat(plan.invertedIndexReducers()).isEqualTo(3);
        assertThat(plan.parallelFirstWave()).isTrue();
        assertThat(plan.parallelFinalWave()).isTrue();
    }

    @Test
    void clusterExecutionPlanBiasesThreeReducerBudgetTowardTermFrequencyAndIndex() {
        HadoopProcessingService.ClusterExecutionPlan plan = HadoopProcessingService.clusterExecutionPlan(3);

        assertThat(plan.reducerBudget()).isEqualTo(3);
        assertThat(plan.termFrequencyReducers()).isEqualTo(2);
        assertThat(plan.documentFrequencyReducers()).isEqualTo(1);
        assertThat(plan.tfIdfReducers()).isEqualTo(3);
        assertThat(plan.documentKeywordsReducers()).isEqualTo(1);
        assertThat(plan.invertedIndexReducers()).isEqualTo(2);
        assertThat(plan.parallelFirstWave()).isTrue();
        assertThat(plan.parallelFinalWave()).isTrue();
    }

    @Test
    void clusterExecutionPlanFallsBackToSerialExecutionWhenOnlyOneReducerIsAvailable() {
        HadoopProcessingService.ClusterExecutionPlan plan = HadoopProcessingService.clusterExecutionPlan(1);

        assertThat(plan.reducerBudget()).isEqualTo(1);
        assertThat(plan.termFrequencyReducers()).isEqualTo(1);
        assertThat(plan.documentFrequencyReducers()).isEqualTo(1);
        assertThat(plan.tfIdfReducers()).isEqualTo(1);
        assertThat(plan.documentKeywordsReducers()).isEqualTo(1);
        assertThat(plan.invertedIndexReducers()).isEqualTo(1);
        assertThat(plan.parallelFirstWave()).isFalse();
        assertThat(plan.parallelFinalWave()).isFalse();
    }
}
