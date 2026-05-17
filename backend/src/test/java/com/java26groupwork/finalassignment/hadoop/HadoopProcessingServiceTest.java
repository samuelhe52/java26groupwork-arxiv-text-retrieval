package com.java26groupwork.finalassignment.hadoop;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HadoopProcessingServiceTest {

    @Test
    void clusterExecutionPlanCapsReducerBudgetAtFourNodes() {
        HadoopProcessingService.ClusterExecutionPlan plan = HadoopProcessingService.clusterExecutionPlan(8);

        assertThat(plan.reducerBudget()).isEqualTo(4);
        assertThat(plan.termStatisticsReducers()).isEqualTo(4);
        assertThat(plan.scoredTermsReducers()).isEqualTo(4);
        assertThat(plan.documentKeywordsReducers()).isEqualTo(4);
    }

    @Test
    void clusterExecutionPlanKeepsMergedStagesAtTheAvailableBudget() {
        HadoopProcessingService.ClusterExecutionPlan plan = HadoopProcessingService.clusterExecutionPlan(3);

        assertThat(plan.reducerBudget()).isEqualTo(3);
        assertThat(plan.termStatisticsReducers()).isEqualTo(3);
        assertThat(plan.scoredTermsReducers()).isEqualTo(3);
        assertThat(plan.documentKeywordsReducers()).isEqualTo(3);
    }

    @Test
    void clusterExecutionPlanFallsBackToOneReducerWhenOnlyOneIsAvailable() {
        HadoopProcessingService.ClusterExecutionPlan plan = HadoopProcessingService.clusterExecutionPlan(1);

        assertThat(plan.reducerBudget()).isEqualTo(1);
        assertThat(plan.termStatisticsReducers()).isEqualTo(1);
        assertThat(plan.scoredTermsReducers()).isEqualTo(1);
        assertThat(plan.documentKeywordsReducers()).isEqualTo(1);
    }
}
