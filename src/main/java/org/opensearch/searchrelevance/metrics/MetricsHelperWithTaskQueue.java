/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.metrics;

import java.util.List;
import java.util.Map;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.searchrelevance.dao.EvaluationResultDao;
import org.opensearch.searchrelevance.dao.ExperimentVariantDao;
import org.opensearch.searchrelevance.dao.JudgmentDao;
import org.opensearch.searchrelevance.model.ExperimentVariant;
import org.opensearch.transport.client.Client;

import lombok.extern.log4j.Log4j2;
import reactor.util.annotation.NonNull;

/**
 * Enhanced metrics helper with task queue for hybrid optimizer experiments
 */
@Log4j2
public class MetricsHelperWithTaskQueue extends MetricsHelper {

    private final HybridSearchTaskManager hybridSearchTaskManager;

    @Inject
    public MetricsHelperWithTaskQueue(
        @NonNull ClusterService clusterService,
        @NonNull Client client,
        @NonNull JudgmentDao judgmentDao,
        @NonNull EvaluationResultDao evaluationResultDao,
        @NonNull ExperimentVariantDao experimentVariantDao,
        @NonNull HybridSearchTaskManager hybridSearchTaskManager
    ) {
        super(clusterService, client, judgmentDao, evaluationResultDao, experimentVariantDao);
        this.hybridSearchTaskManager = hybridSearchTaskManager;
    }

    /**
     * Override the processEvaluationMetrics method to use our optimized implementation
     */
    @Override
    public void processEvaluationMetrics(
        String queryText,
        Map<String, List<String>> indexAndQueries,
        int size,
        List<String> judgmentIds,
        ActionListener<Map<String, Object>> listener,
        List<ExperimentVariant> experimentVariants
    ) {
        // For non-hybrid-optimizer experiments or empty variants, use parent implementation
        if (experimentVariants == null || experimentVariants.isEmpty()) {
            super.processEvaluationMetrics(queryText, indexAndQueries, size, judgmentIds, listener, experimentVariants);
            return;
        }

        // For hybrid optimizer, use specialized implementation with task manager
        if (indexAndQueries.isEmpty() || judgmentIds.isEmpty()) {
            listener.onFailure(new IllegalArgumentException("Missing required parameters"));
            return;
        }

        try {
            // Process judgments to get document scores
            super.processEvaluationMetrics(queryText, indexAndQueries, size, judgmentIds, listener, experimentVariants);
        } catch (Exception e) {
            log.error("Unexpected error in evaluateQueryTextAsync for hybrid optimizer: {}", e.getMessage());
            listener.onFailure(e);
        }
    }
}
