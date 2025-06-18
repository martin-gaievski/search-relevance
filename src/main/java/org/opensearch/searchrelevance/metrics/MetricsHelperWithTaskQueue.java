/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.metrics;

import static org.opensearch.searchrelevance.common.MetricsConstants.POINTWISE_FIELD_NAME_EVALUATION_ID;
import static org.opensearch.searchrelevance.common.MetricsConstants.POINTWISE_FIELD_NAME_EVALUATION_RESULTS;
import static org.opensearch.searchrelevance.common.MetricsConstants.POINTWISE_FIELD_NAME_SEARCH_CONFIGURATION_ID;
import static org.opensearch.searchrelevance.metrics.EvaluationMetrics.calculateEvaluationMetrics;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHit;
import org.opensearch.searchrelevance.dao.EvaluationResultDao;
import org.opensearch.searchrelevance.dao.ExperimentVariantDao;
import org.opensearch.searchrelevance.dao.JudgmentDao;
import org.opensearch.searchrelevance.model.EvaluationResult;
import org.opensearch.searchrelevance.model.ExperimentVariant;
import org.opensearch.searchrelevance.model.builder.SearchRequestBuilder;
import org.opensearch.searchrelevance.utils.TimeUtils;
import org.opensearch.transport.client.Client;

import lombok.extern.log4j.Log4j2;
import reactor.util.annotation.NonNull;

/**
 * Enhanced metrics helper with task queue for hybrid optimizer experiments
 */
@Log4j2
public class MetricsHelperWithTaskQueue extends MetricsHelper {

    private final HybridSearchTaskManager hybridSearchTaskManager;
    private final Client client;
    private final EvaluationResultDao evaluationResultDao;
    private final ExperimentVariantDao experimentVariantDao;

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
        this.client = client;
        this.evaluationResultDao = evaluationResultDao;
        this.experimentVariantDao = experimentVariantDao;
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

    /**
     * Process search configuration with hybrid experiment options using the task manager
     * for better resource management and performance
     */
    public void processHybridExperimentOptions(
        String queryText,
        int size,
        List<String> judgmentIds,
        Map<String, String> docIdToScores,
        Map<String, Object> configToExperimentVariants,
        ActionListener<Map<String, Object>> listener,
        String searchConfigurationId,
        String index,
        String query,
        AtomicBoolean hasFailure,
        AtomicInteger pendingConfigurations,
        List<ExperimentVariant> experimentVariants
    ) {
        if (Objects.isNull(experimentVariants) || experimentVariants.isEmpty()) {
            throw new IllegalArgumentException("Experiment variant for hybrid search cannot be empty");
        }

        log.debug(
            "Delegating hybrid search tasks to task manager: configuration: {}, index: {}, query: {}, variants: {}",
            searchConfigurationId,
            index,
            query,
            experimentVariants.size()
        );

        // Get experiment ID from first variant
        String experimentId = experimentVariants.get(0).getExperimentId();

        // Schedule tasks through the task manager
        hybridSearchTaskManager.scheduleTasks(
            experimentId,
            searchConfigurationId,
            index,
            query,
            queryText,
            size,
            experimentVariants,
            judgmentIds,
            docIdToScores,
            configToExperimentVariants,
            new ActionListener<Map<String, Object>>() {
                @Override
                public void onResponse(Map<String, Object> transformedResults) {
                    // All variants for this search configuration are complete
                    // Results have already been formatted by the task manager

                    // Check if we completed all configurations
                    if (pendingConfigurations.decrementAndGet() == 0 && !hasFailure.get()) {
                        Map<String, Object> finalResults = new HashMap<>();
                        finalResults.put(POINTWISE_FIELD_NAME_SEARCH_CONFIGURATION_ID, searchConfigurationId);
                        finalResults.put(POINTWISE_FIELD_NAME_EVALUATION_RESULTS, transformedResults.get("evaluationResults"));
                        listener.onResponse(finalResults);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (hasFailure.compareAndSet(false, true)) {
                        log.error("Failed processing hybrid search tasks: {}", e.getMessage(), e);
                        listener.onFailure(e);
                    }
                }
            },
            hasFailure,
            pendingConfigurations
        );
    }

    /**
     * Implementation of standard processing when not using task manager
     */
    public void processStandardExperimentOptions(
        String queryText,
        int size,
        List<String> judgmentIds,
        Map<String, String> docIdToScores,
        Map<String, Object> configToEvalIds,
        ActionListener<Map<String, Object>> listener,
        String searchConfigurationId,
        String index,
        String query,
        String searchPipeline,
        AtomicBoolean hasFailure,
        AtomicInteger pendingConfigurations
    ) {
        SearchRequest searchRequest = SearchRequestBuilder.buildSearchRequest(index, query, queryText, searchPipeline, size);
        final String evaluationId = UUID.randomUUID().toString();
        log.debug(
            "Standard processing - Configuration {}: index: {}, query: {}, searchPipeline: {}, evaluationId: {}",
            searchConfigurationId,
            index,
            query,
            searchPipeline,
            evaluationId
        );
        client.search(searchRequest, new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse response) {
                if (hasFailure.get()) return;

                try {
                    if (response.getHits().getTotalHits().value() == 0) {
                        log.warn("No hits found for search config: {}", searchConfigurationId);
                        if (pendingConfigurations.decrementAndGet() == 0) {
                            listener.onResponse(configToEvalIds);
                        }
                        return;
                    }

                    SearchHit[] hits = response.getHits().getHits();
                    List<String> docIds = Arrays.stream(hits).map(SearchHit::getId).collect(Collectors.toList());

                    List<Map<String, Object>> metrics = calculateEvaluationMetrics(docIds, docIdToScores, size);
                    EvaluationResult evaluationResult = new EvaluationResult(
                        evaluationId,
                        TimeUtils.getTimestamp(),
                        searchConfigurationId,
                        queryText,
                        judgmentIds,
                        docIds,
                        metrics
                    );

                    evaluationResultDao.putEvaluationResult(evaluationResult, ActionListener.wrap(success -> {
                        configToEvalIds.put(POINTWISE_FIELD_NAME_SEARCH_CONFIGURATION_ID, searchConfigurationId);
                        configToEvalIds.put(POINTWISE_FIELD_NAME_EVALUATION_ID, evaluationId);
                        if (pendingConfigurations.decrementAndGet() == 0) {
                            listener.onResponse(configToEvalIds);
                        }
                    }, error -> {
                        hasFailure.set(true);
                        listener.onFailure(error);
                    }));
                } catch (Exception e) {
                    hasFailure.set(true);
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                hasFailure.set(true);
                listener.onFailure(e);
            }
        });
    }
}
