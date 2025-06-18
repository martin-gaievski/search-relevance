/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.metrics;

import static org.opensearch.searchrelevance.metrics.EvaluationMetrics.calculateEvaluationMetrics;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.StepListener;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHit;
import org.opensearch.searchrelevance.dao.EvaluationResultDao;
import org.opensearch.searchrelevance.dao.ExperimentVariantDao;
import org.opensearch.searchrelevance.model.AsyncStatus;
import org.opensearch.searchrelevance.model.EvaluationResult;
import org.opensearch.searchrelevance.model.ExperimentVariant;
import org.opensearch.searchrelevance.model.builder.SearchRequestBuilder;
import org.opensearch.searchrelevance.utils.TimeUtils;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

/**
 * Task manager for efficiently handling hybrid optimizer search tasks
 */
public class HybridSearchTaskManager {
    private static final Logger log = LogManager.getLogger(HybridSearchTaskManager.class);

    // Task completion tracking
    private final Map<String, TaskContext> experimentTaskContexts = new HashMap<>();

    // Services
    private final Client client;
    private final ClusterService clusterService;
    private final EvaluationResultDao evaluationResultDao;
    private final ExperimentVariantDao experimentVariantDao;
    private final ThreadPool threadPool;

    @Inject
    public HybridSearchTaskManager(
        Client client,
        ClusterService clusterService,
        EvaluationResultDao evaluationResultDao,
        ExperimentVariantDao experimentVariantDao,
        ThreadPool threadPool
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.evaluationResultDao = evaluationResultDao;
        this.experimentVariantDao = experimentVariantDao;
        this.threadPool = threadPool;
    }

    /**
     * Schedule hybrid search tasks for execution through the managed thread pool
     *
     * @param experimentId The experiment identifier
     * @param searchConfigId The search configuration identifier
     * @param index The index to search
     * @param query The search query
     * @param queryText The query text
     * @param size The search size
     * @param experimentVariants The experiment variants
     * @param judgmentIds The judgment IDs
     * @param docIdToScores Map of document IDs to relevance scores
     * @param configToExperimentVariants Map to collect results
     * @param finalListener Listener to notify when all tasks are complete
     * @param hasFailure Flag to track failures
     * @param pendingConfigurations Counter of pending configurations
     */
    public void scheduleTasks(
        String experimentId,
        String searchConfigId,
        String index,
        String query,
        String queryText,
        int size,
        List<ExperimentVariant> experimentVariants,
        List<String> judgmentIds,
        Map<String, String> docIdToScores,
        Map<String, Object> configToExperimentVariants,
        ActionListener<Map<String, Object>> finalListener,
        AtomicBoolean hasFailure,
        AtomicInteger pendingConfigurations
    ) {
        // Create a context to track tasks for this experiment
        TaskContext taskContext = new TaskContext(
            experimentId,
            searchConfigId,
            experimentVariants.size(),
            configToExperimentVariants,
            finalListener,
            hasFailure,
            pendingConfigurations
        );

        synchronized (experimentTaskContexts) {
            experimentTaskContexts.put(experimentId, taskContext);
        }

        synchronized (configToExperimentVariants) {
            if (!configToExperimentVariants.containsKey(searchConfigId)) {
                configToExperimentVariants.put(searchConfigId, new HashMap<String, Object>());
            }
        }

        // Schedule each experiment variant as a separate task
        for (ExperimentVariant experimentVariant : experimentVariants) {
            scheduleVariantTask(
                experimentId,
                searchConfigId,
                index,
                query,
                queryText,
                size,
                experimentVariant,
                judgmentIds,
                docIdToScores,
                taskContext
            );
        }
    }

    /**
     * Schedule a single variant task for execution
     */
    private void scheduleVariantTask(
        String experimentId,
        String searchConfigId,
        String index,
        String query,
        String queryText,
        int size,
        ExperimentVariant experimentVariant,
        List<String> judgmentIds,
        Map<String, String> docIdToScores,
        TaskContext taskContext
    ) {
        // Capture the current thread context
        ThreadContext.StoredContext storedContext = threadPool.getThreadContext().newStoredContext(true);

        try {
            // Use OpenSearch's thread pool instead of a custom executor
            threadPool.executor(ThreadPool.Names.SEARCH).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    handleTaskFailure(experimentVariant, e, taskContext);
                }

                @Override
                protected void doRun() throws Exception {
                    try {
                        storedContext.restore();

                        if (taskContext.hasFailure.get()) return;

                        executeVariantTask(
                            experimentId,
                            searchConfigId,
                            index,
                            query,
                            queryText,
                            size,
                            experimentVariant,
                            judgmentIds,
                            docIdToScores,
                            taskContext
                        );
                    } finally {
                        storedContext.close();
                    }
                }
            });
        } catch (Exception e) {
            // Handle task rejection by retrying after delay using OpenSearch's scheduled executor
            log.warn("Task rejected. Will retry after delay: {}", e.getMessage());
            retryTaskAfterDelay(
                experimentId,
                searchConfigId,
                index,
                query,
                queryText,
                size,
                experimentVariant,
                judgmentIds,
                docIdToScores,
                taskContext
            );
        }
    }

    /**
     * Retry a task after a delay
     */
    private void retryTaskAfterDelay(
        String experimentId,
        String searchConfigId,
        String index,
        String query,
        String queryText,
        int size,
        ExperimentVariant experimentVariant,
        List<String> judgmentIds,
        Map<String, String> docIdToScores,
        TaskContext taskContext
    ) {
        // Use OpenSearch's thread pool for scheduling delayed tasks
        threadPool.schedule(() -> {
            scheduleVariantTask(
                experimentId,
                searchConfigId,
                index,
                query,
                queryText,
                size,
                experimentVariant,
                judgmentIds,
                docIdToScores,
                taskContext
            );
        }, new TimeValue(1, TimeUnit.SECONDS), ThreadPool.Names.GENERIC);
    }

    /**
     * Execute the variant task
     */
    private void executeVariantTask(
        String experimentId,
        String searchConfigId,
        String index,
        String query,
        String queryText,
        int size,
        ExperimentVariant experimentVariant,
        List<String> judgmentIds,
        Map<String, String> docIdToScores,
        TaskContext taskContext
    ) {
        if (taskContext.hasFailure.get()) return;

        final String evaluationId = UUID.randomUUID().toString();
        Map<String, Object> temporarySearchPipeline = org.opensearch.searchrelevance.experiment.QuerySourceUtil
            .createDefinitionOfTemporarySearchPipeline(experimentVariant);

        SearchRequest searchRequest = SearchRequestBuilder.buildRequestForHybridSearch(
            index,
            query,
            temporarySearchPipeline,
            queryText,
            size
        );

        log.debug(
            "Processing hybrid search sub-experiment: {} configuration: {} index: {}, query: {}, evaluationId: {}",
            experimentVariant.getId(),
            searchConfigId,
            index,
            query,
            evaluationId
        );

        client.search(
            searchRequest,
            ActionListener.wrap(
                response -> processSearchResponse(
                    response,
                    experimentVariant,
                    experimentId,
                    searchConfigId,
                    queryText,
                    size,
                    judgmentIds,
                    docIdToScores,
                    evaluationId,
                    taskContext
                ),
                exception -> handleSearchFailure(exception, experimentVariant, experimentId, evaluationId, taskContext)
            )
        );
    }

    /**
     * Process search response
     */
    private void processSearchResponse(
        SearchResponse response,
        ExperimentVariant experimentVariant,
        String experimentId,
        String searchConfigId,
        String queryText,
        int size,
        List<String> judgmentIds,
        Map<String, String> docIdToScores,
        String evaluationId,
        TaskContext taskContext
    ) {
        if (taskContext.hasFailure.get()) return;

        try {
            if (response.getHits().getTotalHits().value() == 0) {
                log.warn("No hits found for search config: {} and variant: {}", searchConfigId, experimentVariant.getId());
                taskContext.completeVariant();
                return;
            }

            SearchHit[] hits = response.getHits().getHits();
            List<String> docIds = Arrays.stream(hits).map(SearchHit::getId).collect(Collectors.toList());

            List<Map<String, Object>> metrics = calculateEvaluationMetrics(docIds, docIdToScores, size);
            EvaluationResult evaluationResult = new EvaluationResult(
                evaluationId,
                TimeUtils.getTimestamp(),
                searchConfigId,
                queryText,
                judgmentIds,
                docIds,
                metrics
            );

            evaluationResultDao.putEvaluationResult(
                evaluationResult,
                ActionListener.wrap(
                    success -> updateExperimentVariant(experimentVariant, experimentId, searchConfigId, evaluationId, taskContext),
                    error -> handleTaskFailure(experimentVariant, error, taskContext)
                )
            );
        } catch (Exception e) {
            handleTaskFailure(experimentVariant, e, taskContext);
        }
    }

    /**
     * Update experiment variant with evaluation result
     */
    private void updateExperimentVariant(
        ExperimentVariant experimentVariant,
        String experimentId,
        String searchConfigId,
        String evaluationId,
        TaskContext taskContext
    ) {
        ExperimentVariant experimentVariantResult = new ExperimentVariant(
            experimentVariant.getId(),
            TimeUtils.getTimestamp(),
            experimentVariant.getType(),
            AsyncStatus.COMPLETED,
            experimentId,
            experimentVariant.getParameters(),
            Map.of("evaluationResultId", evaluationId)
        );

        StepListener<IndexResponse> voidStepListener = new StepListener<>();
        experimentVariantDao.updateExperimentVariant(experimentVariantResult, voidStepListener);

        voidStepListener.whenComplete(indexResponse -> {
            synchronized (taskContext.configToExperimentVariants) {
                Map<String, Object> map = (Map<String, Object>) taskContext.configToExperimentVariants.get(searchConfigId);
                map.put(experimentVariant.getId(), evaluationId);
            }
            taskContext.completeVariant();
        }, error -> { handleTaskFailure(experimentVariant, error, taskContext); });
    }

    /**
     * Handle search failure
     */
    private void handleSearchFailure(
        Exception e,
        ExperimentVariant experimentVariant,
        String experimentId,
        String evaluationId,
        TaskContext taskContext
    ) {
        ExperimentVariant experimentVariantResult = new ExperimentVariant(
            experimentVariant.getId(),
            TimeUtils.getTimestamp(),
            experimentVariant.getType(),
            AsyncStatus.ERROR,
            experimentId,
            experimentVariant.getParameters(),
            Map.of("evaluationResultId", evaluationId, "error", e.getMessage())
        );

        experimentVariantDao.updateExperimentVariant(experimentVariantResult, ActionListener.wrap(success -> {
            // Just log the error but continue with other variants
            log.error("Error executing variant {}: {}", experimentVariant.getId(), e.getMessage());
            taskContext.completeVariant();
        }, error -> { handleTaskFailure(experimentVariant, error, taskContext); }));
    }

    /**
     * Handle task failure
     */
    private void handleTaskFailure(ExperimentVariant experimentVariant, Exception e, TaskContext taskContext) {
        if (taskContext.hasFailure.compareAndSet(false, true)) {
            log.error("Fatal error processing hybrid search task for variant {}: {}", experimentVariant.getId(), e.getMessage());
            taskContext.finalListener.onFailure(e);
        }
    }

    /**
     * Context for tracking tasks for a specific experiment
     */
    private class TaskContext {
        private final String experimentId;
        private final String searchConfigId;
        private final AtomicInteger remainingVariants;
        private final Map<String, Object> configToExperimentVariants;
        private final ActionListener<Map<String, Object>> finalListener;
        private final AtomicBoolean hasFailure;
        private final AtomicInteger pendingConfigurations;

        public TaskContext(
            String experimentId,
            String searchConfigId,
            int totalVariants,
            Map<String, Object> configToExperimentVariants,
            ActionListener<Map<String, Object>> finalListener,
            AtomicBoolean hasFailure,
            AtomicInteger pendingConfigurations
        ) {
            this.experimentId = experimentId;
            this.searchConfigId = searchConfigId;
            this.remainingVariants = new AtomicInteger(totalVariants);
            this.configToExperimentVariants = configToExperimentVariants;
            this.finalListener = finalListener;
            this.hasFailure = hasFailure;
            this.pendingConfigurations = pendingConfigurations;
        }

        /**
         * Mark a variant as complete and check if all variants are done
         */
        public void completeVariant() {
            if (remainingVariants.decrementAndGet() == 0) {
                // All variants for this search configuration are complete
                if (pendingConfigurations.decrementAndGet() == 0) {
                    // All search configurations are complete, format results and notify listener
                    Map<String, Object> transformedConfigToExperimentVariants = new HashMap<>();
                    transformedConfigToExperimentVariants.put("searchConfigurationId", searchConfigId);

                    List<Map<String, Object>> evaluationResults = formatEvaluationResults();
                    transformedConfigToExperimentVariants.put("evaluationResults", evaluationResults);

                    finalListener.onResponse(transformedConfigToExperimentVariants);

                    // Clean up this task context
                    synchronized (experimentTaskContexts) {
                        experimentTaskContexts.remove(experimentId);
                    }
                }
            }
        }

        /**
         * Format evaluation results for the final response
         */
        private List<Map<String, Object>> formatEvaluationResults() {
            List<Map<String, Object>> results = new java.util.ArrayList<>();
            Map<String, Object> configMap = (Map<String, Object>) configToExperimentVariants.get(searchConfigId);

            configMap.forEach((variantId, evalId) -> {
                Map<String, Object> result = new HashMap<>();
                result.put("evaluationId", evalId);
                result.put("experimentVariantId", variantId);
                results.add(result);
            });

            return results;
        }
    }
}
