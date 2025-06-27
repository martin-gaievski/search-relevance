/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.executors;

import static org.opensearch.searchrelevance.executors.SearchRelevanceExecutor.SEARCH_RELEVANCE_EXEC_THREAD_POOL_NAME;
import static org.opensearch.searchrelevance.metrics.EvaluationMetrics.calculateEvaluationMetrics;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.search.SearchHit;
import org.opensearch.searchrelevance.dao.EvaluationResultDao;
import org.opensearch.searchrelevance.dao.ExperimentVariantDao;
import org.opensearch.searchrelevance.experiment.QuerySourceUtil;
import org.opensearch.searchrelevance.model.AsyncStatus;
import org.opensearch.searchrelevance.model.EvaluationResult;
import org.opensearch.searchrelevance.model.ExperimentBatchStatus;
import org.opensearch.searchrelevance.model.ExperimentVariant;
import org.opensearch.searchrelevance.model.builder.SearchRequestBuilder;
import org.opensearch.searchrelevance.utils.TimeUtils;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import com.google.common.annotations.VisibleForTesting;

import lombok.extern.log4j.Log4j2;

/**
 * Task manager for efficiently handling hybrid optimizer search tasks with concurrency control
 */
@Log4j2
public class HybridSearchTaskManager {
    public static final int TASK_RETRY_DELAY_MILLISECONDS = 1000;
    // Concurrency control settings - dynamic based on processor count
    public static final int ALLOCATED_PROCESSORS = OpenSearchExecutors.allocatedProcessors(Settings.EMPTY);

    private static final int DEFAULT_MIN_CONCURRENT_THREADS = 24;
    private static final int PROCESSOR_NUMBER_DIVISOR = 2;
    protected static final String THREAD_POOL_EXECUTOR_NAME = ThreadPool.Names.GENERIC;

    // Dynamic concurrency limit based on available processors
    private final int maxConcurrentTasks;
    private final Map<String, TaskContext> experimentTaskContexts = new HashMap<>();

    // Concurrency control - limit active search tasks to prevent resource exhaustion
    private final Semaphore concurrencyControl;

    // Services
    private final Client client;
    private final EvaluationResultDao evaluationResultDao;
    private final ExperimentVariantDao experimentVariantDao;
    private final ThreadPool threadPool;

    @Inject
    public HybridSearchTaskManager(
        Client client,
        EvaluationResultDao evaluationResultDao,
        ExperimentVariantDao experimentVariantDao,
        ThreadPool threadPool
    ) {
        this.client = client;
        this.evaluationResultDao = evaluationResultDao;
        this.experimentVariantDao = experimentVariantDao;
        this.threadPool = threadPool;

        this.maxConcurrentTasks = Math.max(2, Math.min(DEFAULT_MIN_CONCURRENT_THREADS, ALLOCATED_PROCESSORS / PROCESSOR_NUMBER_DIVISOR));

        // Initialize concurrency control with dynamic limit to prevent resource exhaustion
        this.concurrencyControl = new Semaphore(maxConcurrentTasks, true);

        log.info(
            "HybridSearchTaskManager initialized with max {} concurrent tasks (processors: {}) and dedicated SearchRelevance thread pool",
            maxConcurrentTasks,
            ALLOCATED_PROCESSORS
        );
    }

    /**
     * Schedule hybrid search tasks for execution with concurrency control
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
        AtomicBoolean hasFailure
    ) {
        // Create a context to track tasks for this experiment
        TaskContext taskContext = new TaskContext(
            experimentId,
            searchConfigId,
            queryText,
            experimentVariants.size(),
            configToExperimentVariants,
            finalListener,
            hasFailure
        );

        synchronized (experimentTaskContexts) {
            experimentTaskContexts.put(experimentId, taskContext);
        }

        synchronized (configToExperimentVariants) {
            if (!configToExperimentVariants.containsKey(searchConfigId)) {
                configToExperimentVariants.put(searchConfigId, new HashMap<String, Object>());
            }
        }

        log.info("Scheduling {} hybrid search tasks for experiment {} with concurrency control", experimentVariants.size(), experimentId);

        // Schedule each experiment variant as a separate task with concurrency control
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
     * Schedule a single variant task for execution with proper backpressure control
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
        if (taskContext.hasFailure.get()) {
            return;
        }

        // try to acquire semaphore permit BEFORE submitting to thread pool
        // this prevents queue overflow by controlling task submission rate
        if (concurrencyControl.tryAcquire()) {
            try {
                // Submit to thread pool only after acquiring permit
                threadPool.executor(SEARCH_RELEVANCE_EXEC_THREAD_POOL_NAME).execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        // Always release permit on failure
                        concurrencyControl.release();

                        // Check if it's a rejected execution (queue full)
                        if (e.getCause() instanceof java.util.concurrent.RejectedExecutionException) {
                            log.warn("Thread pool queue full, retrying task for variant: {}", experimentVariant.getId());
                            scheduleWithBackpressure(
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
                        } else {
                            handleTaskFailure(experimentVariant, e, taskContext);
                        }
                    }

                    @Override
                    protected void doRun() {
                        try {
                            executeVariantTaskWithPermit(
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
                        } catch (Exception exception) {
                            // Always release permit on exception
                            concurrencyControl.release();
                            throw exception;
                        }
                    }
                });
            } catch (RejectedExecutionException rejectedExecutionException) {
                // Thread pool queue is full, release permit and retry with backpressure
                concurrencyControl.release();
                log.warn("Thread pool queue full, scheduling with backpressure for variant: {}", experimentVariant.getId());
                scheduleWithBackpressure(
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
        } else {
            // No permit available - schedule with backpressure
            scheduleWithBackpressure(
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
     * Execute the variant task with acquired semaphore permit
     */
    private void executeVariantTaskWithPermit(
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
        if (taskContext.hasFailure.get()) {
            concurrencyControl.release();
            return;
        }

        final String evaluationId = UUID.randomUUID().toString();
        Map<String, Object> temporarySearchPipeline = QuerySourceUtil.createDefinitionOfTemporarySearchPipeline(experimentVariant);

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

        client.search(searchRequest, ActionListener.wrap(response -> {
            try {
                processSearchResponse(
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
                );
            } finally {
                // Always release permit after processing
                concurrencyControl.release();
            }
        }, exception -> {
            try {
                handleSearchFailure(exception, experimentVariant, experimentId, evaluationId, taskContext);
            } finally {
                // Always release permit after processing
                concurrencyControl.release();
            }
        }));
    }

    /**
     * Schedule task with backpressure when concurrency limit is reached
     */
    private void scheduleWithBackpressure(
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
        log.debug("Concurrency limit reached. Scheduling task with backpressure for variant: {}", experimentVariant.getId());

        // Schedule retry after delay
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
        }, new TimeValue(TASK_RETRY_DELAY_MILLISECONDS, TimeUnit.MILLISECONDS), THREAD_POOL_EXECUTOR_NAME);
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

                // Always persist variant even with no hits
                ExperimentVariant noHitsVariant = new ExperimentVariant(
                    experimentVariant.getId(),
                    TimeUtils.getTimestamp(),
                    experimentVariant.getType(),
                    AsyncStatus.COMPLETED,
                    experimentId,
                    experimentVariant.getParameters(),
                    Map.of("evaluationResultId", evaluationId, "details", "no search hits found")
                );

                experimentVariantDao.putExperimentVariantEfficient(noHitsVariant, ActionListener.wrap(success -> {
                    log.debug("Persisted no-hits variant: {}", experimentVariant.getId());
                    taskContext.completeVariantFailure();
                }, error -> handleTaskFailure(experimentVariant, error, taskContext)));

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

            evaluationResultDao.putEvaluationResultEfficient(
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
     * Create experiment variant with final status
     */
    private void updateExperimentVariant(
        ExperimentVariant experimentVariant,
        String experimentId,
        String searchConfigId,
        String evaluationId,
        TaskContext taskContext
    ) {
        // Create variant directly with COMPLETED status
        ExperimentVariant completedVariant = new ExperimentVariant(
            experimentVariant.getId(),
            TimeUtils.getTimestamp(),
            experimentVariant.getType(),
            AsyncStatus.COMPLETED,
            experimentId,
            experimentVariant.getParameters(),
            Map.of("evaluationResultId", evaluationId)
        );

        taskContext.scheduleVariantWrite(completedVariant, evaluationId, true);

        log.debug("Scheduled write for completed experiment variant: {}", experimentVariant.getId());
        taskContext.completeVariantSuccess();
    }

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

        // Single write with ERROR status, consistent with success path
        experimentVariantDao.putExperimentVariantEfficient(experimentVariantResult, ActionListener.wrap(success -> {
            // Just log the error but continue with other variants
            log.error("Error executing variant {}: {}", experimentVariant.getId(), e.getMessage());
            taskContext.completeVariantFailure();
        }, error -> {
            // Handle write failure gracefully, don't fail entire experiment
            log.error("Failed to persist error status for variant {}: {}", experimentVariant.getId(), error.getMessage());
            taskContext.completeVariantFailure();
        }));
    }

    private void handleTaskFailure(ExperimentVariant experimentVariant, Exception e, TaskContext taskContext) {
        // Check if this is a critical system failure vs individual variant failure
        if (isCriticalSystemFailure(e)) {
            if (taskContext.hasFailure.compareAndSet(false, true)) {
                log.error(
                    "Critical system failure processing hybrid search task for variant {}: {}",
                    experimentVariant.getId(),
                    e.getMessage()
                );
                taskContext.finalListener.onFailure(e);
            }
        } else {
            // Treat as individual variant failure - don't stop entire experiment
            log.error("Variant failure for {}: {}", experimentVariant.getId(), e.getMessage());
            taskContext.completeVariantFailure();
        }
    }

    /**
     * Determine if a throwable represents a critical system failure that should stop the entire experiment
     * vs an individual variant failure that should be recorded but not stop processing
     */
    private boolean isCriticalSystemFailure(Throwable throwable) {
        // Check the current throwable and walk up the cause chain
        Throwable current = throwable;
        while (current != null) {
            // Critical system errors that should stop the experiment
            if (current instanceof OutOfMemoryError || current instanceof StackOverflowError) {
                return true;
            }
            // Check for OpenSearch-specific critical exceptions
            if (current instanceof CircuitBreakingException || current instanceof ClusterBlockException) {
                return true;
            }
            // Move to the cause
            current = current.getCause();
        }

        // All other failures are treated as individual variant failures
        return false;
    }

    /**
     * Get current concurrency metrics for monitoring
     */
    @VisibleForTesting
    protected Map<String, Object> getConcurrencyMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("active_experiments", experimentTaskContexts.size());
        metrics.put("max_concurrent_tasks", maxConcurrentTasks);
        metrics.put("available_permits", concurrencyControl.availablePermits());
        metrics.put("queued_threads", concurrencyControl.getQueueLength());
        metrics.put("thread_pool", SEARCH_RELEVANCE_EXEC_THREAD_POOL_NAME);
        return metrics;
    }

    /**
     * Context for tracking tasks for a specific experiment
     */
    private class TaskContext {
        private final String experimentId;
        private final String searchConfigId;
        private final String queryText;
        private final AtomicInteger remainingVariants;
        private final AtomicInteger successfulVariants;
        private final AtomicInteger failedVariants;
        private final int totalVariants;
        private final Map<String, Object> configToExperimentVariants;
        private final ActionListener<Map<String, Object>> finalListener;
        private final AtomicBoolean hasFailure;

        public TaskContext(
            String experimentId,
            String searchConfigId,
            String queryText,
            int totalVariants,
            Map<String, Object> configToExperimentVariants,
            ActionListener<Map<String, Object>> finalListener,
            AtomicBoolean hasFailure
        ) {
            this.experimentId = experimentId;
            this.searchConfigId = searchConfigId;
            this.queryText = queryText;
            this.totalVariants = totalVariants;
            this.remainingVariants = new AtomicInteger(totalVariants);
            this.successfulVariants = new AtomicInteger(0);
            this.failedVariants = new AtomicInteger(0);
            this.configToExperimentVariants = configToExperimentVariants;
            this.finalListener = finalListener;
            this.hasFailure = hasFailure;

            log.info("TaskContext initialized for experiment {} with {} variants", experimentId, totalVariants);
        }

        /**
         * Write a variant individually using efficient refresh policy
         */
        public void scheduleVariantWrite(ExperimentVariant variant, String evaluationId, boolean isSuccess) {
            experimentVariantDao.putExperimentVariantEfficient(variant, ActionListener.wrap(response -> {
                log.debug("write successful for variant: {}", variant.getId());
                if (isSuccess) {
                    synchronized (configToExperimentVariants) {
                        Map<String, Object> map = (Map<String, Object>) configToExperimentVariants.get(searchConfigId);
                        map.put(variant.getId(), evaluationId);
                    }
                }
            }, error -> { log.error("write failed for variant {}: {}", variant.getId(), error.getMessage()); }));
        }

        /**
         * Mark a variant as successfully completed
         */
        public void completeVariantSuccess() {
            successfulVariants.incrementAndGet();
            completeVariant();
        }

        /**
         * Mark a variant as failed
         */
        public void completeVariantFailure() {
            failedVariants.incrementAndGet();
            completeVariant();
        }

        /**
         * Mark a variant as complete and check if all variants are done
         */
        private void completeVariant() {
            if (remainingVariants.decrementAndGet() == 0) {
                finishExperiment();
            }
        }

        /**
         * Finish the experiment and send final response
         */
        private void finishExperiment() {
            // Create results even if all variants failed (non-fatal to experiment)
            Map<String, Object> transformedConfigToExperimentVariants = new HashMap<>();
            transformedConfigToExperimentVariants.put("searchConfigurationId", searchConfigId);

            List<Map<String, Object>> evaluationResults = formatEvaluationResults();
            transformedConfigToExperimentVariants.put("evaluationResults", evaluationResults);

            // Add failure summary
            Map<String, Object> summary = new HashMap<>();
            summary.put("totalVariants", totalVariants);
            summary.put("successfulVariants", successfulVariants.get());
            summary.put("failedVariants", failedVariants.get());
            transformedConfigToExperimentVariants.put("summary", summary);

            if (failedVariants.get() == totalVariants) {
                log.error(
                    "All {} variants failed for search config {} in experiment {} with query '{}' - continuing experiment",
                    totalVariants,
                    searchConfigId,
                    experimentId,
                    queryText
                );
                transformedConfigToExperimentVariants.put("status", ExperimentBatchStatus.ALL_FAILED);
            } else if (failedVariants.get() > 0) {
                log.warn(
                    "Partial failure for search config {} in experiment {} with query '{}': {}/{} variants succeeded",
                    searchConfigId,
                    experimentId,
                    queryText,
                    successfulVariants.get(),
                    totalVariants
                );
                transformedConfigToExperimentVariants.put("status", ExperimentBatchStatus.PARTIAL_SUCCESS);
            } else {
                transformedConfigToExperimentVariants.put("status", ExperimentBatchStatus.SUCCESS);
            }

            // continue, don't fail the entire experiment for one search config
            finalListener.onResponse(transformedConfigToExperimentVariants);

            synchronized (experimentTaskContexts) {
                experimentTaskContexts.remove(experimentId);
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
