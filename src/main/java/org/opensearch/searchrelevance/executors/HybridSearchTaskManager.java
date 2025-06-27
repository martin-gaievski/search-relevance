/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.executors;

import static org.opensearch.searchrelevance.executors.SearchRelevanceExecutor.SEARCH_RELEVANCE_EXEC_THREAD_POOL_NAME;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.searchrelevance.dao.EvaluationResultDao;
import org.opensearch.searchrelevance.dao.ExperimentVariantDao;
import org.opensearch.searchrelevance.experiment.QuerySourceUtil;
import org.opensearch.searchrelevance.model.ExperimentVariant;
import org.opensearch.searchrelevance.model.builder.SearchRequestBuilder;
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
    public static final int ALLOCATED_PROCESSORS = OpenSearchExecutors.allocatedProcessors(Settings.EMPTY);

    private static final int DEFAULT_MIN_CONCURRENT_THREADS = 24;
    private static final int PROCESSOR_NUMBER_DIVISOR = 2;
    protected static final String THREAD_POOL_EXECUTOR_NAME = ThreadPool.Names.GENERIC;

    private final int maxConcurrentTasks;
    private final Map<String, ExperimentTaskContext> experimentTaskContexts = new HashMap<>();
    private final Semaphore concurrencyControl;

    // Services
    private final Client client;
    private final EvaluationResultDao evaluationResultDao;
    private final ExperimentVariantDao experimentVariantDao;
    private final ThreadPool threadPool;
    private final SearchResponseProcessor searchResponseProcessor;

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
        this.searchResponseProcessor = new SearchResponseProcessor(evaluationResultDao, experimentVariantDao);

        this.maxConcurrentTasks = Math.max(2, Math.min(DEFAULT_MIN_CONCURRENT_THREADS, ALLOCATED_PROCESSORS / PROCESSOR_NUMBER_DIVISOR));
        this.concurrencyControl = new Semaphore(maxConcurrentTasks, true);

        log.info(
            "HybridSearchTaskManager initialized with max {} concurrent tasks (processors: {}) and dedicated SearchRelevance thread pool",
            maxConcurrentTasks,
            ALLOCATED_PROCESSORS
        );
    }

    /**
     * Schedule hybrid search tasks for execution with concurrency control
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
        ExperimentTaskContext taskContext = ExperimentTaskContext.builder()
            .experimentId(experimentId)
            .searchConfigId(searchConfigId)
            .queryText(queryText)
            .totalVariants(experimentVariants.size())
            .configToExperimentVariants(configToExperimentVariants)
            .finalListener(finalListener)
            .hasFailure(hasFailure)
            .experimentVariantDao(experimentVariantDao)
            .build();

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
            VariantTaskParameters params = VariantTaskParameters.builder()
                .experimentId(experimentId)
                .searchConfigId(searchConfigId)
                .index(index)
                .query(query)
                .queryText(queryText)
                .size(size)
                .experimentVariant(experimentVariant)
                .judgmentIds(judgmentIds)
                .docIdToScores(docIdToScores)
                .taskContext(taskContext)
                .build();
            
            scheduleVariantTask(params);
        }
    }

    /**
     * Schedule a single variant task for execution with proper backpressure control
     */
    private void scheduleVariantTask(VariantTaskParameters params) {
        if (params.getTaskContext().getHasFailure().get()) {
            return;
        }

        // Try to acquire semaphore permit BEFORE submitting to thread pool
        if (concurrencyControl.tryAcquire()) {
            try {
                threadPool.executor(SEARCH_RELEVANCE_EXEC_THREAD_POOL_NAME).execute(new VariantTaskRunnable(params));
            } catch (RejectedExecutionException rejectedExecutionException) {
                // Thread pool queue is full, release permit and retry with backpressure
                concurrencyControl.release();
                log.warn("Thread pool queue full, scheduling with backpressure for variant: {}", params.getExperimentVariant().getId());
                scheduleWithBackpressure(params);
            }
        } else {
            // No permit available - schedule with backpressure
            scheduleWithBackpressure(params);
        }
    }

    /**
     * Schedule task with backpressure when concurrency limit is reached
     */
    private void scheduleWithBackpressure(VariantTaskParameters params) {
        log.debug("Concurrency limit reached. Scheduling task with backpressure for variant: {}", params.getExperimentVariant().getId());

        threadPool.schedule(
            () -> scheduleVariantTask(params),
            new TimeValue(TASK_RETRY_DELAY_MILLISECONDS, TimeUnit.MILLISECONDS),
            THREAD_POOL_EXECUTOR_NAME
        );
    }

    /**
     * Execute the variant task with acquired semaphore permit
     */
    private void executeVariantTaskWithPermit(VariantTaskParameters params) {
        if (params.getTaskContext().getHasFailure().get()) {
            concurrencyControl.release();
            return;
        }

        final String evaluationId = UUID.randomUUID().toString();
        Map<String, Object> temporarySearchPipeline = QuerySourceUtil.createDefinitionOfTemporarySearchPipeline(params.getExperimentVariant());

        SearchRequest searchRequest = SearchRequestBuilder.buildRequestForHybridSearch(
            params.getIndex(),
            params.getQuery(),
            temporarySearchPipeline,
            params.getQueryText(),
            params.getSize()
        );

        log.debug(
            "Processing hybrid search sub-experiment: {} configuration: {} index: {}, query: {}, evaluationId: {}",
            params.getExperimentVariant().getId(),
            params.getSearchConfigId(),
            params.getIndex(),
            params.getQuery(),
            evaluationId
        );

        client.search(searchRequest, ActionListener.wrap(response -> {
            try {
                searchResponseProcessor.processSearchResponse(
                    response,
                    params.getExperimentVariant(),
                    params.getExperimentId(),
                    params.getSearchConfigId(),
                    params.getQueryText(),
                    params.getSize(),
                    params.getJudgmentIds(),
                    params.getDocIdToScores(),
                    evaluationId,
                    params.getTaskContext()
                );
            } finally {
                concurrencyControl.release();
            }
        }, exception -> {
            try {
                handleSearchFailure(exception, params.getExperimentVariant(), params.getExperimentId(), evaluationId, params.getTaskContext());
            } finally {
                concurrencyControl.release();
            }
        }));
    }

    private void handleSearchFailure(
        Exception e,
        ExperimentVariant experimentVariant,
        String experimentId,
        String evaluationId,
        ExperimentTaskContext taskContext
    ) {
        // Check if this is a critical system failure vs individual variant failure
        if (isCriticalSystemFailure(e)) {
            if (taskContext.getHasFailure().compareAndSet(false, true)) {
                log.error(
                    "Critical system failure processing hybrid search task for variant {}: {}",
                    experimentVariant.getId(),
                    e.getMessage()
                );
                taskContext.getFinalListener().onFailure(e);
            }
        } else {
            // Treat as individual variant failure - don't stop entire experiment
            searchResponseProcessor.handleSearchFailure(e, experimentVariant, experimentId, evaluationId, taskContext);
        }
    }

    /**
     * Determine if a throwable represents a critical system failure that should stop the entire experiment
     * vs an individual variant failure that should be recorded but not stop processing
     */
    private boolean isCriticalSystemFailure(Throwable throwable) {
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
            current = current.getCause();
        }
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
     * Runnable for executing variant tasks
     */
    private class VariantTaskRunnable extends AbstractRunnable {
        private final VariantTaskParameters params;

        VariantTaskRunnable(VariantTaskParameters params) {
            this.params = params;
        }

        @Override
        public void onFailure(Exception e) {
            concurrencyControl.release();

            // Check if it's a rejected execution (queue full)
            if (e.getCause() instanceof RejectedExecutionException) {
                log.warn("Thread pool queue full, retrying task for variant: {}", params.getExperimentVariant().getId());
                scheduleWithBackpressure(params);
            } else {
                handleTaskFailure(params.getExperimentVariant(), e, params.getTaskContext());
            }
        }

        @Override
        protected void doRun() {
            try {
                executeVariantTaskWithPermit(params);
            } catch (Exception exception) {
                concurrencyControl.release();
                throw exception;
            }
        }
    }

    private void handleTaskFailure(ExperimentVariant experimentVariant, Exception e, ExperimentTaskContext taskContext) {
        // Check if this is a critical system failure vs individual variant failure
        if (isCriticalSystemFailure(e)) {
            if (taskContext.getHasFailure().compareAndSet(false, true)) {
                log.error(
                    "Critical system failure processing hybrid search task for variant {}: {}",
                    experimentVariant.getId(),
                    e.getMessage()
                );
                taskContext.getFinalListener().onFailure(e);
            }
        } else {
            // Treat as individual variant failure - don't stop entire experiment
            log.error("Variant failure for {}: {}", experimentVariant.getId(), e.getMessage());
            taskContext.completeVariantFailure();
        }
    }
}
