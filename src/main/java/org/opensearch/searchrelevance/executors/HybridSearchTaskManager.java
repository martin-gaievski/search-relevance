/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.executors;

import static org.opensearch.searchrelevance.executors.SearchRelevanceExecutor.SEARCH_RELEVANCE_EXEC_THREAD_POOL_NAME;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
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
 * Class for managing tasks related to hybrid search
 */
@Log4j2
public class HybridSearchTaskManager {
    public static final int TASK_RETRY_DELAY_MILLISECONDS = 1000;
    public static final int ALLOCATED_PROCESSORS = OpenSearchExecutors.allocatedProcessors(Settings.EMPTY);

    private static final int DEFAULT_MIN_CONCURRENT_THREADS = 24;
    private static final int PROCESSOR_NUMBER_DIVISOR = 2;
    protected static final String THREAD_POOL_EXECUTOR_NAME = ThreadPool.Names.GENERIC;

    private final int maxConcurrentTasks;
    private final ConcurrentHashMap<String, ExperimentTaskContext> experimentTaskContexts = new ConcurrentHashMap<>();
    private final Semaphore concurrencyControl;

    // Use LongAdder for better concurrent counting performance
    private final LongAdder activeTasks = new LongAdder();

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
            "HybridSearchTaskManagerOptimized initialized with max {} concurrent tasks (processors: {})",
            maxConcurrentTasks,
            ALLOCATED_PROCESSORS
        );
    }

    /**
     * Schedule hybrid search tasks using non-blocking mechanisms
     */
    public CompletableFuture<Map<String, Object>> scheduleTasksAsync(
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
        AtomicBoolean hasFailure
    ) {
        // Create a CompletableFuture to track the overall completion
        CompletableFuture<Map<String, Object>> resultFuture = new CompletableFuture<>();

        // Create optimized task context
        ExperimentTaskContext taskContext = new ExperimentTaskContext(
            experimentId,
            searchConfigId,
            queryText,
            experimentVariants.size(),
            new ConcurrentHashMap<>(configToExperimentVariants),
            resultFuture,
            hasFailure,
            experimentVariantDao
        );

        // Use putIfAbsent for atomic operation
        experimentTaskContexts.putIfAbsent(experimentId, taskContext);

        // Initialize config map using computeIfAbsent (non-blocking)
        taskContext.getConfigToExperimentVariants().computeIfAbsent(searchConfigId, k -> new ConcurrentHashMap<String, Object>());

        log.info(
            "Scheduling {} hybrid search tasks for experiment {} with non-blocking concurrency",
            experimentVariants.size(),
            experimentId
        );

        // Schedule tasks asynchronously
        List<CompletableFuture<Void>> variantFutures = experimentVariants.stream().map(variant -> {
            VariantTaskParameters params = VariantTaskParameters.builder()
                .experimentId(experimentId)
                .searchConfigId(searchConfigId)
                .index(index)
                .query(query)
                .queryText(queryText)
                .size(size)
                .experimentVariant(variant)
                .judgmentIds(judgmentIds)
                .docIdToScores(docIdToScores)
                .taskContext(taskContext)
                .build();

            return scheduleVariantTaskAsync(params);
        }).toList();

        // When all variants complete, clean up
        CompletableFuture.allOf(variantFutures.toArray(new CompletableFuture[0])).whenComplete((v, ex) -> {
            experimentTaskContexts.remove(experimentId);
            activeTasks.decrement();
        });

        return resultFuture;
    }

    /**
     * Schedule a single variant task asynchronously
     */
    private CompletableFuture<Void> scheduleVariantTaskAsync(VariantTaskParameters params) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (params.getTaskContext().getHasFailure().get()) {
            future.complete(null);
            return future;
        }

        // Try to acquire permit non-blocking
        if (concurrencyControl.tryAcquire()) {
            activeTasks.increment();
            submitTaskToThreadPool(params, future);
        } else {
            // Schedule with backpressure using CompletableFuture
            CompletableFuture.delayedExecutor(
                TASK_RETRY_DELAY_MILLISECONDS,
                TimeUnit.MILLISECONDS,
                threadPool.executor(THREAD_POOL_EXECUTOR_NAME)
            ).execute(() -> {
                scheduleVariantTaskAsync(params).whenComplete((v, ex) -> {
                    if (ex != null) {
                        future.completeExceptionally(ex);
                    } else {
                        future.complete(v);
                    }
                });
            });
        }

        return future;
    }

    private void submitTaskToThreadPool(VariantTaskParameters params, CompletableFuture<Void> future) {
        try {
            threadPool.executor(SEARCH_RELEVANCE_EXEC_THREAD_POOL_NAME).execute(new OptimizedVariantTaskRunnable(params, future));
        } catch (RejectedExecutionException e) {
            concurrencyControl.release();
            activeTasks.decrement();
            log.warn("Thread pool queue full, retrying for variant: {}", params.getExperimentVariant().getId());

            // Retry with backpressure
            CompletableFuture.delayedExecutor(
                TASK_RETRY_DELAY_MILLISECONDS,
                TimeUnit.MILLISECONDS,
                threadPool.executor(THREAD_POOL_EXECUTOR_NAME)
            ).execute(() -> scheduleVariantTaskAsync(params));
        }
    }

    /**
     * Execute variant task using CompletableFuture for better async handling
     */
    private void executeVariantTaskAsync(VariantTaskParameters params, CompletableFuture<Void> future) {
        if (params.getTaskContext().getHasFailure().get()) {
            concurrencyControl.release();
            activeTasks.decrement();
            future.complete(null);
            return;
        }

        final String evaluationId = UUID.randomUUID().toString();
        Map<String, Object> temporarySearchPipeline = QuerySourceUtil.createDefinitionOfTemporarySearchPipeline(
            params.getExperimentVariant()
        );

        SearchRequest searchRequest = SearchRequestBuilder.buildRequestForHybridSearch(
            params.getIndex(),
            params.getQuery(),
            temporarySearchPipeline,
            params.getQueryText(),
            params.getSize()
        );

        // Convert ActionListener to CompletableFuture
        CompletableFuture<Void> searchFuture = new CompletableFuture<>();

        client.search(searchRequest, new ActionListener<>() {
            @Override
            public void onResponse(org.opensearch.action.search.SearchResponse response) {
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
                    searchFuture.complete(null);
                } catch (Exception e) {
                    searchFuture.completeExceptionally(e);
                } finally {
                    concurrencyControl.release();
                    activeTasks.decrement();
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    handleSearchFailure(e, params.getExperimentVariant(), params.getExperimentId(), evaluationId, params.getTaskContext());
                    searchFuture.complete(null);
                } catch (Exception ex) {
                    searchFuture.completeExceptionally(ex);
                } finally {
                    concurrencyControl.release();
                    activeTasks.decrement();
                }
            }
        });

        // Chain the futures
        searchFuture.whenComplete((v, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
            } else {
                future.complete(null);
            }
        });
    }

    private void handleSearchFailure(
        Exception e,
        ExperimentVariant experimentVariant,
        String experimentId,
        String evaluationId,
        ExperimentTaskContext taskContext
    ) {
        if (isCriticalSystemFailure(e)) {
            if (taskContext.getHasFailure().compareAndSet(false, true)) {
                log.error("Critical system failure for variant {}: {}", experimentVariant.getId(), e.getMessage());
                taskContext.getResultFuture().completeExceptionally(e);
            }
        } else {
            searchResponseProcessor.handleSearchFailure(e, experimentVariant, experimentId, evaluationId, taskContext);
        }
    }

    private boolean isCriticalSystemFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof OutOfMemoryError || current instanceof StackOverflowError) {
                return true;
            }
            if (current instanceof CircuitBreakingException || current instanceof ClusterBlockException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Get current concurrency metrics
     */
    @VisibleForTesting
    protected Map<String, Object> getConcurrencyMetrics() {
        return Map.of(
            "active_experiments",
            experimentTaskContexts.size(),
            "active_tasks",
            activeTasks.sum(),
            "max_concurrent_tasks",
            maxConcurrentTasks,
            "available_permits",
            concurrencyControl.availablePermits(),
            "queued_threads",
            concurrencyControl.getQueueLength(),
            "thread_pool",
            SEARCH_RELEVANCE_EXEC_THREAD_POOL_NAME
        );
    }

    /**
     * Optimized runnable using CompletableFuture
     */
    private class OptimizedVariantTaskRunnable extends AbstractRunnable {
        private final VariantTaskParameters params;
        private final CompletableFuture<Void> future;

        OptimizedVariantTaskRunnable(VariantTaskParameters params, CompletableFuture<Void> future) {
            this.params = params;
            this.future = future;
        }

        @Override
        public void onFailure(Exception e) {
            concurrencyControl.release();
            activeTasks.decrement();

            if (e.getCause() instanceof RejectedExecutionException) {
                log.warn("Thread pool queue full, retrying task for variant: {}", params.getExperimentVariant().getId());
                scheduleVariantTaskAsync(params).whenComplete((v, ex) -> {
                    if (ex != null) {
                        future.completeExceptionally(ex);
                    } else {
                        future.complete(v);
                    }
                });
            } else {
                handleTaskFailure(params.getExperimentVariant(), e, params.getTaskContext());
                future.completeExceptionally(e);
            }
        }

        @Override
        protected void doRun() {
            executeVariantTaskAsync(params, future);
        }
    }

    private void handleTaskFailure(ExperimentVariant experimentVariant, Exception e, ExperimentTaskContext taskContext) {
        if (isCriticalSystemFailure(e)) {
            if (taskContext.getHasFailure().compareAndSet(false, true)) {
                log.error("Critical system failure for variant {}: {}", experimentVariant.getId(), e.getMessage());
                taskContext.getResultFuture().completeExceptionally(e);
            }
        } else {
            log.error("Variant failure for {}: {}", experimentVariant.getId(), e.getMessage());
            taskContext.completeVariantFailure();
        }
    }
}
