/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.executors;

import org.apache.lucene.search.TaskExecutor;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.PackagePrivate;

/**
 * {@link SearchRelevanceExecutor} provides necessary implementation and instances to execute
 * search relevance tasks in parallel using a dedicated thread pool. This ensures that one thread pool
 * is used for search relevance execution per node. The number of parallelization is constrained
 * by twice the allocated processor count since most search relevance operations are expected to be
 * short-lived. This will help achieve optimal parallelization and reasonable throughput.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SearchRelevanceExecutor {

    private static final String SEARCH_RELEVANCE_EXEC_THREAD_POOL_NAME = "_plugin_search_relevance_executor";
    private static final Integer DEFAULT_SEARCH_RELEVANCE_EXEC_THREAD_POOL_QUEUE_SIZE = 1000;
    private static final Integer MAX_THREAD_SIZE = 1000;
    private static final Integer MIN_THREAD_SIZE = 2;
    private static final Integer PROCESSOR_COUNT_DIVISOR = 2;

    private static TaskExecutor taskExecutor;

    /**
     * Provide fixed executor builder to use for search relevance executors
     * @param settings Node level settings
     * @return the executor builder for search relevance's custom thread pool.
     */
    public static ExecutorBuilder<?> getExecutorBuilder(final Settings settings) {
        int numberOfThreads = getFixedNumberOfThreadSize(settings);
        int queueSize = DEFAULT_SEARCH_RELEVANCE_EXEC_THREAD_POOL_QUEUE_SIZE;
        return new FixedExecutorBuilder(
            settings,
            SEARCH_RELEVANCE_EXEC_THREAD_POOL_NAME,
            numberOfThreads,
            queueSize,
            SEARCH_RELEVANCE_EXEC_THREAD_POOL_NAME
        );
    }

    /**
    * Initialize {@link TaskExecutor} to run tasks concurrently using {@link ThreadPool}
     * @param threadPool OpenSearch's thread pool instance
     */
    public static void initialize(ThreadPool threadPool) {
        if (threadPool == null) {
            throw new IllegalArgumentException(
                "Argument thread-pool to Search Relevance Executor cannot be null. This is required to build executor to run actions in parallel"
            );
        }
        taskExecutor = new TaskExecutor(threadPool.executor(SEARCH_RELEVANCE_EXEC_THREAD_POOL_NAME));
    }

    /**
     * Return TaskExecutor Wrapper that helps runs tasks concurrently
     * @return TaskExecutor instance to help run search tasks in parallel
     */
    public static TaskExecutor getExecutor() {
        return taskExecutor != null ? taskExecutor : new TaskExecutor(Runnable::run);
    }

    @PackagePrivate
    public static String getThreadPoolName() {
        return SEARCH_RELEVANCE_EXEC_THREAD_POOL_NAME;
    }

    /**
     * Will use thread size as half the default allocated processor. We selected half allocated processor
     * since search relevance operations are resource-intensive. This will balance throughput and prevent resource exhaustion.
     * To avoid out of range, we will return 2 as minimum processor count and 1000 as maximum thread size
     */
    private static int getFixedNumberOfThreadSize(final Settings settings) {
        final int allocatedProcessors = OpenSearchExecutors.allocatedProcessors(settings);
        int threadSize = Math.max(allocatedProcessors / PROCESSOR_COUNT_DIVISOR, MIN_THREAD_SIZE);
        return Math.min(threadSize, MAX_THREAD_SIZE);
    }
}
